package awssqs

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/service/sqs"
	"log"
	"strconv"
	"strings"
)

// support for large messages (using S3)
// this is a copy of the Java implementation for compatibility. See https://github.com/awslabs/amazon-sqs-java-extended-client-lib
var oversizeMessageAttributeName = "SQSLargePayloadSize"
var bucketNameMarker             = "-..s3BucketName..-"
var bucketKeyMarker              = "-..s3Key..-"

var s3BucketMapKeyValue          = "s3BucketName"
var s3KeyMapKeyValue             = "s3Key"
var s3MarkerTag                  = "com.amazon.sqs.javamessaging.MessageS3Pointer"

//
// we need to be compatible with the Java library that provides oversize message support... this is an example of the
// structure they write
//
// [ "com.amazon.sqs.javamessaging.MessageS3Pointer",
//   { "s3BucketName":"virgo4-ingest-staging-messages",
//     "s3Key":"9b9e4bc4-8bd8-4527-a25e-818f17dd5aab"
//   }
// ]
//
//
type S3MarkerPayload[2] interface{}

//
// our message factory based on a message from AWS
//
func MakeMessage(awsMessage sqs.Message) (*Message, error) {

	message := &Message{
		ReceiptHandle: ReceiptHandle(*awsMessage.ReceiptHandle),
		Attribs:       makeAttributes(awsMessage.MessageAttributes),
		Payload:       []byte(*awsMessage.Body),
	}

	// check to see if this is a special 'oversize' message which stores the payload in S3, if it is, do the necessary processing
	s3size, found := message.GetAttribute(oversizeMessageAttributeName)
	if found == true {

		//log.Printf( "INFO: constructing oversize message" )

		// use this later
		sz, _ := strconv.Atoi(s3size)

		// mark as oversize and extract the S3 marker from the payload which has bucket and key information
		message.oversize = true
		bucket, key, err := message.decodeS3MarkerInformation(message.Payload)
		if err != nil {
			return nil, err
		}

		// construct the new receipt handle... we overload it with bucket and key information
		newReceiptHandle := message.makeEnhancedReceiptHandle(bucket, key, message.ReceiptHandle)

		// get the actual message contents from S3
		contents, err := s3Get(bucket, key, sz)
		if err != nil {
			return nil, err
		}

		// ensure the actual size of the S3 object we read matches the reported size
		if len(contents) != sz {
			return nil, MismatchedContentsSize
		}

		// update the contents of the message (overwriting the S3 marker object there)
		message.Payload = contents

		// save the 'enhanced' receipt handle
		message.ReceiptHandle = newReceiptHandle

		// finally, delete the 'marker' attribute we use for indicating this is a special type of message
		message.deleteAttribute(oversizeMessageAttributeName)
	}

	return message, nil
}

// make a set of our message attributes from AWS message metadata
func makeAttributes(attribs map[string]*sqs.MessageAttributeValue) Attributes {
	attributes := make([]Attribute, 0, len(attribs))
	for k, v := range attribs {
		attributes = append(attributes, Attribute{Name: k, Value: *v.StringValue})
	}
	a := Attributes(attributes)
	return a
}

//
// Message helpers methods
//

// an approximation of the message size. Used in calculations to ensure we do not exceed the
// maximum message block size imposed by AWS
func (m *Message) Size() uint {

	var padFactor = 3 // a guess at the padding for each string in the attribute set
	sz := uint(len(m.Payload))
	for _, a := range m.Attribs {
		sz += uint(len(a.Name) + len(a.Value) + (2 * padFactor))
	}
	//log.Printf( "INFO: reporting size %d", sz )
	return sz
}

// is this a oversize 'oversize' message
func (m *Message) IsOversize() bool {
	return m.oversize
}

// if this is an oversize  message, delete the bucket contents
func (m *Message) DeleteOversizeMessage() error {

	// if this is not a oversize message, then ignore
	if m.oversize == false {
		return nil
	}

	//log.Printf( "INFO: deleting oversize message" )

	// a oversize 'large' messages encodes the bucket attributes in the receipt handle
	bucket, key := m.getBucketAttributes(m.ReceiptHandle)
	if bucket != "" && key != "" {
		return s3Delete(bucket, key)
	}

	return BadReceiptHandleError
}

func (m *Message) ConvertToOversizeMessage(bucket string) error {

	// if this is already marked as a oversize message, then ignore
	if m.oversize == true {
		return nil
	}

	//log.Printf( "INFO: converting oversize message" )

	// add the contents to S3
	key, err := s3Add(bucket, m.Payload)
	if err != nil {
		return err
	}

	// create the replacement contents for the message
	contents := m.encodeS3MarkerInformation(bucket, key)

	// create the enhanced receipt handle
	m.ReceiptHandle = m.makeEnhancedReceiptHandle(bucket, key, m.ReceiptHandle)

	// add the special message attribute we use to identify an oversize message
	m.addAttribute(oversizeMessageAttributeName, strconv.Itoa(len(m.Payload)))

	// replace the contents of the original message with the new contents
	m.Payload = contents

	// mark as oversize
	m.oversize = true

	return nil
}

// because the receipt handle is overloaded, we use a helper method to access it
func (m *Message) GetReceiptHandle() ReceiptHandle {

	// if we are not overloading the receipt handle, just return it
	if m.oversize == false {
		return m.ReceiptHandle
	}

	return m.getNativeReceiptHandle(m.ReceiptHandle)
}

// get an attribute
func (m *Message) GetAttribute( attribute string ) ( string, bool ) {

	for _, a := range m.Attribs {
		if a.Name == attribute {
			return a.Value, true
		}
	}
	return "", false
}

// clone the content but none of the internal state
func (m *Message) ContentClone( ) * Message {

	newMessage := &Message{}
	newMessage.Attribs = m.Attribs
	newMessage.Payload = m.Payload
	return newMessage
}

//
// implementation methods
//

// decode the S3 marker information from the supplied payload
func (m *Message) decodeS3MarkerInformation(payload []byte) (string, string, error) {

	s3MarkerPayload := S3MarkerPayload{}
	err := json.Unmarshal([]byte( payload ), &s3MarkerPayload)
	if err != nil {
		log.Printf("ERROR: json unmarshal: %s", err)
		return "", "", err
	}

	s3, ok := s3MarkerPayload[1].(map[string]interface{})
	if ok == false {
		log.Printf("ERROR: type assertion error in decodeS3MarkerInformation" )
		return "", "", fmt.Errorf( "type assertion error")
	}

	// wildly optimistic that these assertions will not fail
	bucket, _ := s3[s3BucketMapKeyValue].(string)
	key, _ := s3[s3KeyMapKeyValue].(string)
	return bucket, key, nil
}

// encode the S3 marker information based on the supplied bucket information
func (m *Message) encodeS3MarkerInformation(bucket string, key string) []byte {

	return []byte( fmt.Sprintf("[\"%s\",{\"%s\":\"%s\",\"%s\":\"%s\"}]",
		s3MarkerTag,
		s3BucketMapKeyValue,
		bucket,
		s3KeyMapKeyValue,
		key ) )
}

// extract the bucket attributes from the enhanced receipt handle according to the standard format
func (m *Message) getBucketAttributes(receiptHandle ReceiptHandle) (string, string) {
	bucket, key, _ := m.splitEnhancedReceiptHandle(receiptHandle)
	return bucket, key
}

// extract the native receipt handle from the enhanced receipt handle according to the standard format
func (m *Message) getNativeReceiptHandle(receiptHandle ReceiptHandle) ReceiptHandle {
	_, _, receipt := m.splitEnhancedReceiptHandle(receiptHandle)
	return receipt
}

// split an 'enhanced' receipt handle into its component pieces
// an enhanced receipt handle consists of the following:
//
// bucket name delimiter | bucket name | bucket name delimiter | bucket key delimiter | bucket key | bucket key delimiter | native receipt handle
//
func (m *Message) splitEnhancedReceiptHandle(receiptHandle ReceiptHandle) (string, string, ReceiptHandle) {

	bucketTokens := strings.Split(string(receiptHandle), bucketNameMarker)
	keyTokens := strings.Split(string(receiptHandle), bucketKeyMarker)
	bucket, key, receipt := "", "", ReceiptHandle("")

	// do we have what we need to extract the bucket name
	if len(bucketTokens) == 3 {
		bucket = bucketTokens[1]
	} else {
		log.Printf("WARNING: cannot find bucket value in receipt handle")
	}

	// do we have what we need to extract the key name and original receipt handle
	if len(keyTokens) == 3 {
		key = keyTokens[1]
		receipt = ReceiptHandle(keyTokens[2])
	} else {
		log.Printf("WARNING: cannot find key value in receipt handle")
	}

	return bucket, key, receipt
}

func (m *Message) makeEnhancedReceiptHandle(bucket string, key string, receiptHandle ReceiptHandle) ReceiptHandle {
	return ReceiptHandle(bucketNameMarker+bucket+bucketNameMarker+
		bucketKeyMarker+key+bucketKeyMarker) + receiptHandle
}

func (m *Message) addAttribute(attribute string, value string) bool {

	m.deleteAttribute(attribute)
	m.Attribs = append(m.Attribs, Attribute{Name: attribute, Value: value})
	return true
}

func (m *Message) deleteAttribute(attribute string) bool {

	for ix, a := range m.Attribs {
		if a.Name == attribute {
			m.Attribs = append(m.Attribs[:ix], m.Attribs[ix+1:]...)
			return true
		}
	}
	return false
}

//
// end of file
//
