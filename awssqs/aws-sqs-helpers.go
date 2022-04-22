package awssqs

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"log"
	"strconv"
)

// log a warning if any SQS request takes longer than this
var warnIfRequestTakesLonger = int64(250)

// construct an AWS send structure when provided a message
// the index value is used to differentiate requests when they are made in blocks
func constructSend(message Message, index int) *sqs.SendMessageBatchRequestEntry {

	// if we have attributes to send
	if len(message.Attribs) != 0 {
		return &sqs.SendMessageBatchRequestEntry{
			MessageAttributes: awsAttribsFromMessageAttribs(message.Attribs),
			MessageBody:       aws.String(string(message.Payload)),
			Id:                aws.String(strconv.Itoa(index)),
		}
	}

	// no attributes
	return &sqs.SendMessageBatchRequestEntry{
		MessageBody: aws.String(string(message.Payload)),
		Id:          aws.String(strconv.Itoa(index)),
	}
}

// construct an AWS delete object when provided a receipt handle
// the index value is used to differentiate requests when they are made in blocks
func constructDelete(deleteHandle ReceiptHandle, index int) *sqs.DeleteMessageBatchRequestEntry {

	return &sqs.DeleteMessageBatchRequestEntry{
		ReceiptHandle: aws.String(string(deleteHandle)),
		Id:            aws.String(strconv.Itoa(index)),
	}
}

func awsAttribsFromMessageAttribs(attribs Attributes) map[string]*sqs.MessageAttributeValue {
	attributes := make(map[string]*sqs.MessageAttributeValue)
	for _, a := range attribs {
		attributes[a.Name] = &sqs.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(a.Value),
		}
	}
	return attributes
}

// sometimes it is interesting to know if our SQS queries are slow
func warnIfSlow(elapsed int64, prefix string) {

	if elapsed >= warnIfRequestTakesLonger {
		log.Printf("INFO: %s elapsed %d ms", prefix, elapsed)
	}
}

//
// end of file
//
