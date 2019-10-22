package awssqs

import (
	"fmt"
	"time"
)

// the maximum number of messages in a block
var MAX_SQS_BLOCK_COUNT = uint(10)

// the maximum size of a block
var MAX_SQS_BLOCK_SIZE = uint(262144)

// the maximum size of a message
var MAX_SQS_MESSAGE_SIZE = MAX_SQS_BLOCK_SIZE

// the maximum queue wait time (in seconds)
var MAX_SQS_WAIT_TIME = uint(20)

// Errors
var BlockCountTooLargeError = fmt.Errorf("Block count is too large. Must be %d or less", MAX_SQS_BLOCK_COUNT)
var BlockTooLargeError = fmt.Errorf("Block size is too large. Must be %d or less", MAX_SQS_BLOCK_SIZE)
var MessageTooLargeError = fmt.Errorf("Message size is too large. Must be %d or less", MAX_SQS_MESSAGE_SIZE)
var WaitTooLargeError = fmt.Errorf("Wait time is too large. Must be %d or less", MAX_SQS_WAIT_TIME)
var BadQueueNameError = fmt.Errorf("Queue name does not exist")
var BadQueueHandleError = fmt.Errorf("Queue handle is bad")
var OneOrMoreOperationsUnsuccessfulError = fmt.Errorf("One or more operations were not successful")
var BadReceiptHandleError = fmt.Errorf("Receipt handle format is incorrect for large message support")
var MismatchedContentsSize = fmt.Errorf("Actual S3 message size differs from expected size")
var MissingConfiguration = fmt.Errorf("Configuration information is incomplete")

// standard attribute keys and values
var AttributeKeyRecordId = "id"
var AttributeKeyRecordType = "type"
var AttributeKeyRecordSource = "source"
var AttributeKeyRecordOperation = "operation"

var AttributeValueRecordTypeB64Marc = "base64/marc"
var AttributeValueRecordTypeXml = "xml"
var AttributeValueRecordOperationUpdate = "update"
var AttributeValueRecordOperationDelete = "delete"

// simplifications
type QueueHandle string
type ReceiptHandle string
type OpStatus bool

// just a KV pair
type Attribute struct {
	Name  string
	Value string
}

type Attributes []Attribute

type Message struct {
	Attribs       Attributes
	ReceiptHandle ReceiptHandle
	Payload       []byte

	// used by the implementation
	oversize bool
}

type AWS_SQS interface {

	// get a queue handle (URL) when provided a queue name
	QueueHandle(string) (QueueHandle, error)

	// get a batch of messages from the specified queue. Will return on receipt of any messages
	// without waiting and will wait no longer than the wait time if no messages are received.
	BatchMessageGet(queue QueueHandle, maxMessages uint, waitTime time.Duration) ([]Message, error)

	// put a batch of messages to the specified queue.
	// in the event of one or more failure, the operation status array will indicate which
	// messages were processed successfully and which were not.
	BatchMessagePut(queue QueueHandle, messages []Message) ([]OpStatus, error)

	// mark a batch of messages from the specified queue as suitable for delete. This mechanism
	// prevents messages from being reprocessed.
	BatchMessageDelete(queue QueueHandle, messages []Message) ([]OpStatus, error)
}

// our configuration structure
type AwsSqsConfig struct {
	MessageBucketName string // the name of the bucket to use for oversize messages
}

// factory for our SQS interface
func NewAwsSqs(config AwsSqsConfig) (AWS_SQS, error) {

	// mock the implementation here if necessary
	aws, err := newAwsSqs(config)
	return aws, err
}

//
// end of file
//
