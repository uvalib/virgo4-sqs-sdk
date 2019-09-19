package awssqs

import (
   "fmt"
   "time"
)

// the maximum number of messages in a block
var MAX_SQS_BLOCK_COUNT = uint( 10 )

// the maximum size of a block
var MAX_SQS_BLOCK_SIZE = uint( 262144 )

// the maximum size of a message
var MAX_SQS_MESSAGE_SIZE = MAX_SQS_BLOCK_SIZE

// the maximum queue wait time (in seconds)
var MAX_SQS_WAIT_TIME = uint( 20 )

// Errors
var BlockCountTooLargeError = fmt.Errorf( "Block count is too large. Must be %d or less", MAX_SQS_BLOCK_COUNT )
var BlockTooLargeError = fmt.Errorf( "Block size is too large. Must be %d or less", MAX_SQS_BLOCK_SIZE )
var MessageTooLargeError = fmt.Errorf( "Message size is too large. Must be %d or less", MAX_SQS_MESSAGE_SIZE )
var WaitTooLargeError = fmt.Errorf( "Wait time is too large. Must be %d or less", MAX_SQS_WAIT_TIME )
var BadQueueNameError = fmt.Errorf( "Queue name does not exist" )
var BadQueueHandleError = fmt.Errorf( "Queue handle is bad" )
var OneOrMoreOperationsUnsuccessfulError = fmt.Errorf( "One or more operations were not successful" )

// simplifications
type QueueHandle  string
type DeleteHandle string
type OpStatus     bool

// just a KV pair
type Attribute struct {
   Name  string
   Value string
}

type Attributes []Attribute
type Payload string

type Message struct {
   Attribs      Attributes
   DeleteHandle DeleteHandle
   Payload      Payload
}

type AWS_SQS interface {
   QueueHandle( string ) ( QueueHandle, error )
   BatchMessageGet( queue QueueHandle, maxMessages uint, waitTime time.Duration ) ( []Message, error )
   BatchMessagePut( queue QueueHandle, messages []Message ) ( []OpStatus, error )
   BatchMessageDelete( queue QueueHandle, messages []Message ) ( []OpStatus, error )
}

type AwsSqsConfig struct {
   supportLargeMessages bool
   s3bucketName         string
}

// Initialize our AWS_SQS connection
func NewAwsSqs( config AwsSqsConfig ) (AWS_SQS, error ) {

   // mock the implementation here if necessary

   aws, err := newAwsSqs( config )
   return aws, err
}

//
// end of file
//