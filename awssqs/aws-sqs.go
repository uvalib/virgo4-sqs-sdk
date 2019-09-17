package awssqs

import (
   "fmt"
   "time"
)

// the maximum number of messages in a block
var MAX_SQS_BLOCK_COUNT = 10

// the maximum size of a block
var MAX_SQS_BLOCK_SIZE = 262144

// the maximum queue wait time (in seconds)
var MAX_SQS_WAIT_TIME = 20

// Errors
var BlockCountTooLargeError = fmt.Errorf( "Block count is too large. Must be %d or less", MAX_SQS_BLOCK_COUNT )
var BlockTooLargeError = fmt.Errorf( "Block size is too large. Must be %d or less", MAX_SQS_BLOCK_SIZE )
var WaitTooLargeError = fmt.Errorf( "Wait time is too large. Must be %d or less", MAX_SQS_WAIT_TIME )

// simplifications
type QueueHandle  string
type DeleteHandle string
type OpStatus     bool

// just a KV pair
type Attribute struct {
   name  string
   value string
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
func NewAwsSqs( AwsSqsConfig ) (AWS_SQS, error ) {

   // mock the implementation here if necessary

   aws, err := newAwsSqs( )
   return aws, err
}

//
// end of file
//