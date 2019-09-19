package awssqs

import (
   "fmt"
   "strconv"
   "strings"
   "time"

   "github.com/aws/aws-sdk-go/aws"
   "github.com/aws/aws-sdk-go/aws/session"
   "github.com/aws/aws-sdk-go/service/sqs"
)

var emptyOpList = make( []OpStatus, 0 )
var emptyMessageList = make( []Message, 0 )

// text for a specific error cases
var malformedInputErrorPrefix = "MalformedInput"
var invalidAddressErrorPrefix = "InvalidAddress"

// this is our implementation
type awsSqsImpl struct {
   config    AwsSqsConfig
   svc      * sqs.SQS
}

// Initialize our AWS_SQS abstraction
func newAwsSqs( config AwsSqsConfig ) (AWS_SQS, error ) {

   sess, err := session.NewSession( )
   if err != nil {
      return nil, err
   }

   svc := sqs.New( sess )

   return &awsSqsImpl{config, svc }, nil
}

func ( awsi *awsSqsImpl) QueueHandle( queueName string ) ( QueueHandle, error ) {

   // get the queue URL from the name
   result, err := awsi.svc.GetQueueUrl( &sqs.GetQueueUrlInput{
      QueueName: aws.String( queueName ),
   })

   if err != nil {
      if strings.HasPrefix( err.Error(), sqs.ErrCodeQueueDoesNotExist ) {
         return "", BadQueueNameError
      }
      return "", err
   }

   return QueueHandle( *result.QueueUrl ), nil
}

func ( awsi *awsSqsImpl) BatchMessageGet( queue QueueHandle, maxMessages uint, waitTime time.Duration ) ( []Message, error ) {

   // ensure the block size is not too large
   if maxMessages > MAX_SQS_BLOCK_COUNT {
      return emptyMessageList, BlockCountTooLargeError
   }

   // ensure the wait time is not too large
   if waitTime.Seconds() > float64( MAX_SQS_WAIT_TIME ) {
      return emptyMessageList, WaitTooLargeError
   }

   q := string( queue )

   result, err := awsi.svc.ReceiveMessage( &sqs.ReceiveMessageInput{
      //AttributeNames: []*string{
      //	aws.String( sqs.QueueAttributeNameAll ),
      //},
      MessageAttributeNames: []*string{
         aws.String( sqs.QueueAttributeNameAll ),
      },
      QueueUrl:            &q,
      MaxNumberOfMessages: aws.Int64( int64( maxMessages ) ),
      WaitTimeSeconds:     aws.Int64( int64( waitTime.Seconds() ) ),
   })

   if err != nil {
      if strings.HasPrefix( err.Error(), invalidAddressErrorPrefix ) {
         return emptyMessageList, BadQueueHandleError
      }
      return emptyMessageList, err
   }

   // if we did not get any messages
   sz := len( result.Messages )
   if sz == 0 {
      return emptyMessageList, nil
   }

   messages := make( []Message, 0, sz )
   for _, m := range result.Messages {
      messages = append( messages, messageFromAwsStruct( *m ) )
   }

   return messages, nil
}

func ( awsi *awsSqsImpl) BatchMessagePut( queue QueueHandle, messages []Message ) ( []OpStatus, error ) {

   // early exit if no messages provided
   sz := len( messages )
   if sz == 0 {
      return emptyOpList, nil
   }

   // ensure the block size is not too large
   if uint( sz ) > MAX_SQS_BLOCK_COUNT {
      return emptyOpList, BlockCountTooLargeError
   }

   // calculate the total block size and the number of messages that are larger than the maximum message size
   var totalSize uint = 0
   oversizeCount := 0
   for _, m := range messages {
      var sz uint = uint( len( m.Payload ) )
      if sz > MAX_SQS_MESSAGE_SIZE {
         oversizeCount++
      }
      totalSize += sz
   }

   // if the total block size is too large and we do not have any messages that are too large then we can split
   // the block in half and handle each one individually
   if totalSize > MAX_SQS_BLOCK_SIZE {
      if oversizeCount == 0 {
         half := sz / 2
         fmt.Printf( "Splitting block at %d\n", half )
         op1, err1 := awsi.BatchMessagePut( queue, messages[ 0:half ] )
         op2, err2 := awsi.BatchMessagePut( queue, messages[ half: ] )
         copy( op1, op2 )
         if err1 != nil {
            return op1, err1
         } else {
            return op1, err2
         }
      } else {
         return emptyOpList, MessageTooLargeError
      }
   }

   q := string( queue )
   //fmt.Printf( "Queue: %s\n", q )

   batch := make( []*sqs.SendMessageBatchRequestEntry, 0, sz )
   ops := make( []OpStatus, 0, sz )

   for ix, m := range messages {
      batch = append( batch, constructSend( m, ix ) )
      ops = append( ops, true )
   }

   //fmt.Printf( "Sending: %d\n", len( batch ) )

   response, err := awsi.svc.SendMessageBatch( &sqs.SendMessageBatchInput{
      Entries:     batch,
      QueueUrl:    &q,
   })

   if err != nil {
      if strings.HasPrefix( err.Error(), invalidAddressErrorPrefix ) {
         return emptyOpList, BadQueueHandleError
      }
      return emptyOpList, err
   }

   for _, f := range response.Failed {
      fmt.Printf( "ERROR: %s %s\n", *f.Id, *f.Message )
      id, converr := strconv.Atoi( *f.Id )
      if converr == nil && id < sz {
         ops[ id ] = false
         err = OneOrMoreOperationsUnsuccessfulError
      }
   }

   //for _, f := range response.Successful {
   //   fmt.Printf( "OK: %s\n", *f.Id )
   //}

   return ops, err
}

func ( awsi *awsSqsImpl) BatchMessageDelete( queue QueueHandle, messages []Message ) ( []OpStatus, error ) {

   // early exit if no messages provided
   var sz uint = uint( len( messages ) )
   if sz == 0 {
      return emptyOpList, nil
   }

   // ensure the block size is not too large
   if sz > MAX_SQS_BLOCK_COUNT {
      return emptyOpList, BlockCountTooLargeError
   }

   q := string( queue )

   batch := make( []*sqs.DeleteMessageBatchRequestEntry, 0, sz )
   ops := make( []OpStatus, 0, sz )

   // the delete loop, assume everything worked
   for ix, m := range messages {
      batch = append( batch, constructDelete( m.DeleteHandle, ix ) )
      ops = append( ops, true )
   }

   response, err := awsi.svc.DeleteMessageBatch( &sqs.DeleteMessageBatchInput{
      Entries:     batch,
      QueueUrl:    &q,
   })

   if err != nil {
      if strings.HasPrefix( err.Error(), invalidAddressErrorPrefix ) {
         return emptyOpList, BadQueueHandleError
      }
      return emptyOpList, err
   }

   for _, f := range response.Failed {
      fmt.Printf( "ERROR: %s %s\n", *f.Id, *f.Message )
      id, converr := strconv.Atoi( *f.Id )
      if converr == nil && uint( id ) < sz {
         ops[ id ] = false
         err = OneOrMoreOperationsUnsuccessfulError
      }
   }

   //for _, f := range response.Successful {
   //   fmt.Printf( "OK: %s\n", *f.Id )
   //}

   return ops, err
}

//
// private helper methods
//

func constructSend( message Message, index int ) * sqs.SendMessageBatchRequestEntry {

   //fmt.Printf( "%+v\n", message )
   return &sqs.SendMessageBatchRequestEntry{
      MessageAttributes: awsAttribsFromMessageAttribs( message.Attribs ),
      MessageBody:       aws.String( string( message.Payload ) ),
      Id:                aws.String( strconv.Itoa( index )),
   }
}

func constructDelete( deleteHandle DeleteHandle, index int ) * sqs.DeleteMessageBatchRequestEntry {

   return &sqs.DeleteMessageBatchRequestEntry{
      ReceiptHandle: aws.String( string( deleteHandle ) ),
      Id:            aws.String( strconv.Itoa( index )),
   }
}

func messageFromAwsStruct( awsMessage sqs.Message ) Message {

   return Message{
      DeleteHandle: DeleteHandle( *awsMessage.ReceiptHandle ),
      Attribs:      messageAttribsFromAwsStrict( awsMessage.MessageAttributes ),
      Payload:      Payload( *awsMessage.Body ),
   }
}

func messageAttribsFromAwsStrict( attribs map[string] * sqs.MessageAttributeValue  ) Attributes {
   attributes := make( []Attribute, 0, len( attribs ) )
   for k, v := range attribs {
      attributes = append( attributes, Attribute{ Name: k, Value: *v.StringValue })
   }
   a := Attributes( attributes )
   return a
}

func awsAttribsFromMessageAttribs( attribs Attributes ) map[string] * sqs.MessageAttributeValue {
   attributes := make( map[string] * sqs.MessageAttributeValue )
   for _, a := range attribs {
      attributes[ a.Name ] = &sqs.MessageAttributeValue{
         DataType: aws.String("String" ),
         StringValue: aws.String( a.Value ),
      }
   }
   return attributes
}

//
// end of file
//
