package awssqs

import (
   "strconv"
   "time"

   "github.com/aws/aws-sdk-go/aws"
   "github.com/aws/aws-sdk-go/aws/session"
   "github.com/aws/aws-sdk-go/service/sqs"
)

var emptyOpList = make( []OpStatus, 0 )
var emptyMessageList = make( []Message, 0 )

// this is our implementation
type awsSqsImpl struct {
   svc       * sqs.SQS
}

// Initialize our AWS_SQS abstraction
func newAwsSqs( ) (AWS_SQS, error ) {

   sess, err := session.NewSession( )
   if err != nil {
      return nil, err
   }

   svc := sqs.New( sess )

   return &awsSqsImpl{svc }, nil
}

func ( awsi *awsSqsImpl) QueueHandle( queueName string ) ( QueueHandle, error ) {

   // get the queue URL from the name
   result, err := awsi.svc.GetQueueUrl( &sqs.GetQueueUrlInput{
      QueueName: aws.String( queueName ),
   })

   if err != nil {
      return "", err
   }

   return QueueHandle( *result.QueueUrl ), nil
}

func ( awsi *awsSqsImpl) BatchMessageGet( queue QueueHandle, maxMessages uint, waitTime time.Duration ) ( []Message, error ) {

   // ensure the block size is not too large
   if int( maxMessages ) > MAX_SQS_BLOCK_COUNT {
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
      WaitTimeSeconds:     aws.Int64( int64( waitTime ) ),
   })

   if err != nil {
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
   if sz > MAX_SQS_BLOCK_COUNT {
      return emptyOpList, BlockCountTooLargeError
   }

   q := string( queue )

   batch := make( []*sqs.SendMessageBatchRequestEntry, 0, sz )
   ops := make( []OpStatus, 0, sz )

   for ix, m := range messages {
      batch = append( batch, constructSend( m, ix ) )
      ops = append( ops, true )
   }

   _, err := awsi.svc.SendMessageBatch( &sqs.SendMessageBatchInput{
      Entries:     batch,
      QueueUrl:    &q,
   })

   if err != nil {
      return emptyOpList, err
   }

   //
   // FIXME
   // process to determine if they all succeeded or not
   //

   return ops, nil
}

func ( awsi *awsSqsImpl) BatchMessageDelete( queue QueueHandle, messages []Message ) ( []OpStatus, error ) {

   // early exit if no messages provided
   sz := len( messages )
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

   _, err := awsi.svc.DeleteMessageBatch( &sqs.DeleteMessageBatchInput{
      Entries:     batch,
      QueueUrl:    &q,
   })

   //
   // FIXME
   // process to determine if they all succeeded or not
   //
   
   if err != nil {
      return emptyOpList, err
   }

   return ops, nil
}

//
// private helper methods
//

func constructSend( message Message, index int ) * sqs.SendMessageBatchRequestEntry {

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
      attributes = append( attributes, Attribute{ name: k, value: *v.StringValue })
   }
   a := Attributes( attributes )
   return a
}

func awsAttribsFromMessageAttribs( attribs Attributes ) map[string] * sqs.MessageAttributeValue {
   attributes := make( map[string] * sqs.MessageAttributeValue )
   for _, a := range attribs {
      attributes[ a.name ] = &sqs.MessageAttributeValue{
         DataType: aws.String("String" ),
         StringValue: aws.String( a.value ),
      }
   }
   return attributes
}

//
// end of file
//
