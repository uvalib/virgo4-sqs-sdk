package awssqs

import (
   "log"
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

   // validate the configuration
   if len( config.s3bucketName ) == 0 {
      return nil, MissingConfiguration
   }

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

   start := time.Now( )
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
   elapsed := int64( time.Since( start ) / time.Millisecond)

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

   // we want to warn if the receive took a long time (and yielded messages)
   warnIfSlow( elapsed, "ReceiveMessage" )

   // build the response message set from the returned AWS structures
   messages := make( []Message, 0, sz )
   for _, m := range result.Messages {
      m, err := MakeMessage( *m )
      if err != nil {
         return emptyMessageList, err
      }
      messages = append( messages, *m )
   }

   return messages, nil
}

func ( awsi * awsSqsImpl) BatchMessagePut( queue QueueHandle, messages []Message ) ( []OpStatus, error ) {

   // early exit if no messages provided
   sz := len( messages )
   if sz == 0 {
      return emptyOpList, nil
   }

   // ensure the block size is not too large
   if uint( sz ) > MAX_SQS_BLOCK_COUNT {
      return emptyOpList, BlockCountTooLargeError
   }

   // our operation status array
   ops := make( []OpStatus, sz, sz )

   // convert any oversize messages (use index access to the array because this updates the messages)
   for ix, _ := range messages {
      ops[ ix ] = true
      sz := messages[ ix ].Size( )
      if sz > MAX_SQS_MESSAGE_SIZE {
         err := messages[ ix ].ConvertToOversizeMessage( awsi.config.s3bucketName )
         if err != nil {
            // FIXME
            return emptyOpList, err
            //ops[ ix ] = false
         }
      }
   }

   // calculate the total block size and the number of messages that are larger than the maximum message size
   var totalSize uint = 0
   for _, m := range messages {
      totalSize += m.Size( )
   }

   // if the total block size is too large then we can split the block in half and handle each one individually
   if totalSize > MAX_SQS_BLOCK_SIZE {
      half := sz / 2
      log.Printf( "WARNING: blocksize too large, splitting at %d", half )
      op1, err1 := awsi.BatchMessagePut( queue, messages[ 0:half ] )
      op2, err2 := awsi.BatchMessagePut( queue, messages[ half: ] )
      copy( op1, op2 )
      if err1 != nil {
         return op1, err1
      } else {
         return op1, err2
      }
   }

   q := string( queue )

   batch := make( []*sqs.SendMessageBatchRequestEntry, 0, sz )

   for ix, m := range messages {
      batch = append( batch, constructSend( m, ix ) )
   }

   start := time.Now( )
   response, err := awsi.svc.SendMessageBatch( &sqs.SendMessageBatchInput{
      Entries:     batch,
      QueueUrl:    &q,
   })
   elapsed := int64( time.Since( start ) / time.Millisecond)

   // we want to warn if the receive took a long time
   warnIfSlow( elapsed, "SendMessageBatch" )

   if err != nil {
      if strings.HasPrefix( err.Error(), invalidAddressErrorPrefix ) {
         return emptyOpList, BadQueueHandleError
      }
      return emptyOpList, err
   }

   for _, f := range response.Failed {
      log.Printf( "ERROR: ID %s send not successful (%s)", *f.Id, *f.Message )
      id, converr := strconv.Atoi( *f.Id )
      if converr == nil && id < sz {
         ops[ id ] = false
         err = OneOrMoreOperationsUnsuccessfulError
      }
   }

   //for _, f := range response.Successful {
   //   log.Printf( "OK: %s", *f.Id )
   //}

   return ops, err
}

func ( awsi *awsSqsImpl) BatchMessageDelete( queue QueueHandle, messages []Message ) ( []OpStatus, error ) {

   // early exit if no messages provided
   var sz = uint( len( messages ) )
   if sz == 0 {
      return emptyOpList, nil
   }

   // ensure the block size is not too large
   if sz > MAX_SQS_BLOCK_COUNT {
      return emptyOpList, BlockCountTooLargeError
   }

   q := string( queue )

   batch := make( []*sqs.DeleteMessageBatchRequestEntry, 0, sz )
   ops := make( []OpStatus, sz, sz )

   // the SQS delete loop, initially, assume everything works
   for ix, m := range messages {
      ops[ ix ] = true
      batch = append( batch, constructDelete( m.GetReceiptHandle( ), ix ) )
   }

   start := time.Now( )
   response, err := awsi.svc.DeleteMessageBatch( &sqs.DeleteMessageBatchInput{
      Entries:     batch,
      QueueUrl:    &q,
   })
   elapsed := int64( time.Since( start ) / time.Millisecond)

   // we want to warn if the receive took a long time
   warnIfSlow( elapsed, "DeleteMessageBatch" )

   if err != nil {
      if strings.HasPrefix( err.Error(), invalidAddressErrorPrefix ) {
         return emptyOpList, BadQueueHandleError
      }
      return emptyOpList, err
   }

   for _, f := range response.Failed {
      log.Printf( "ERROR: ID %s delete not successful (%s)", *f.Id, *f.Message )
      id, converr := strconv.Atoi( *f.Id )
      if converr == nil && uint( id ) < sz {
         ops[ id ] = false
         err = OneOrMoreOperationsUnsuccessfulError
      } else {
         log.Printf( "ERROR: Suspect ID %s delete not successful", *f.Id )
      }
   }

   // we have now deleted the messages from SQS, delete any oversize payloads from S3
   for _, f := range response.Successful {
      id, converr := strconv.Atoi( *f.Id )
      if converr == nil && uint( id ) < sz {
         if messages[ id ].IsOversize() == true {
            deleteError := messages[ id ].DeleteOversizeMessage( )
            if deleteError != nil {
               ops[ id ] = false
               err = OneOrMoreOperationsUnsuccessfulError
            }
         }
      } else {
         log.Printf( "ERROR: Suspect ID %s delete not successful", *f.Id )
      }
   }

   return ops, err
}

//
// end of file
//
