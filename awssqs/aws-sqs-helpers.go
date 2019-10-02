package awssqs

import (
   "github.com/aws/aws-sdk-go/aws"
   "github.com/aws/aws-sdk-go/service/sqs"
   "log"
   "strconv"
)

// log a warning if any SQS request takes longer than this
var warnIfRequestTakesLonger = int64( 250 )

//
// Other helper methods
//
func constructSend( message Message, index int ) * sqs.SendMessageBatchRequestEntry {

   return &sqs.SendMessageBatchRequestEntry{
      MessageAttributes: awsAttribsFromMessageAttribs( message.Attribs ),
      MessageBody:       aws.String( string( message.Payload ) ),
      Id:                aws.String( strconv.Itoa( index )),
   }
}

func constructDelete( deleteHandle ReceiptHandle, index int ) * sqs.DeleteMessageBatchRequestEntry {

   return &sqs.DeleteMessageBatchRequestEntry{
      ReceiptHandle: aws.String( string( deleteHandle ) ),
      Id:            aws.String( strconv.Itoa( index )),
   }
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

func warnIfSlow( elapsed int64, prefix string ) {

   if elapsed >= warnIfRequestTakesLonger {
      log.Printf("WARNING: %s elapsed %d ms", prefix, elapsed)
   }
}

//
// end of file
//
