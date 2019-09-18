package main

import (
   "fmt"
   "testing"
   "time"
)

var goodQueueName = "virgo4-ingest-test-staging"
var badQueueName = "xxx"
var badQueueHandle = QueueHandle( "blablabla" )
var badDeleteHandle = DeleteHandle( "blablabla" )
var goodWaitTime = 15 * time.Second
var badWaitTime = 99 * time.Second
var zeroWaitTime = 0 * time.Second

//
// General behavior tests
//
func TestCorrectMessageCount(t *testing.T) {

   awssqs, err := NewAwsSqs( AwsSqsConfig{ } )
   if err != nil {
      t.Fatalf("%t\n", err)
   }

   queueHandle, err := awssqs.QueueHandle( goodQueueName )
   if err != nil {
      t.Fatalf("%t\n", err)
   }

   clearQueue( t, awssqs, queueHandle )
}

func TestCorrectSmallMessageContent(t *testing.T) {

   awssqs, err := NewAwsSqs( AwsSqsConfig{ } )
   if err != nil {
      t.Fatalf("%t\n", err)
   }

   queueHandle, err := awssqs.QueueHandle( goodQueueName )
   if err != nil {
      t.Fatalf("%t\n", err)
   }

   clearQueue( t, awssqs, queueHandle )
}

func TestCorrectLargeMessageContent(t *testing.T) {

   awssqs, err := NewAwsSqs( AwsSqsConfig{ } )
   if err != nil {
      t.Fatalf("%t\n", err)
   }

   queueHandle, err := awssqs.QueueHandle( goodQueueName )
   if err != nil {
      t.Fatalf("%t\n", err)
   }

   clearQueue( t, awssqs, queueHandle )
}

//
// QueueHandle method invariant tests
//

func TestQueueHandleHappyDay(t *testing.T) {

   awssqs, err := NewAwsSqs( AwsSqsConfig{ } )
   if err != nil {
      t.Fatalf("%t\n", err)
   }

   queueHandle, err := awssqs.QueueHandle( goodQueueName )
   if err != nil {
      t.Fatalf("%t\n", err)
   }

   if len( queueHandle ) == 0 {
      t.Fatalf("Queue handle is blank\n" )
   }
}

func TestQueueHandleBadName(t *testing.T) {

   awssqs, err := NewAwsSqs( AwsSqsConfig{ } )
   if err != nil {
      t.Fatalf("%t\n", err)
   }

   _, err = awssqs.QueueHandle( badQueueName )
   if err != BadQueueNameError {
      t.Fatalf("%t\n", err)
   }
}

//
// BatchMessageGet method invariant tests
//

func TestBatchMessageGetHappyDay(t *testing.T) {

   awssqs, err := NewAwsSqs( AwsSqsConfig{ } )
   if err != nil {
      t.Fatalf("%t\n", err)
   }

   queueHandle, err := awssqs.QueueHandle( goodQueueName )
   if err != nil {
      t.Fatalf("%t\n", err)
   }

   messages, err := awssqs.BatchMessageGet( queueHandle, 1, zeroWaitTime )
   if err != nil {
      t.Fatalf("%t\n", err)
   }

   if len( messages ) > 1 {
      t.Fatalf("Received more messages than expected\n" )
   }
}

func TestBatchMessageGetBadQueueHandle(t *testing.T) {

   awssqs, err := NewAwsSqs( AwsSqsConfig{ } )
   if err != nil {
      t.Fatalf("%t\n", err)
   }

   _, err = awssqs.BatchMessageGet( badQueueHandle, uint( MAX_SQS_BLOCK_COUNT ), goodWaitTime )
   if err != BadQueueHandleError {
      t.Fatalf("%t\n", err)
   }
}

func TestBatchMessageGetBadBlockSize(t *testing.T) {

   awssqs, err := NewAwsSqs( AwsSqsConfig{ } )
   if err != nil {
      t.Fatalf("%t\n", err)
   }

   queueHandle, err := awssqs.QueueHandle( goodQueueName )
   if err != nil {
      t.Fatalf("%t\n", err)
   }

   _, err = awssqs.BatchMessageGet( queueHandle, uint( MAX_SQS_BLOCK_COUNT + 1 ), goodWaitTime )
   if err != BlockCountTooLargeError {
      t.Fatalf("%t\n", err)
   }
}

func TestBatchMessageGetBadWaitTime(t *testing.T) {

   awssqs, err := NewAwsSqs( AwsSqsConfig{ } )
   if err != nil {
      t.Fatalf("%t\n", err)
   }

   queueHandle, err := awssqs.QueueHandle( goodQueueName )
   if err != nil {
      t.Fatalf("%t\n", err)
   }

   _, err = awssqs.BatchMessageGet( queueHandle, uint( MAX_SQS_BLOCK_COUNT ), badWaitTime )
   if err != WaitTooLargeError {
      t.Fatalf("%t\n", err)
   }
}

//
// BatchMessagePut method invariant tests
//

func TestBatchMessagePutHappyDay( t *testing.T ) {

   awssqs, err := NewAwsSqs( AwsSqsConfig{ } )
   if err != nil {
      t.Fatalf("%t\n", err)
   }

   queueHandle, err := awssqs.QueueHandle( goodQueueName )
   if err != nil {
      t.Fatalf("%t\n", err)
   }

   count := 1
   messages := makeMessages( count )
   ops, err := awssqs.BatchMessagePut( queueHandle, messages )
   if err != nil {
      t.Fatalf("%t\n", err)
   }
   if allOperationsOK( ops ) == false {
      t.Fatalf("One or more put operations failed\n")
   }
}

func TestBatchMessagePutBadQueueHandle( t *testing.T ) {

   awssqs, err := NewAwsSqs( AwsSqsConfig{ } )
   if err != nil {
      t.Fatalf("%t\n", err)
   }

   count := 1
   messages := makeMessages( count )
   ops, err := awssqs.BatchMessagePut( badQueueHandle, messages )
   if err != BadQueueHandleError {
      t.Fatalf("%t\n", err)
   }
   if allOperationsOK( ops ) == false {
      t.Fatalf("One or more put operations failed\n")
   }
}

func TestBatchMessagePutBadBlockCount( t *testing.T ) {

   awssqs, err := NewAwsSqs( AwsSqsConfig{ } )
   if err != nil {
      t.Fatalf("%t\n", err)
   }

   queueHandle, err := awssqs.QueueHandle( goodQueueName )
   if err != nil {
      t.Fatalf("%t\n", err)
   }

   count := MAX_SQS_BLOCK_COUNT + 1
   messages := makeMessages( count )
   ops, err := awssqs.BatchMessagePut( queueHandle, messages )
   if err != BlockCountTooLargeError {
      t.Fatalf("%t\n", err)
   }
   if allOperationsOK( ops ) == false {
      t.Fatalf("One or more put operations failed\n")
   }
}

//
// BatchMessageDelete method invariant tests
//

func TestBatchMessageDeleteHappyDay( t *testing.T ) {

   awssqs, err := NewAwsSqs( AwsSqsConfig{ } )
   if err != nil {
      t.Fatalf("%t\n", err)
   }

   queueHandle, err := awssqs.QueueHandle( goodQueueName )
   if err != nil {
      t.Fatalf("%t\n", err)
   }

   count := 1
   messages := makeMessages( count )
   ops, err := awssqs.BatchMessagePut( queueHandle, messages )
   if err != nil {
      t.Fatalf("%t\n", err)
   }
   if allOperationsOK( ops ) == false {
      t.Fatalf("One or more put operations failed\n")
   }

   messages, err = awssqs.BatchMessageGet( queueHandle, 1, goodWaitTime )
   if err != nil {
      t.Fatalf("%t\n", err)
   }

   ops, err = awssqs.BatchMessageDelete( queueHandle, messages )
   if err != nil {
      t.Fatalf("%t\n", err)
   }
   if allOperationsOK( ops ) == false {
      t.Fatalf("One or more delete operations failed\n")
   }
}

func TestBatchMessageDeleteBadQueueHandle( t *testing.T ) {

   awssqs, err := NewAwsSqs( AwsSqsConfig{ } )
   if err != nil {
      t.Fatalf("%t\n", err)
   }

   count := 1
   messages := makeMessages( count )
   _, err = awssqs.BatchMessageDelete( badQueueHandle, messages )
   if err != BadQueueHandleError {
      t.Fatalf("%t\n", err)
   }
}

func TestBatchMessageDeleteBadBlockCount( t *testing.T ) {

   awssqs, err := NewAwsSqs( AwsSqsConfig{ } )
   if err != nil {
      t.Fatalf("%t\n", err)
   }

   queueHandle, err := awssqs.QueueHandle( goodQueueName )
   if err != nil {
      t.Fatalf("%t\n", err)
   }

   count := MAX_SQS_BLOCK_COUNT + 1
   messages := makeMessages( count )
   _, err = awssqs.BatchMessageDelete( queueHandle, messages )
   if err != BlockCountTooLargeError {
      t.Fatalf("%t\n", err)
   }
}

func TestBatchMessageDeleteBadDeleteHandle( t *testing.T ) {

   awssqs, err := NewAwsSqs( AwsSqsConfig{ } )
   if err != nil {
      t.Fatalf("%t\n", err)
   }

   queueHandle, err := awssqs.QueueHandle( goodQueueName )
   if err != nil {
      t.Fatalf("%t\n", err)
   }

   count := 1
   messages := makeMessages( count )
   messages[ 0 ].DeleteHandle = badDeleteHandle

   ops, err := awssqs.BatchMessageDelete( queueHandle, messages )
   if err != nil {
      t.Fatalf("%t\n", err)
   }
   if ops[ 0 ] == true {
      t.Fatalf("Delete operation succeeded unexpectidly\n")
   }
}

//
// helper methods
//

func clearQueue( t *testing.T, awssqs AWS_SQS, handle QueueHandle ) {

}

func makeMessages( count int ) [] Message {

   messages := make( []Message, 0, count )
   attributes := make( []Attribute, 0, 1 )
   attributes = append( attributes, Attribute{ "type", "text" } )
   for i := 0;  i < count; i++ {
      messages = append( messages, Message{ Attribs: attributes, Payload: Payload( fmt.Sprintf( "this is message %d", i ) )} )
   }
   return messages
}

func allOperationsOK( ops []OpStatus ) bool {
   for _, b := range ops {
      if b == false {
         return false
      }
   }
   return true
}

//
// end of file
//
