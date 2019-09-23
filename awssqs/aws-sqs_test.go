package awssqs

import (
   "crypto/md5"
   "fmt"
   "math/rand"
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

// used when generating random messages
var smallMessageSize = uint( 37628 )
var largeMessageSize = MAX_SQS_MESSAGE_SIZE * 2
var characters = []rune("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

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

   count := uint( rand.Intn( int( MAX_SQS_BLOCK_COUNT ) ) )
   messages := makeStandardMessages( count )
   ops, err := awssqs.BatchMessagePut( queueHandle, messages )
   if err != nil {
      t.Fatalf("%t\n", err)
   }
   if allOperationsOK( ops ) == false {
      t.Fatalf("One or more put operations reported failed incorrectly\n")
   }

   messages, err = awssqs.BatchMessageGet( queueHandle, MAX_SQS_BLOCK_COUNT, goodWaitTime )
   if err != nil {
      t.Fatalf("%t\n", err)
   }

   if uint(len( messages )) != count {
      t.Fatalf("Received a different number of messages than expected (expected: %d, received: %d)\n", count, len( messages ) )
   }
}

func TestCorrectSmallMessageContent(t *testing.T) {

   // seed the RNG because we use it when creating messages
   rand.Seed(time.Now().UnixNano())

   awssqs, err := NewAwsSqs( AwsSqsConfig{ } )
   if err != nil {
      t.Fatalf("%t\n", err)
   }

   queueHandle, err := awssqs.QueueHandle( goodQueueName )
   if err != nil {
      t.Fatalf("%t\n", err)
   }

   clearQueue( t, awssqs, queueHandle )

   count := uint( rand.Intn( int( MAX_SQS_BLOCK_COUNT ) ) )
   messages := makeSmallMessages( count )
   ops, err := awssqs.BatchMessagePut( queueHandle, messages )
   if err != nil {
      t.Fatalf("%t\n", err)
   }
   if allOperationsOK( ops ) == false {
      t.Fatalf("One or more put operations reported failed incorrectly\n")
   }

   messages, err = awssqs.BatchMessageGet( queueHandle, MAX_SQS_BLOCK_COUNT, goodWaitTime )
   if err != nil {
      t.Fatalf("%t\n", err)
   }

   if uint(len( messages )) != count {
      t.Fatalf("Received a different number of messages than expected (expected: %d, received: %d)\n", count, len( messages ) )
   }

   verifyMessages( t, messages )
}

func TestCorrectLargeMessageContent(t *testing.T) {

   // seed the RNG because we use it when creating messages
   rand.Seed(time.Now().UnixNano())

   awssqs, err := NewAwsSqs( AwsSqsConfig{ } )
   if err != nil {
      t.Fatalf("%t\n", err)
   }

   queueHandle, err := awssqs.QueueHandle( goodQueueName )
   if err != nil {
      t.Fatalf("%t\n", err)
   }

   clearQueue( t, awssqs, queueHandle )

   count := uint( rand.Intn( int( MAX_SQS_BLOCK_COUNT ) ) )
   messages := makeLargeMessages( count )
   ops, err := awssqs.BatchMessagePut( queueHandle, messages )
   if err != nil {
      t.Fatalf("%t\n", err)
   }
   if allOperationsOK( ops ) == false {
      t.Fatalf("One or more put operations reported failed incorrectly\n")
   }

   messages, err = awssqs.BatchMessageGet( queueHandle, MAX_SQS_BLOCK_COUNT, goodWaitTime )
   if err != nil {
      t.Fatalf("%t\n", err)
   }

   if uint( len( messages ) ) != count {
      t.Fatalf("Received a different number of messages than expected (expected: %d, received: %d)\n", count, len( messages ) )
   }

   verifyMessages( t, messages )
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

   _, err = awssqs.BatchMessageGet( badQueueHandle, MAX_SQS_BLOCK_COUNT, goodWaitTime )
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

   _, err = awssqs.BatchMessageGet( queueHandle, MAX_SQS_BLOCK_COUNT + 1, goodWaitTime )
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

   _, err = awssqs.BatchMessageGet( queueHandle, MAX_SQS_BLOCK_COUNT, badWaitTime )
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

   count := uint( 1 )
   messages := makeStandardMessages( count )
   ops, err := awssqs.BatchMessagePut( queueHandle, messages )
   if err != nil {
      t.Fatalf("%t\n", err)
   }
   if allOperationsOK( ops ) == false {
      t.Fatalf("One or more put operations reported failed incorrectly\n")
   }
}

func TestBatchMessagePutBadQueueHandle( t *testing.T ) {

   awssqs, err := NewAwsSqs( AwsSqsConfig{ } )
   if err != nil {
      t.Fatalf("%t\n", err)
   }

   count := uint( 1 )
   messages := makeStandardMessages( count )
   ops, err := awssqs.BatchMessagePut( badQueueHandle, messages )
   if err != BadQueueHandleError {
      t.Fatalf("%t\n", err)
   }
   if allOperationsOK( ops ) == false {
      t.Fatalf("One or more put operations reported failed incorrectly\n")
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
   messages := makeStandardMessages( count )
   ops, err := awssqs.BatchMessagePut( queueHandle, messages )
   if err != BlockCountTooLargeError {
      t.Fatalf("%t\n", err)
   }
   if allOperationsOK( ops ) == false {
      t.Fatalf("One or more put operations reported failed incorrectly\n")
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

   count := uint( 1 )
   messages := makeStandardMessages( count )
   ops, err := awssqs.BatchMessagePut( queueHandle, messages )
   if err != nil {
      t.Fatalf("%t\n", err)
   }
   if allOperationsOK( ops ) == false {
      t.Fatalf("One or more put operations reported failed unexpectedly\n")
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
      t.Fatalf("One or more delete operations reported failed unexpectedly\n")
   }
}

func TestBatchMessageDeleteBadQueueHandle( t *testing.T ) {

   awssqs, err := NewAwsSqs( AwsSqsConfig{ } )
   if err != nil {
      t.Fatalf("%t\n", err)
   }

   count := uint( 1 )
   messages := makeStandardMessages( count )
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
   messages := makeStandardMessages( count )
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

   count := uint( 1 )
   messages := makeStandardMessages( count )
   messages[ 0 ].DeleteHandle = badDeleteHandle

   ops, err := awssqs.BatchMessageDelete( queueHandle, messages )
   if err != OneOrMoreOperationsUnsuccessfulError {
      t.Fatalf("%t\n", err)
   }
   if ops[ 0 ] == true {
      t.Fatalf("Delete operation reported success incorrectly\n")
   }
}

func TestSmallMessageSize( t *testing.T ) {

   message := makeSmallMessage( )
   expected := smallMessageSize + attributesSize( message.Attribs )
   actual := message.Size( )
   if expected != actual {
      t.Fatalf("Unexpected size. Expected %d, got %d\n", expected, actual )
   }
}

func TestLargeMessageSize( t *testing.T ) {

   message := makeLargeMessage( )
   expected := largeMessageSize + attributesSize( message.Attribs )
   actual := message.Size( )
   if expected != actual {
      t.Fatalf("Unexpected size. Expected %d, got %d\n", expected, actual )
   }
}

func TestEmptyMessageSize( t *testing.T ) {

   var message Message
   expected := uint( 0 )
   actual := message.Size( )
   if expected != actual {
      t.Fatalf("Unexpected size. Expected %d, got %d\n", expected, actual )
   }
}

//
// helper methods
//

func clearQueue( t *testing.T, awssqs AWS_SQS, handle QueueHandle ) {

   for {
      messages, err := awssqs.BatchMessageGet(handle, MAX_SQS_BLOCK_COUNT, zeroWaitTime)
      if err != nil {
         t.Fatalf("%t\n", err)
      }
      if len( messages ) == 0 {
         break
      }
   }
}

func makeStandardMessages( count uint ) [] Message {

   messages := make( []Message, 0, count )
   i := uint( 0 )
   for i < count {
      messages = append( messages, makeStandardMessage( ) )
      i++
   }
   return messages
}

func makeSmallMessages( count uint ) [] Message {

   messages := make( []Message, 0, count )
   i := uint( 0 )
   for i < count {
      messages = append( messages, makeSmallMessage( ) )
      i++
   }
   return messages
}

func makeLargeMessages( count uint ) [] Message {

   messages := make( []Message, 0, count )
   i := uint( 0 )
   for i < count {
      messages = append( messages, makeLargeMessage( ) )
      i++
   }
   return messages
}

func makeStandardMessage( ) Message {

   attributes := make( []Attribute, 0, 1 )
   attributes = append( attributes, Attribute{ "type", "text" } )
   return Message{ Attribs: attributes, Payload: Payload( fmt.Sprintf( "this is message at %s", time.Now( ) ) )}
}

func makeSmallMessage( ) Message {

   payload := randomString( smallMessageSize )
   hash := fmt.Sprintf("%x", md5.Sum([]byte( payload ) ) )
   attributes := make( []Attribute, 0, 2 )
   attributes = append( attributes, Attribute{ "type", "text" } )
   attributes = append( attributes, Attribute{ "hash", hash } )
   return Message{ Attribs: attributes, Payload: Payload( payload ) }
}

func makeLargeMessage( ) Message {

   payload := randomString( largeMessageSize )
   hash := fmt.Sprintf("%x", md5.Sum([]byte( payload ) ) )
   attributes := make( []Attribute, 0, 2 )
   attributes = append( attributes, Attribute{ "type", "text" } )
   attributes = append( attributes, Attribute{ "hash", hash } )
   return Message{ Attribs: attributes, Payload: Payload( payload ) }
}

func verifyMessages( t *testing.T, messages [] Message ) {

   for _, m := range messages {
      reportedHash := extractAttribute( m.Attribs, "hash" )
      actualHash := fmt.Sprintf("%x", md5.Sum([]byte( m.Payload ) ) )
      if actualHash != reportedHash {
         t.Fatalf("Message signatures do not match (expected: %s, actual %s)\n", reportedHash, actualHash )
      }
   }
}

func extractAttribute( attribs Attributes, name string ) string {
   
   for _, a := range attribs {
      if a.Name == name {
         return a.Value
      }
   }
   return ""
}

func attributesSize( attribs Attributes ) uint {

   var padFactor = 8
   sz := uint( 0 )
   for _, a := range attribs {
      sz += uint( len( a.Name ) + len( a.Value ) + ( 2 * padFactor ) )
   }
   return sz
}

func allOperationsOK( ops []OpStatus ) bool {
   for _, b := range ops {
      if b == false {
         return false
      }
   }
   return true
}

func randomString( size uint ) string {

   b := make([]rune, size )
   for i := range b {
      b[i] = characters[rand.Intn(len(characters))]
   }
   return string(b)
}


//
// end of file
//
