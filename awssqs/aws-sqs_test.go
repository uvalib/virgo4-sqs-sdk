package awssqs

import (
   "testing"
)

var goodQueueName = "virgo4-ingest-test-staging"
var badQueueName = "xxx"

func TestQueueNameToHandleHappyDay(t *testing.T) {

   awssqs, err := NewAwsSqs( AwsSqsConfig{ } )
   if err != nil {
      t.Fatalf("%t\n", err)
   }

   _, err = awssqs.QueueHandle( goodQueueName )
   if err != nil {
      t.Fatalf("%t\n", err)
   }
}

//
// end of file
//
