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

var emptyOpList = make([]OpStatus, 0)
var emptyMessageList = make([]Message, 0)

// this is our interface implementation
type awsSqsImpl struct {
	config AwsSqsConfig
	svc    *sqs.SQS
}

// factory for our SQS interface
func newAwsSqs(config AwsSqsConfig) (AWS_SQS, error) {

	// validate the inbound configuration
	if len(config.MessageBucketName) == 0 {
		return nil, ErrMissingConfiguration
	}

	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}

	svc := sqs.New(sess)

	return &awsSqsImpl{config, svc}, nil
}

// QueueHandle get a queue handle (URL) when provided a queue name
func (awsi *awsSqsImpl) QueueHandle(queueName string) (QueueHandle, error) {

	// get the queue URL from the name
	result, err := awsi.svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})

	if err != nil {
		if strings.HasPrefix(err.Error(), sqs.ErrCodeQueueDoesNotExist) {
			return "", ErrBadQueueName
		}
		return "", err
	}

	return QueueHandle(*result.QueueUrl), nil
}

// GetMessagesAvailable get the number of messages available in the specified queue
func (awsi *awsSqsImpl) GetMessagesAvailable(queueName string) (uint, error) {

	// get the queue handle
	queue, err := awsi.QueueHandle(queueName)
	if err != nil {
		return 0, err
	}

	// and get the necessary attribute
	q := string(queue)
	attr := "ApproximateNumberOfMessages"
	res, err := awsi.svc.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		QueueUrl: &q,
		AttributeNames: []*string{
			&attr,
		},
	})
	if err != nil {
		return 0, err
	}

	count, _ := strconv.Atoi(*res.Attributes[attr])
	return uint(count), nil
}

// BatchMessageGet get a batch of messages from the specified queue. Will return on receipt of any messages
// without waiting and will wait no longer than the wait time if no messages are received.
func (awsi *awsSqsImpl) BatchMessageGet(queue QueueHandle, maxMessages uint, waitTime time.Duration) ([]Message, error) {

	// ensure the block size is not too large
	if maxMessages > MAX_SQS_BLOCK_COUNT {
		return emptyMessageList, ErrBlockCountTooLarge
	}

	// ensure the wait time is not too large
	if waitTime.Seconds() > float64(MAX_SQS_WAIT_TIME) {
		return emptyMessageList, ErrWaitTooLarge
	}

	q := string(queue)

	start := time.Now()
	result, err := awsi.svc.ReceiveMessage(&sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		QueueUrl:            &q,
		MaxNumberOfMessages: aws.Int64(int64(maxMessages)),
		WaitTimeSeconds:     aws.Int64(int64(waitTime.Seconds())),
	})
	elapsed := int64(time.Since(start) / time.Millisecond)

	if err != nil {
		if strings.HasPrefix(err.Error(), sqs.ErrCodeQueueDoesNotExist) {
			return emptyMessageList, ErrBadQueueHandle
		}
		return emptyMessageList, err
	}

	// if we did not get any messages
	sz := len(result.Messages)
	if sz == 0 {
		return emptyMessageList, nil
	}

	// we want to warn if the receive took a long time (and yielded messages)
	warnIfSlow(elapsed, "ReceiveMessage")

	// build the response message set from the returned AWS structures
	messages := make([]Message, 0, sz)
	var returnErr error
	wasError := false
	for _, m := range result.Messages {
		// make a new message and append to the list
		m, err := MakeMessage(*m)
		messages = append(messages, *m)
		if err != nil {
			// sometimes we have incomplete messages so capture that info here...
			// incomplete messages are marked as such so can be handled elsewhere
			wasError = true
			returnErr = err
		}
	}

	// if one (or more) error occurred, return it with the list of messages
	if wasError == true {
		return messages, returnErr
	}
	return messages, nil
}

// BatchMessagePut put a batch of messages to the specified queue.
// in the event of one or more failure, the operation status array will indicate which
// messages were processed successfully and which were not.
func (awsi *awsSqsImpl) BatchMessagePut(queue QueueHandle, messages []Message) ([]OpStatus, error) {

	// early exit if no messages provided
	sz := len(messages)
	if sz == 0 {
		return emptyOpList, nil
	}

	// ensure the block size is not too large
	if uint(sz) > MAX_SQS_BLOCK_COUNT {
		return emptyOpList, ErrBlockCountTooLarge
	}

	// our operation status array
	ops := make([]OpStatus, sz)

	// initialize the operation status array to all successful and convert any
	// oversize messages (use index access to the array because this updates the messages)
	for ix := range messages {
		ops[ix] = true

		sz := messages[ix].Size()
		if sz > MAX_SQS_MESSAGE_SIZE {
			err := messages[ix].ConvertToOversizeMessage(awsi.config.MessageBucketName)
			if err != nil {
				log.Printf("WARNING: failed converting oversize message, ignoring further processing for it")
				ops[ix] = false
			}
		}
	}

	// calculate the total block size and the number of messages that are larger than the maximum message size
	var totalSize uint = 0
	for ix := range messages {
		totalSize += messages[ix].Size()
	}

	// if the total block size is too large then we can split the block in half and handle each one individually
	if totalSize > MAX_SQS_BLOCK_SIZE {
		half := sz / 2
		if half == 0 {
			// an insane situation, bomb out
			log.Fatalf("ERROR: cannot split block further, aborting")
		}
		log.Printf("INFO: blocksize too large, splitting at %d", half)
		op1, err1 := awsi.BatchMessagePut(queue, messages[0:half])
		op2, err2 := awsi.BatchMessagePut(queue, messages[half:])
		op1 = append(op1, op2...)
		if err1 != nil {
			return op1, err1
		} else {
			return op1, err2
		}
	}

	q := string(queue)
	mGroup := ""
	if strings.HasSuffix(q, "fifo") == true {
		mGroup = "default"
	}

	batch := make([]*sqs.SendMessageBatchRequestEntry, 0, sz)

	// make a batch of messages that we successfully processed so far
	for ix, m := range messages {
		if ops[ix] == true {
			batch = append(batch, constructSend(m, ix, mGroup))
		}
	}

	start := time.Now()
	response, err := awsi.svc.SendMessageBatch(&sqs.SendMessageBatchInput{
		Entries:  batch,
		QueueUrl: &q,
	})
	elapsed := int64(time.Since(start) / time.Millisecond)

	// we want to warn if the receive took a long time
	warnIfSlow(elapsed, "SendMessageBatch")

	if err != nil {
		if strings.HasPrefix(err.Error(), sqs.ErrCodeQueueDoesNotExist) {
			return emptyOpList, ErrBadQueueHandle
		}
		return emptyOpList, err
	}

	for _, f := range response.Failed {
		log.Printf("WARNING: ID %s send not successful (%s)", *f.Id, *f.Message)
		id, converr := strconv.Atoi(*f.Id)
		if converr == nil && id < sz {
			ops[id] = false
		}
	}

	// if any of the operation statuses are failures, return an error indicating so
	for _, b := range ops {
		if b == false {
			return ops, ErrOneOrMoreOperationsUnsuccessful
		}
	}

	return ops, nil
}

// BatchMessageDelete mark a batch of messages from the specified queue as suitable for delete. This mechanism
// prevents messages from being reprocessed.
func (awsi *awsSqsImpl) BatchMessageDelete(queue QueueHandle, messages []Message) ([]OpStatus, error) {

	// early exit if no messages provided
	var sz = uint(len(messages))
	if sz == 0 {
		return emptyOpList, nil
	}

	// ensure the block size is not too large
	if sz > MAX_SQS_BLOCK_COUNT {
		return emptyOpList, ErrBlockCountTooLarge
	}

	q := string(queue)

	batch := make([]*sqs.DeleteMessageBatchRequestEntry, 0, sz)
	ops := make([]OpStatus, sz)

	// the SQS delete loop, initially, assume everything works
	for ix, m := range messages {
		ops[ix] = true
		batch = append(batch, constructDelete(m.GetReceiptHandle(), ix))
	}

	start := time.Now()
	response, err := awsi.svc.DeleteMessageBatch(&sqs.DeleteMessageBatchInput{
		Entries:  batch,
		QueueUrl: &q,
	})
	elapsed := int64(time.Since(start) / time.Millisecond)

	// we want to warn if the receive took a long time
	warnIfSlow(elapsed, "DeleteMessageBatch")

	if err != nil {
		if strings.HasPrefix(err.Error(), sqs.ErrCodeQueueDoesNotExist) {
			return emptyOpList, ErrBadQueueHandle
		}
		return emptyOpList, err
	}

	for _, f := range response.Failed {
		log.Printf("WARNING: ID %s delete not successful (%s)", *f.Id, *f.Message)
		id, converr := strconv.Atoi(*f.Id)
		if converr == nil && uint(id) < sz {
			ops[id] = false
		} else {
			log.Printf("WARNING: suspect ID %s in delete response", *f.Id)
		}
	}

	// we have now deleted the messages from SQS, delete any oversize payloads from S3
	for _, f := range response.Successful {
		id, converr := strconv.Atoi(*f.Id)
		if converr == nil && uint(id) < sz {
			if messages[id].IsOversize() == true {
				deleteError := messages[id].DeleteOversizeMessage()
				if deleteError != nil {
					log.Printf("WARNING: failed deleting oversize message")
					ops[id] = false
				}
			}
		} else {
			log.Printf("WARNING: suspect ID %s in delete response", *f.Id)
		}
	}

	// if any of the operation statuses are failures, return an error indicating so
	for _, b := range ops {
		if b == false {
			return ops, ErrOneOrMoreOperationsUnsuccessful
		}
	}

	return ops, nil
}

// MessagePutRetry retry a batched put after one or more of the operations fails.
// retry the specified amount of times and return an error of after retrying one or messages
// has still not been sent successfully.
func (awsi *awsSqsImpl) MessagePutRetry(queue QueueHandle, messages []Message, opStatus []OpStatus, retries uint) error {

	// if we made it here then there is still operations outstanding and we have run out of attempts.
	// just return an error
	if retries == 0 {
		log.Printf("ERROR: out of retries, giving up")
		return ErrOneOrMoreOperationsUnsuccessful
	}

	// create the retry batch
	retryBatch := make([]Message, 0)
	for ix, op := range opStatus {
		if op == false {
			retryBatch = append(retryBatch, messages[ix])
		}
	}

	// make sure there are items to retry... if not return success
	sz := len(retryBatch)
	if sz == 0 {
		return nil
	}

	// sleep for a while
	time.Sleep(100 * time.Millisecond)

	log.Printf("INFO: retrying %d item(s)... (%d remaining tries)", sz, retries)

	opStatusRetry, err := awsi.BatchMessagePut(queue, retryBatch)
	// if success then we are done
	if err == nil {
		return nil
	}

	// if not success, anything other than an error we can retry is fatal so give up
	if err != ErrOneOrMoreOperationsUnsuccessful {
		return err
	}

	// try again and reduce the retries count
	return awsi.MessagePutRetry(queue, retryBatch, opStatusRetry, retries-1)
}

//
// end of file
//
