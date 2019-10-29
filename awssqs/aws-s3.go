package awssqs

import (
	"bytes"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/google/uuid"
)

var downloader *s3manager.Downloader
var uploader *s3manager.Uploader
var s3service *s3.S3

// set up our S3 management objects
func init() {

	sess, err := session.NewSession()
	if err == nil {
		uploader = s3manager.NewUploader(sess)
		downloader = s3manager.NewDownloader(sess)
		s3service = s3.New(sess)
	}
}

// add the buffer to a new S3 object and return the object key
func s3Add(bucket string, contents []byte) (string, error) {

	key := uuid.New().String()
	contentSize := len( contents )

	log.Printf("INFO: uploading to s3://%s/%s (%d bytes)", bucket, key, contentSize)

	upParams := &s3manager.UploadInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(contents),
	}

	start := time.Now()

	// Perform an upload.
	_, err := uploader.Upload(upParams)
	if err != nil {
		return "", err
	}

	// we validate the expected file size against the actually uploaded size
	//if int64( contentSize ) != uploadSize {
	//	return nil, fmt.Errorf("upload failure. expected %d bytes, actual %d bytes", contentSize, uploadSize)
	//}

	duration := time.Since(start)
	log.Printf("INFO: upload of s3://%s/%s complete in %0.2f seconds", bucket, key, duration.Seconds())

	return key, nil
}

// read the contents from the specified S3 object returning a buffer to the contents
func s3Get(bucket string, key string, expectedSize int) ([]byte, error) {

	log.Printf("INFO: downloading from s3://%s/%s (%d bytes)", bucket, key, expectedSize)

	start := time.Now()

	buff := &aws.WriteAtBuffer{}
	downloadSize, err := downloader.Download(buff,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})

	if err != nil {
		return nil, err
	}

	// we validate the expected file size against the actually downloaded size
	if int64( expectedSize ) != downloadSize {
		return nil, fmt.Errorf("download failure. expected %d bytes, received %d bytes", expectedSize, downloadSize)
	}

	duration := time.Since(start)
	log.Printf("INFO: download of s3://%s/%s complete in %0.2f seconds", bucket, key, duration.Seconds())

	return buff.Bytes(), nil
}

// delete the specified S3 object
func s3Delete(bucket string, key string) error {

	log.Printf("INFO: deleting s3://%s/%s", bucket, key)

	start := time.Now()
	_, err := s3service.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return err
	}

	duration := time.Since(start)
	log.Printf("INFO: delete of s3://%s/%s complete in %0.2f seconds", bucket, key, duration.Seconds())
	return nil
}

//
// end of file
//
