package awssqs

import (
   "bytes"
   "log"
   "time"

   "github.com/google/uuid"
   "github.com/aws/aws-sdk-go/aws"
   "github.com/aws/aws-sdk-go/aws/session"
   "github.com/aws/aws-sdk-go/service/s3"
   "github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var downloader * s3manager.Downloader
var uploader * s3manager.Uploader
var s3service * s3.S3

// set up our S3 management objects
func init( ) {

   sess, err := session.NewSession( )
   if err == nil {
      uploader = s3manager.NewUploader( sess )
      downloader = s3manager.NewDownloader( sess )
      s3service = s3.New( sess )
   }
}

func s3Add( bucket string, contents []byte ) ( string, error ) {

   key := uuid.New( ).String( )

   log.Printf( "INFO: uploading to s3://%s/%s (%d bytes)", bucket, key, len( contents ) )

   upParams := &s3manager.UploadInput{
      Bucket: &bucket,
      Key:    &key,
      Body:   bytes.NewReader( contents ),
   }

   start := time.Now()

   // Perform an upload.
   _, err := uploader.Upload(upParams)
   if err != nil {
      return "", err
   }

   duration := time.Since(start)
   log.Printf("Upload of s3://%s/%s complete in %0.2f seconds", bucket, key, duration.Seconds() )

   return key, nil
}

func s3Get( bucket string, key string, size int ) ( []byte, error ) {

   log.Printf( "INFO: downloading from s3://%s/%s (%d bytes)", bucket, key, size )

   start := time.Now()

   buff := &aws.WriteAtBuffer{}
   _, err := downloader.Download( buff,
      &s3.GetObjectInput{
         Bucket: aws.String(bucket),
         Key:    aws.String(key),
      })

   if err != nil {
      return nil, err
   }

   duration := time.Since(start)
   log.Printf("Download of s3://%s/%s complete in %0.2f seconds", bucket, key, duration.Seconds() )

   return buff.Bytes(), nil
}

func s3Delete( bucket string, key string ) error {

   log.Printf( "INFO: deleting s3://%s/%s", bucket, key )

   start := time.Now()
   _, err := s3service.DeleteObject( &s3.DeleteObjectInput{ Bucket: aws.String(bucket), Key: aws.String( key ) } )
   if err != nil {
      return err
   }

   duration := time.Since(start)
   log.Printf("Delete of s3://%s/%s complete in %0.2f seconds", bucket, key, duration.Seconds() )
   return nil
}

//
// end of file
//
