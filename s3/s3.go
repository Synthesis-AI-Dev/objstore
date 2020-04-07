package s3

import (
	"bytes"
	"compress/zlib"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"time"

	"github.com/Synthesis-AI-Dev/objstore"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// S3 implements the objstore.Store interface for working with S3.
type S3 struct {
	client *s3.S3
}

// New is the S3 constructor.
func New(config aws.Config) *S3 {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Config:            config,
		SharedConfigState: session.SharedConfigEnable,
	}))

	return &S3{
		client: s3.New(sess),
	}
}

// GetPresignedURL constructs a URL for a bucket and key that are not publicly
// accessible.
func (s *S3) GetPresignedURL(bucket, key string, expire time.Duration) (string, error) {
	req, _ := s.client.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	return req.Presign(expire)
}

// Upload sends the body to the S3 bucket/key
func (s *S3) Upload(body io.Reader, bucket, key string, opts objstore.Options) error {
	if bucket == "" {
		return fmt.Errorf("bucket name cannot be empty")
	}
	if key == "" {
		return fmt.Errorf("key name cannot be empty")
	}
	if objstore.UseCompression(opts) {
		var buff bytes.Buffer
		zWriter := zlib.NewWriter(&buff)
		defer zWriter.Close()
		b, err := ioutil.ReadAll(body)
		if err != nil {
			return err
		}
		if _, err := zWriter.Write(b); err != nil {
			return err
		}
		body = bytes.NewReader(buff.Bytes())
	}
	uploader := s3manager.NewUploaderWithClient(s.client)
	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   body,
	})
	return err
}

// Download downloads the named key from the bucket and returns its contents
// or an error.
func (s *S3) Download(bucket, key string, opts objstore.Options) ([]byte, error) {
	downloader := s3manager.NewDownloaderWithClient(s.client)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	buff := &aws.WriteAtBuffer{}
	_, err := downloader.DownloadWithContext(ctx, buff, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	if objstore.UseCompression(opts) {
		body, err := zlib.NewReader(bytes.NewReader(buff.Bytes()))
		if err != nil {
			return nil, err
		}
		defer body.Close()

		return ioutil.ReadAll(body)
	}
	return buff.Bytes(), err
}
