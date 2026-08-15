package source

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	v1 "spider/api/v1"
)

// S3Config holds connection parameters for S3 or MinIO origin.
type S3Config struct {
	Bucket       string
	Endpoint     string // e.g. "http://minio:9000" or "http://localhost:9000"
	Region       string // e.g. "us-east-1"
	AccessKey    string
	SecretKey    string
	UsePathStyle bool
}

// S3Source implements Source for AWS S3 and S3-compatible object stores like MinIO.
type S3Source struct {
	client *s3.Client
	bucket string
}

// NewS3Source creates an S3Source instance.
func NewS3Source(cfg S3Config) (*S3Source, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("S3 bucket name is required")
	}

	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}

	var optFns []func(*config.LoadOptions) error
	optFns = append(optFns, config.WithRegion(region))

	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		optFns = append(optFns, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		))
	}

	awsCfg, err := config.LoadDefaultConfig(context.Background(), optFns...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			endpoint := cfg.Endpoint
			if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
				endpoint = "http://" + endpoint
			}
			o.BaseEndpoint = aws.String(endpoint)
		}
		o.UsePathStyle = cfg.UsePathStyle
	})

	return &S3Source{
		client: client,
		bucket: cfg.Bucket,
	}, nil
}

// ListFiles lists all objects in the bucket under prefix.
func (s *S3Source) ListFiles(ctx context.Context, prefix string) ([]FileInfo, error) {
	var results []FileInfo
	var continuationToken *string

	cleanPrefix := strings.TrimPrefix(v1.NormalizePath(prefix), "/")
	if cleanPrefix != "" && !strings.HasSuffix(cleanPrefix, "/") {
		cleanPrefix += "/"
	}

	for {
		input := &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.bucket),
			Prefix:            aws.String(cleanPrefix),
			ContinuationToken: continuationToken,
		}

		output, err := s.client.ListObjectsV2(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects in bucket %s: %w", s.bucket, err)
		}

		for _, item := range output.Contents {
			key := aws.ToString(item.Key)
			if strings.HasSuffix(key, "/") {
				continue // skip directory markers
			}
			relPath := strings.TrimPrefix(key, cleanPrefix)
			results = append(results, FileInfo{
				Path: v1.NormalizePath(relPath),
				Size: aws.ToInt64(item.Size),
				Mode: "0644",
			})
		}

		if !aws.ToBool(output.IsTruncated) {
			break
		}
		continuationToken = output.NextContinuationToken
	}

	return results, nil
}

// ReadChunk performs a byte-range GET request for the object.
func (s *S3Source) ReadChunk(ctx context.Context, path string, offset int64, size int64) ([]byte, error) {
	key := strings.TrimPrefix(v1.NormalizePath(path), "/")
	rangeHeader := fmt.Sprintf("bytes=%d-%d", offset, offset+size-1)

	input := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Range:  aws.String(rangeHeader),
	}

	output, err := s.client.GetObject(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to get object chunk %s (%s): %w", key, rangeHeader, err)
	}
	defer output.Body.Close()

	buf := make([]byte, size)
	n, err := io.ReadFull(output.Body, buf)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, fmt.Errorf("failed to read chunk payload %s: %w", key, err)
	}

	return buf[:n], nil
}

// Open returns a streaming reader for full object download.
func (s *S3Source) Open(ctx context.Context, path string) (io.ReadCloser, error) {
	key := strings.TrimPrefix(v1.NormalizePath(path), "/")
	input := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}
	output, err := s.client.GetObject(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to open object %s: %w", key, err)
	}
	return output.Body, nil
}
