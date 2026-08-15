package source

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"
)

// FileInfo contains metadata for an object in source storage.
type FileInfo struct {
	Path string // Relative forward-slash path
	Size int64  // Size in bytes
	Mode string // Permission mode string (e.g. 0644)
}

// Source abstracts external seed or origin storage providers.
type Source interface {
	// ListFiles returns all files located under the specified prefix.
	ListFiles(ctx context.Context, prefix string) ([]FileInfo, error)

	// ReadChunk reads a byte slice from path starting at offset with specified size.
	ReadChunk(ctx context.Context, path string, offset int64, size int64) ([]byte, error)

	// Open returns a full streaming reader for the given path.
	Open(ctx context.Context, path string) (io.ReadCloser, error)
}

// ParseSourceURI creates the appropriate Source instance from a URI string (e.g. file:///path, s3://bucket/path, minio://bucket/path).
func ParseSourceURI(ctx context.Context, uri string, s3Endpoint string, s3Region string, s3AccessKey string, s3SecretKey string) (Source, string, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, "", fmt.Errorf("invalid source URI %q: %w", uri, err)
	}

	switch u.Scheme {
	case "file", "":
		path := u.Path
		if u.Scheme == "" {
			path = uri
		}
		fsSrc, err := NewFilesystemSource(path)
		if err != nil {
			return nil, "", err
		}
		return fsSrc, "", nil

	case "s3", "minio":
		bucket := u.Host
		prefix := strings.TrimPrefix(u.Path, "/")
		s3Src, err := NewS3Source(S3Config{
			Bucket:       bucket,
			Endpoint:     s3Endpoint,
			Region:       s3Region,
			AccessKey:    s3AccessKey,
			SecretKey:    s3SecretKey,
			UsePathStyle: true,
		})
		if err != nil {
			return nil, "", err
		}
		return s3Src, prefix, nil

	default:
		return nil, "", fmt.Errorf("unsupported source scheme %q", u.Scheme)
	}
}
