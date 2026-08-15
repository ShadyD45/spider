package source

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	v1 "spider/api/v1"
)

// FilesystemSource implements Source for a local directory.
type FilesystemSource struct {
	baseDir string
}

// NewFilesystemSource creates a new FilesystemSource root.
func NewFilesystemSource(baseDir string) (*FilesystemSource, error) {
	abs, err := filepath.Abs(baseDir)
	if err != nil {
		return nil, fmt.Errorf("invalid base directory %s: %w", baseDir, err)
	}
	info, err := os.Stat(abs)
	if err != nil {
		return nil, fmt.Errorf("base directory not accessible %s: %w", abs, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("path %s is not a directory", abs)
	}
	return &FilesystemSource{baseDir: abs}, nil
}

func (fs *FilesystemSource) resolve(relPath string) (string, error) {
	cleanRel := filepath.FromSlash(v1.NormalizePath(relPath))
	full := filepath.Join(fs.baseDir, cleanRel)
	// Security check to avoid path traversal escapes
	rel, err := filepath.Rel(fs.baseDir, full)
	if err != nil || strings.HasPrefix(rel, "..") {
		return "", fmt.Errorf("path escapes source root: %s", relPath)
	}
	return full, nil
}

// ListFiles recursively scans baseDir under prefix.
func (fs *FilesystemSource) ListFiles(ctx context.Context, prefix string) ([]FileInfo, error) {
	searchDir := fs.baseDir
	if prefix != "" {
		var err error
		searchDir, err = fs.resolve(prefix)
		if err != nil {
			return nil, err
		}
	}

	var results []FileInfo
	err := filepath.Walk(searchDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if !info.IsDir() {
			rel, err := filepath.Rel(fs.baseDir, path)
			if err != nil {
				return err
			}
			normPath := v1.NormalizePath(rel)
			modeStr := fmt.Sprintf("%04o", info.Mode().Perm())
			results = append(results, FileInfo{
				Path: normPath,
				Size: info.Size(),
				Mode: modeStr,
			})
		}
		return nil
	})

	return results, err
}

// ReadChunk reads a byte slice from a file.
func (fs *FilesystemSource) ReadChunk(ctx context.Context, path string, offset int64, size int64) ([]byte, error) {
	full, err := fs.resolve(path)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(full)
	if err != nil {
		return nil, fmt.Errorf("failed to open source file %s: %w", path, err)
	}
	defer f.Close()

	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek in source file %s to %d: %w", path, offset, err)
	}

	buf := make([]byte, size)
	n, err := io.ReadFull(f, buf)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, fmt.Errorf("failed to read %d bytes from %s: %w", size, path, err)
	}

	return buf[:n], nil
}

// Open returns a streaming reader for a file.
func (fs *FilesystemSource) Open(ctx context.Context, path string) (io.ReadCloser, error) {
	full, err := fs.resolve(path)
	if err != nil {
		return nil, err
	}
	return os.Open(full)
}
