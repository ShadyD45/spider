package cache

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	v1 "spider/api/v1"
	"spider/pkg/chunk"
)

var (
	ErrChunkNotFound   = errors.New("chunk not found in cache")
	ErrHashMismatch    = errors.New("chunk content hash mismatch")
	ErrInvalidHashName = errors.New("invalid chunk hash format")
)

// Cache manages the disk-backed content-addressed store.
type Cache struct {
	rootDir   string
	chunksDir string
	tmpDir    string
	mfstDir   string
	mu        sync.RWMutex
}

// NewCache initializes cache directories at rootDir.
func NewCache(rootDir string) (*Cache, error) {
	if rootDir == "" {
		rootDir = "/var/lib/spider"
	}

	c := &Cache{
		rootDir:   rootDir,
		chunksDir: filepath.Join(rootDir, "chunks", "sha256"),
		tmpDir:    filepath.Join(rootDir, "tmp"),
		mfstDir:   filepath.Join(rootDir, "manifests"),
	}

	for _, dir := range []string{c.chunksDir, c.tmpDir, c.mfstDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create cache directory %s: %w", dir, err)
		}
	}

	return c, nil
}

// parseHash extracts the 64-character hex part from "sha256:<hex>".
func parseHash(hash string) (string, error) {
	if !strings.HasPrefix(hash, "sha256:") {
		return "", ErrInvalidHashName
	}
	hexPart := strings.TrimPrefix(hash, "sha256:")
	if len(hexPart) != 64 {
		return "", ErrInvalidHashName
	}
	return hexPart, nil
}

// chunkPath returns the hierarchical filesystem path for a chunk hash (e.g. chunks/sha256/ab/abcdef...).
func (c *Cache) chunkPath(hash string) (string, error) {
	hexPart, err := parseHash(hash)
	if err != nil {
		return "", err
	}
	shard := hexPart[:2]
	return filepath.Join(c.chunksDir, shard, hexPart), nil
}

// HasChunk checks if a verified chunk exists in cache.
func (c *Cache) HasChunk(hash string) bool {
	p, err := c.chunkPath(hash)
	if err != nil {
		return false
	}
	info, err := os.Stat(p)
	return err == nil && !info.IsDir()
}

// RootDir returns the cache root.
func (c *Cache) RootDir() string { return c.rootDir }

// GetChunkPath returns the absolute path to a chunk file if it exists.
func (c *Cache) GetChunkPath(hash string) (string, error) {
	p, err := c.chunkPath(hash)
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(p); err != nil {
		if os.IsNotExist(err) {
			return "", ErrChunkNotFound
		}
		return "", err
	}
	return p, nil
}

// GetChunkReader opens a chunk file for reading and returns its size.
func (c *Cache) GetChunkReader(hash string) (io.ReadCloser, int64, error) {
	p, err := c.chunkPath(hash)
	if err != nil {
		return nil, 0, err
	}

	f, err := os.Open(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, ErrChunkNotFound
		}
		return nil, 0, err
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, err
	}

	return f, info.Size(), nil
}

// PutChunk stores byte slice atomically, strictly verifying SHA-256 of the bytes on disk before commit.
func (c *Cache) PutChunk(hash string, data []byte) error {
	if _, err := parseHash(hash); err != nil {
		return err
	}
	return c.PutChunkFromReader(hash, bytes.NewReader(data))
}

// PutChunkFromReader streams data from reader, writes to tmp, re-hashes the on-disk bytes, and atomically commits.
func (c *Cache) PutChunkFromReader(hash string, r io.Reader) error {
	if _, err := parseHash(hash); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	destPath, err := c.chunkPath(hash)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
		return fmt.Errorf("failed to create shard directory: %w", err)
	}

	tmpFile, err := os.CreateTemp(c.tmpDir, "chunk-*")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	tmpName := tmpFile.Name()
	defer func() {
		_ = os.Remove(tmpName)
	}()

	if _, err := io.Copy(tmpFile, r); err != nil {
		tmpFile.Close()
		return fmt.Errorf("failed to copy chunk stream: %w", err)
	}
	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		return fmt.Errorf("failed to sync temp chunk: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	// Re-hash the written file so commit is based on durable bytes, not the in-memory buffer.
	onDisk, err := os.Open(tmpName)
	if err != nil {
		return fmt.Errorf("failed to reopen temp chunk for verification: %w", err)
	}
	actual, _, err := chunk.ComputeReaderHash(onDisk)
	_ = onDisk.Close()
	if err != nil {
		return fmt.Errorf("failed to hash temp chunk: %w", err)
	}
	if actual != hash {
		return fmt.Errorf("%w: expected %s, computed %s", ErrHashMismatch, hash, actual)
	}

	if err := os.Rename(tmpName, destPath); err != nil {
		return fmt.Errorf("failed to move chunk to cache: %w", err)
	}

	return nil
}

// SaveManifest persists an artifact manifest to cache/manifests/<artifact_id>.json.
func (c *Cache) SaveManifest(m *v1.ArtifactManifest) error {
	if err := m.Validate(); err != nil {
		return fmt.Errorf("cannot save invalid manifest: %w", err)
	}

	hexPart, err := parseHash(m.ArtifactID)
	if err != nil {
		return err
	}

	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal manifest: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	dest := filepath.Join(c.mfstDir, hexPart+".json")
	tmpFile, err := os.CreateTemp(c.tmpDir, "manifest-*")
	if err != nil {
		return err
	}
	tmpName := tmpFile.Name()
	defer func() { _ = os.Remove(tmpName) }()

	if _, err := tmpFile.Write(data); err != nil {
		tmpFile.Close()
		return err
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}

	return os.Rename(tmpName, dest)
}

// GetManifest loads an artifact manifest from cache.
func (c *Cache) GetManifest(artifactID string) (*v1.ArtifactManifest, error) {
	hexPart, err := parseHash(artifactID)
	if err != nil {
		return nil, err
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	dest := filepath.Join(c.mfstDir, hexPart+".json")
	data, err := os.ReadFile(dest)
	if err != nil {
		return nil, err
	}

	return v1.ParseManifest(data)
}

// ListChunks returns all verified chunk hashes currently stored in the cache.
func (c *Cache) ListChunks() ([]string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var hashes []string
	err := filepath.Walk(c.chunksDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			base := filepath.Base(path)
			if len(base) == 64 {
				hashes = append(hashes, "sha256:"+base)
			}
		}
		return nil
	})

	return hashes, err
}

// TotalCachedBytes calculates total bytes used by chunk files.
func (c *Cache) TotalCachedBytes() (int64, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var total int64
	err := filepath.Walk(c.chunksDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			total += info.Size()
		}
		return nil
	})

	return total, err
}

// DeleteChunk removes a chunk from the cache.
func (c *Cache) DeleteChunk(hash string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	p, err := c.chunkPath(hash)
	if err != nil {
		return err
	}
	return os.Remove(p)
}
