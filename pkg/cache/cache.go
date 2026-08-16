package cache

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	v1 "spider/api/v1"
	"spider/pkg/chunk"
)

var (
	ErrChunkNotFound   = errors.New("chunk not found in cache")
	ErrHashMismatch    = errors.New("chunk content hash mismatch")
	ErrInvalidHashName = errors.New("invalid chunk hash format")
	ErrIncomplete      = errors.New("chunk partial is incomplete")
)

// ChunkStore manages the disk-backed content-addressed chunk store.
type ChunkStore struct {
	rootDir    string
	chunksDir  string
	tmpDir     string
	partialDir string
	mfstDir    string
}

// Cache is a compatibility alias for ChunkStore.
type Cache = ChunkStore

// NewChunkStore initializes chunk store directories at rootDir.
func NewChunkStore(rootDir string) (*ChunkStore, error) {
	if rootDir == "" {
		rootDir = "/var/lib/spider"
	}

	c := &ChunkStore{
		rootDir:    rootDir,
		chunksDir:  filepath.Join(rootDir, "chunks", "sha256"),
		tmpDir:     filepath.Join(rootDir, "tmp"),
		partialDir: filepath.Join(rootDir, "tmp", "partial"),
		mfstDir:    filepath.Join(rootDir, "manifests"),
	}

	for _, dir := range []string{c.chunksDir, c.tmpDir, c.partialDir, c.mfstDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create cache directory %s: %w", dir, err)
		}
	}

	return c, nil
}

// NewCache is a compatibility alias for NewChunkStore.
func NewCache(rootDir string) (*ChunkStore, error) {
	return NewChunkStore(rootDir)
}

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

func (c *ChunkStore) chunkPath(hash string) (string, error) {
	hexPart, err := parseHash(hash)
	if err != nil {
		return "", err
	}
	shard := hexPart[:2]
	return filepath.Join(c.chunksDir, shard, hexPart), nil
}

func (c *ChunkStore) partialPath(hash string) (string, error) {
	hexPart, err := parseHash(hash)
	if err != nil {
		return "", err
	}
	return filepath.Join(c.partialDir, hexPart), nil
}

// HasChunk checks if a verified chunk exists in the store.
func (c *ChunkStore) HasChunk(hash string) bool {
	p, err := c.chunkPath(hash)
	if err != nil {
		return false
	}
	info, err := os.Stat(p)
	return err == nil && !info.IsDir()
}

// CommittedChunkSize returns the on-disk size of a committed chunk, if present.
func (c *ChunkStore) CommittedChunkSize(hash string) (int64, bool) {
	p, err := c.chunkPath(hash)
	if err != nil {
		return 0, false
	}
	info, err := os.Stat(p)
	if err != nil || info.IsDir() {
		return 0, false
	}
	return info.Size(), true
}

// HasValidCommittedChunk reports whether a committed chunk exists and matches expectedSize when set.
func (c *ChunkStore) HasValidCommittedChunk(hash string, expectedSize int64) bool {
	if !c.HasChunk(hash) {
		return false
	}
	if expectedSize <= 0 {
		return true
	}
	sz, ok := c.CommittedChunkSize(hash)
	return ok && sz == expectedSize
}

// RootDir returns the store root.
func (c *ChunkStore) RootDir() string { return c.rootDir }

// GetChunkPath returns the absolute path to a committed chunk file if it exists.
func (c *ChunkStore) GetChunkPath(hash string) (string, error) {
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

// GetChunkReader opens a committed chunk for reading (seekable).
func (c *ChunkStore) GetChunkReader(hash string) (io.ReadSeekCloser, int64, error) {
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

// PartialSize returns durable bytes already written for an incomplete chunk.
func (c *ChunkStore) PartialSize(hash string) int64 {
	p, err := c.partialPath(hash)
	if err != nil {
		return 0
	}
	info, err := os.Stat(p)
	if err != nil || info.IsDir() {
		return 0
	}
	return info.Size()
}

// DiscardPartial removes an incomplete download for hash.
func (c *ChunkStore) DiscardPartial(hash string) error {
	p, err := c.partialPath(hash)
	if err != nil {
		return err
	}
	if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// PutChunk stores a byte slice atomically after verifying SHA-256.
func (c *ChunkStore) PutChunk(hash string, data []byte) error {
	return c.PutChunkFromReader(hash, bytes.NewReader(data))
}

// PutChunkFromReader replaces any partial, streams to a hash-named file, verifies, and commits.
func (c *ChunkStore) PutChunkFromReader(hash string, r io.Reader) error {
	if err := c.DiscardPartial(hash); err != nil {
		return err
	}
	if err := c.AppendPartial(hash, r); err != nil {
		_ = c.DiscardPartial(hash)
		return err
	}
	return c.CommitPartial(hash)
}

// AppendPartial appends reader bytes onto the hash-named partial file (no global lock).
func (c *ChunkStore) AppendPartial(hash string, r io.Reader) error {
	if _, err := parseHash(hash); err != nil {
		return err
	}
	p, err := c.partialPath(hash)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		return err
	}
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open partial: %w", err)
	}
	if _, err := io.Copy(f, r); err != nil {
		_ = f.Close()
		return fmt.Errorf("write partial: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync partial: %w", err)
	}
	return f.Close()
}

// CommitPartial verifies the full partial against hash and atomically inserts it into the CAS.
func (c *ChunkStore) CommitPartial(hash string) error {
	if _, err := parseHash(hash); err != nil {
		return err
	}
	p, err := c.partialPath(hash)
	if err != nil {
		return err
	}
	onDisk, err := os.Open(p)
	if err != nil {
		return fmt.Errorf("open partial for verify: %w", err)
	}
	actual, _, err := chunk.ComputeReaderHash(onDisk)
	_ = onDisk.Close()
	if err != nil {
		return fmt.Errorf("hash partial: %w", err)
	}
	if actual != hash {
		_ = os.Remove(p)
		return fmt.Errorf("%w: expected %s, computed %s", ErrHashMismatch, hash, actual)
	}

	destPath, err := c.chunkPath(hash)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
		return fmt.Errorf("failed to create shard directory: %w", err)
	}
	if err := os.Rename(p, destPath); err != nil {
		return fmt.Errorf("failed to move chunk to cache: %w", err)
	}
	return nil
}

// HashPartial returns the SHA-256 of the current partial file.
func (c *ChunkStore) HashPartial(hash string) (string, error) {
	p, err := c.partialPath(hash)
	if err != nil {
		return "", err
	}
	f, err := os.Open(p)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}

// SaveManifest persists an artifact manifest to manifests/<artifact_id>.json.
func (c *ChunkStore) SaveManifest(m *v1.ArtifactManifest) error {
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

// GetManifest loads an artifact manifest from the store.
func (c *ChunkStore) GetManifest(artifactID string) (*v1.ArtifactManifest, error) {
	hexPart, err := parseHash(artifactID)
	if err != nil {
		return nil, err
	}

	dest := filepath.Join(c.mfstDir, hexPart+".json")
	data, err := os.ReadFile(dest)
	if err != nil {
		return nil, err
	}

	return v1.ParseManifest(data)
}

// ListChunks returns all verified chunk hashes currently stored.
func (c *ChunkStore) ListChunks() ([]string, error) {
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

// TotalCachedBytes calculates total bytes used by committed chunk files.
func (c *ChunkStore) TotalCachedBytes() (int64, error) {
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

// DeleteChunk removes a committed chunk.
func (c *ChunkStore) DeleteChunk(hash string) error {
	p, err := c.chunkPath(hash)
	if err != nil {
		return err
	}
	return os.Remove(p)
}
