package chunk

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"

	v1 "spider/api/v1"
)

// Chunk represents an in-memory or persisted chunk of data with its SHA-256 hash.
type Chunk struct {
	Hash   string
	Data   []byte
	Offset int64
	Size   int64
}

// ComputeHash calculates the canonical "sha256:<hex>" string for byte slice.
func ComputeHash(data []byte) string {
	h := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(h[:])
}

// ComputeReaderHash streams data from an io.Reader and calculates its SHA-256 hash.
func ComputeReaderHash(r io.Reader) (string, int64, error) {
	h := sha256.New()
	n, err := io.Copy(h, r)
	if err != nil {
		return "", 0, fmt.Errorf("failed to hash reader: %w", err)
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil)), n, nil
}

// VerifyHash streams r and returns an error if the SHA-256 digest does not match expectedHash.
func VerifyHash(r io.Reader, expectedHash string) error {
	actual, n, err := ComputeReaderHash(r)
	if err != nil {
		return err
	}
	if actual != expectedHash {
		return fmt.Errorf("hash mismatch after %d bytes: expected %s, got %s", n, expectedHash, actual)
	}
	return nil
}

// Chunker splits data streams into fixed-size chunks.
type Chunker struct {
	chunkSize int64
}

// NewChunker creates a Chunker with specified chunk size (defaults to DefaultChunkSize if <= 0).
func NewChunker(chunkSize int64) *Chunker {
	if chunkSize <= 0 {
		chunkSize = v1.DefaultChunkSize
	}
	return &Chunker{chunkSize: chunkSize}
}

// ChunkReader reads from an io.Reader and streams Chunk slices.
func (c *Chunker) ChunkReader(r io.Reader) ([]Chunk, error) {
	var chunks []Chunk
	var offset int64
	buf := make([]byte, c.chunkSize)

	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			chunkBytes := make([]byte, n)
			copy(chunkBytes, buf[:n])
			h := ComputeHash(chunkBytes)
			chunks = append(chunks, Chunk{
				Hash:   h,
				Data:   chunkBytes,
				Offset: offset,
				Size:   int64(n),
			})
			offset += int64(n)
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading chunk at offset %d: %w", offset, err)
		}
	}

	return chunks, nil
}

// ChunkFile reads a file from disk and returns its chunk references and chunk list.
func (c *Chunker) ChunkFile(filePath string) ([]Chunk, []v1.ChunkRef, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer f.Close()

	chunks, err := c.ChunkReader(f)
	if err != nil {
		return nil, nil, err
	}

	refs := make([]v1.ChunkRef, len(chunks))
	for i, ch := range chunks {
		refs[i] = v1.ChunkRef{
			Hash:   ch.Hash,
			Offset: ch.Offset,
			Size:   ch.Size,
		}
	}

	return chunks, refs, nil
}
