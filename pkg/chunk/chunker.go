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

// ChunkStream reads r one chunk at a time and invokes onChunk. The callback must
// copy or persist ch.Data before returning if it needs the payload later.
func (c *Chunker) ChunkStream(r io.Reader, onChunk func(Chunk) error) error {
	if onChunk == nil {
		return fmt.Errorf("onChunk callback is required")
	}
	buf := make([]byte, c.chunkSize)
	var offset int64
	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			chunkBytes := make([]byte, n)
			copy(chunkBytes, buf[:n])
			ch := Chunk{
				Hash:   ComputeHash(chunkBytes),
				Data:   chunkBytes,
				Offset: offset,
				Size:   int64(n),
			}
			if cbErr := onChunk(ch); cbErr != nil {
				return cbErr
			}
			offset += int64(n)
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("error reading chunk at offset %d: %w", offset, err)
		}
	}
}

// ChunkReader reads from an io.Reader and returns all chunks (including payloads).
func (c *Chunker) ChunkReader(r io.Reader) ([]Chunk, error) {
	var chunks []Chunk
	err := c.ChunkStream(r, func(ch Chunk) error {
		chunks = append(chunks, ch)
		return nil
	})
	return chunks, err
}

// ChunkFile reads a file from disk and returns its chunk references and chunk list.
func (c *Chunker) ChunkFile(filePath string) ([]Chunk, []v1.ChunkRef, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer f.Close()

	var chunks []Chunk
	var refs []v1.ChunkRef
	err = c.ChunkStream(f, func(ch Chunk) error {
		chunks = append(chunks, ch)
		refs = append(refs, v1.ChunkRef{Hash: ch.Hash, Offset: ch.Offset, Size: ch.Size})
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return chunks, refs, nil
}
