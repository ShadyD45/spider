package chunk

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestChunkerReader(t *testing.T) {
	chunkSize := int64(1024)
	chunker := NewChunker(chunkSize)

	// Create 2500 bytes of data (should produce 3 chunks: 1024, 1024, 452)
	data := bytes.Repeat([]byte("A"), 2500)
	chunks, err := chunker.ChunkReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("ChunkReader failed: %v", err)
	}

	if len(chunks) != 3 {
		t.Fatalf("Expected 3 chunks, got %d", len(chunks))
	}

	if chunks[0].Size != 1024 || chunks[1].Size != 1024 || chunks[2].Size != 452 {
		t.Fatalf("Unexpected chunk sizes: %d, %d, %d", chunks[0].Size, chunks[1].Size, chunks[2].Size)
	}

	if chunks[0].Offset != 0 || chunks[1].Offset != 1024 || chunks[2].Offset != 2048 {
		t.Fatalf("Unexpected chunk offsets: %d, %d, %d", chunks[0].Offset, chunks[1].Offset, chunks[2].Offset)
	}

	// Verify hashes match
	for _, c := range chunks {
		expectedHash := ComputeHash(c.Data)
		if c.Hash != expectedHash {
			t.Errorf("Hash mismatch: expected %s, got %s", expectedHash, c.Hash)
		}
		if err := VerifyHash(bytes.NewReader(c.Data), c.Hash); err != nil {
			t.Fatalf("VerifyHash rejected valid chunk: %v", err)
		}
	}
}

func TestChunkerFile(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "sample.bin")
	content := []byte("Hello World, Spider Artifact Mesh!")
	if err := os.WriteFile(filePath, content, 0644); err != nil {
		t.Fatal(err)
	}

	chunker := NewChunker(10)
	chunks, refs, err := chunker.ChunkFile(filePath)
	if err != nil {
		t.Fatalf("ChunkFile failed: %v", err)
	}

	if len(chunks) != 4 || len(refs) != 4 {
		t.Fatalf("Expected 4 chunks and 4 refs, got %d chunks, %d refs", len(chunks), len(refs))
	}
}

func TestChunkStreamDoesNotRetainPayloads(t *testing.T) {
	chunkSize := int64(64)
	chunker := NewChunker(chunkSize)
	data := bytes.Repeat([]byte("B"), 64*20)
	var refs int
	var maxLive int
	err := chunker.ChunkStream(bytes.NewReader(data), func(ch Chunk) error {
		if len(ch.Data) > maxLive {
			maxLive = len(ch.Data)
		}
		if ch.Size > chunkSize {
			t.Fatalf("chunk larger than chunkSize: %d", ch.Size)
		}
		refs++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if refs != 20 {
		t.Fatalf("expected 20 chunks, got %d", refs)
	}
	if maxLive > int(chunkSize) {
		t.Fatalf("live payload exceeded chunk size: %d", maxLive)
	}
}
