package cache

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"testing"

	v1 "spider/api/v1"
)

func hashOf(data []byte) string {
	h := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(h[:])
}

func TestCachePutAndGet(t *testing.T) {
	tempDir := t.TempDir()
	c, err := NewCache(tempDir)
	if err != nil {
		t.Fatal(err)
	}

	data := []byte("Testing atomic content-addressed cache put")
	h := hashOf(data)

	// Has before put
	if c.HasChunk(h) {
		t.Fatal("Expected chunk not to exist yet")
	}

	// Put valid chunk
	if err := c.PutChunk(h, data); err != nil {
		t.Fatalf("PutChunk failed: %v", err)
	}

	// Has after put
	if !c.HasChunk(h) {
		t.Fatal("Expected chunk to exist after put")
	}

	// Get reader
	r, size, err := c.GetChunkReader(h)
	if err != nil {
		t.Fatalf("GetChunkReader failed: %v", err)
	}
	defer r.Close()

	if size != int64(len(data)) {
		t.Fatalf("Expected size %d, got %d", len(data), size)
	}

	readBack, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readBack, data) {
		t.Fatal("Readback data mismatch")
	}

	// Put corrupted chunk (hash mismatch)
	corruptHash := hashOf([]byte("different payload"))
	err = c.PutChunk(corruptHash, data)
	if err == nil {
		t.Fatal("Expected error on hash mismatch, got nil")
	}
	if !errors.Is(err, ErrHashMismatch) {
		t.Fatalf("Expected ErrHashMismatch, got %v", err)
	}

	// PutChunkFromReader mismatch
	err = c.PutChunkFromReader(corruptHash, bytes.NewReader(data))
	if !errors.Is(err, ErrHashMismatch) {
		t.Fatalf("Expected ErrHashMismatch from PutChunkFromReader, got %v", err)
	}

	// Valid stream put
	streamData := []byte("streamed chunk payload for integrity test")
	streamHash := hashOf(streamData)
	if err := c.PutChunkFromReader(streamHash, bytes.NewReader(streamData)); err != nil {
		t.Fatalf("PutChunkFromReader failed: %v", err)
	}
	if !c.HasChunk(streamHash) {
		t.Fatal("Expected streamed chunk to exist after put")
	}

	// List chunks
	chunks, err := c.ListChunks()
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 2 {
		t.Fatalf("ListChunks mismatch: %+v", chunks)
	}
	seen := map[string]bool{h: false, streamHash: false}
	for _, ch := range chunks {
		if _, ok := seen[ch]; !ok {
			t.Fatalf("unexpected chunk in list: %s", ch)
		}
		seen[ch] = true
	}
}

func TestCacheManifestPersistence(t *testing.T) {
	tempDir := t.TempDir()
	c, err := NewCache(tempDir)
	if err != nil {
		t.Fatal(err)
	}

	chunk1Hash := hashOf([]byte("file1"))
	m := &v1.ArtifactManifest{
		Name:      "test-model",
		Version:   "1.0",
		ChunkSize: 1024,
		Files: []v1.FileEntry{
			{
				Path: "data.txt",
				Size: int64(len("file1")),
				Mode: "0644",
				Chunks: []v1.ChunkRef{
					{Hash: chunk1Hash, Offset: 0, Size: int64(len("file1"))},
				},
			},
		},
	}

	if err := m.Finalize(); err != nil {
		t.Fatal(err)
	}

	if err := c.SaveManifest(m); err != nil {
		t.Fatalf("SaveManifest failed: %v", err)
	}

	loaded, err := c.GetManifest(m.ArtifactID)
	if err != nil {
		t.Fatalf("GetManifest failed: %v", err)
	}

	if loaded.ArtifactID != m.ArtifactID || loaded.Name != m.Name {
		t.Fatalf("Manifest mismatch: %+v vs %+v", loaded, m)
	}
}
