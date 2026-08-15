package v1

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func fakeHash(data string) string {
	h := sha256.Sum256([]byte(data))
	return "sha256:" + hex.EncodeToString(h[:])
}

func TestManifestFinalizeAndValidate(t *testing.T) {
	chunk1Hash := fakeHash("chunk-1")
	chunk2Hash := fakeHash("chunk-2")

	m := &ArtifactManifest{
		Name:      "test-model",
		Version:   "1.0.0",
		ChunkSize: DefaultChunkSize,
		Files: []FileEntry{
			{
				Path: "weights/model.bin",
				Size: 8192,
				Mode: "0644",
				Chunks: []ChunkRef{
					{Hash: chunk1Hash, Offset: 0, Size: 4096},
					{Hash: chunk2Hash, Offset: 4096, Size: 4096},
				},
			},
			{
				Path: "config.json",
				Size: 4096,
				Mode: "0644",
				Chunks: []ChunkRef{
					{Hash: chunk1Hash, Offset: 0, Size: 4096}, // Deduplicated chunk test
				},
			},
		},
	}

	if err := m.Finalize(); err != nil {
		t.Fatalf("Finalize failed: %v", err)
	}

	if err := m.Validate(); err != nil {
		t.Fatalf("Validate failed: %v", err)
	}

	// Verify deduplicated chunk list
	hashes := m.AllChunkHashes()
	if len(hashes) != 2 {
		t.Fatalf("Expected 2 deduplicated chunks, got %d", len(hashes))
	}

	// Verify sorting
	if m.Files[0].Path != "config.json" || m.Files[1].Path != "weights/model.bin" {
		t.Fatalf("Files not sorted canonically: %+v", m.Files)
	}

	m.ArtifactID = fakeHash("tampered-id")
	if err := m.Validate(); err == nil {
		t.Fatal("expected Validate to reject tampered artifactId")
	}
}

func TestManifestValidationFailures(t *testing.T) {
	m := &ArtifactManifest{
		SchemaVersion: 1,
		Name:          "",
		Version:       "1.0",
		Files:         []FileEntry{},
	}
	if err := m.Validate(); err == nil {
		t.Fatal("Expected error for empty name/files")
	}
}
