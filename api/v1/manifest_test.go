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

func TestValidateRejectsPathTraversal(t *testing.T) {
	h := fakeHash("x")
	m := &ArtifactManifest{
		SchemaVersion: 1,
		Name:          "bad",
		Version:       "1",
		ChunkSize:     1,
		TotalSize:     1,
		Files: []FileEntry{{
			Path:   "a/../../outside.txt",
			Size:   1,
			Chunks: []ChunkRef{{Hash: h, Offset: 0, Size: 1}},
		}},
	}
	id, err := m.ComputeID()
	if err != nil {
		t.Fatal(err)
	}
	m.ArtifactID = id
	if err := m.Validate(); err == nil {
		t.Fatal("expected Validate to reject traversal path")
	}
}

func TestValidateDoesNotReorderFiles(t *testing.T) {
	h1 := fakeHash("1")
	h2 := fakeHash("2")
	m := &ArtifactManifest{
		SchemaVersion: 1,
		Name:          "order",
		Version:       "1",
		ChunkSize:     1,
		TotalSize:     2,
		Files: []FileEntry{
			{Path: "z.bin", Size: 1, Chunks: []ChunkRef{{Hash: h1, Offset: 0, Size: 1}}},
			{Path: "a.bin", Size: 1, Chunks: []ChunkRef{{Hash: h2, Offset: 0, Size: 1}}},
		},
	}
	id, err := m.ComputeID()
	if err != nil {
		t.Fatal(err)
	}
	m.ArtifactID = id
	before := m.Files[0].Path
	if err := m.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := m.Validate(); err != nil {
		t.Fatal(err)
	}
	if m.Files[0].Path != before || m.Files[0].Path != "z.bin" {
		t.Fatalf("Validate must not reorder Files, got %q", m.Files[0].Path)
	}
	if err := m.Finalize(); err != nil {
		t.Fatal(err)
	}
	if m.Files[0].Path != "a.bin" {
		t.Fatalf("Finalize should sort files, got %q", m.Files[0].Path)
	}
}
