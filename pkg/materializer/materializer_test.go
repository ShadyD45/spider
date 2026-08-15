package materializer

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	v1 "spider/api/v1"
	"spider/pkg/cache"
	"spider/pkg/chunk"
)

func TestMaterializer(t *testing.T) {
	tempCacheDir := t.TempDir()
	c, err := cache.NewCache(tempCacheDir)
	if err != nil {
		t.Fatal(err)
	}

	chunk1Data := []byte("First 16 bytes!!")
	chunk2Data := []byte("Second 16 bytes!")
	chunk1Hash := chunk.ComputeHash(chunk1Data)
	chunk2Hash := chunk.ComputeHash(chunk2Data)

	if err := c.PutChunk(chunk1Hash, chunk1Data); err != nil {
		t.Fatal(err)
	}
	if err := c.PutChunk(chunk2Hash, chunk2Data); err != nil {
		t.Fatal(err)
	}

	manifest := &v1.ArtifactManifest{
		Name:      "test-materialize",
		Version:   "1.0.0",
		ChunkSize: 16,
		Files: []v1.FileEntry{
			{
				Path: "subdir/nested/combined.txt",
				Size: 32,
				Mode: "0644",
				Chunks: []v1.ChunkRef{
					{Hash: chunk1Hash, Offset: 0, Size: 16},
					{Hash: chunk2Hash, Offset: 16, Size: 16},
				},
			},
			{
				Path: "single.txt",
				Size: 16,
				Mode: "0644",
				Chunks: []v1.ChunkRef{
					{Hash: chunk1Hash, Offset: 0, Size: 16},
				},
			},
		},
	}

	if err := manifest.Finalize(); err != nil {
		t.Fatal(err)
	}

	outDir := t.TempDir()
	m := NewMaterializer(DefaultOptions())

	if err := m.Materialize(context.Background(), manifest, c, outDir); err != nil {
		t.Fatalf("Materialize failed: %v", err)
	}

	// Verify combined file
	combinedPath := filepath.Join(outDir, "subdir", "nested", "combined.txt")
	combinedContent, err := os.ReadFile(combinedPath)
	if err != nil {
		t.Fatalf("Failed to read materialized combined file: %v", err)
	}
	expectedCombined := append(chunk1Data, chunk2Data...)
	if string(combinedContent) != string(expectedCombined) {
		t.Fatalf("Combined content mismatch: got %q, expected %q", string(combinedContent), string(expectedCombined))
	}

	// Verify single file
	singlePath := filepath.Join(outDir, "single.txt")
	singleContent, err := os.ReadFile(singlePath)
	if err != nil {
		t.Fatalf("Failed to read single file: %v", err)
	}
	if string(singleContent) != string(chunk1Data) {
		t.Fatalf("Single content mismatch: got %q, expected %q", string(singleContent), string(chunk1Data))
	}
}

func TestMaterializeRejectsCorruptCache(t *testing.T) {
	tempCacheDir := t.TempDir()
	c, err := cache.NewCache(tempCacheDir)
	if err != nil {
		t.Fatal(err)
	}

	data := []byte("Honest chunk bytes")
	h := chunk.ComputeHash(data)
	if err := c.PutChunk(h, data); err != nil {
		t.Fatal(err)
	}

	path, err := c.GetChunkPath(h)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("tampered cache payload"), 0644); err != nil {
		t.Fatal(err)
	}

	manifest := &v1.ArtifactManifest{
		Name:      "corrupt-cache",
		Version:   "1.0.0",
		ChunkSize: int64(len(data)),
		Files: []v1.FileEntry{
			{
				Path: "weights.bin",
				Size: int64(len(data)),
				Mode: "0644",
				Chunks: []v1.ChunkRef{
					{Hash: h, Offset: 0, Size: int64(len(data))},
				},
			},
		},
	}
	if err := manifest.Finalize(); err != nil {
		t.Fatal(err)
	}

	outDir := t.TempDir()
	m := NewMaterializer(DefaultOptions())
	err = m.Materialize(context.Background(), manifest, c, outDir)
	if err == nil {
		t.Fatal("expected materialize to fail on corrupt cached chunk")
	}
	if _, statErr := os.Stat(filepath.Join(outDir, "weights.bin")); !os.IsNotExist(statErr) {
		t.Fatal("partial corrupt file should be removed after integrity failure")
	}
}

func TestSafeJoinRejectsEscape(t *testing.T) {
	base := t.TempDir()
	if _, err := SafeJoin(base, "../etc/passwd"); err == nil {
		t.Fatal("expected escape rejection")
	}
	got, err := SafeJoin(base, "ok/file.bin")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(filepath.ToSlash(got), "ok/file.bin") && !strings.Contains(got, "ok") {
		t.Fatalf("unexpected join %s", got)
	}
}
