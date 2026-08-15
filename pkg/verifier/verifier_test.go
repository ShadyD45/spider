package verifier

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	v1 "spider/api/v1"
	"spider/pkg/cache"
	"spider/pkg/chunk"
	"spider/pkg/materializer"
)

func TestVerifyChunkAndCacheAudit(t *testing.T) {
	tempDir := t.TempDir()
	c, err := cache.NewCache(filepath.Join(tempDir, "cache"))
	if err != nil {
		t.Fatal(err)
	}

	validData := []byte("Valid 32-byte chunk data payload")
	validHash := chunk.ComputeHash(validData)

	if err := c.PutChunk(validHash, validData); err != nil {
		t.Fatal(err)
	}

	// 1. Verify valid chunk
	ok, err := VerifyChunk(c, validHash)
	if err != nil || !ok {
		t.Fatalf("Expected valid chunk, got ok=%v, err=%v", ok, err)
	}

	// 2. Full cache audit
	verified, corrupt, err := VerifyCache(c)
	if err != nil || verified != 1 || len(corrupt) != 0 {
		t.Fatalf("VerifyCache failed: verified=%d, corrupt=%v, err=%v", verified, corrupt, err)
	}

	// 3. Inject silent bit-rot / disk corruption into chunk file
	chunkPath, err := c.GetChunkPath(validHash)
	if err != nil {
		t.Fatal(err)
	}
	corruptedData := append([]byte("Corrupted "), validData[10:]...)
	if err := os.WriteFile(chunkPath, corruptedData, 0644); err != nil {
		t.Fatal(err)
	}

	// 4. Verify detects corruption
	ok, err = VerifyChunk(c, validHash)
	if ok || err == nil {
		t.Fatalf("Expected corruption error, got ok=%v, err=%v", ok, err)
	}

	verified, corrupt, err = VerifyCache(c)
	if len(corrupt) != 1 || corrupt[0] != validHash {
		t.Fatalf("Expected 1 corrupt chunk detected, got %v", corrupt)
	}
}

func TestVerifyMaterializedDirectory(t *testing.T) {
	tempDir := t.TempDir()
	cacheDir := filepath.Join(tempDir, "cache")
	matDir := filepath.Join(tempDir, "materialized")

	c, err := cache.NewCache(cacheDir)
	if err != nil {
		t.Fatal(err)
	}

	c1Data := []byte("First 16 bytes!!")
	c2Data := []byte("Second 16 bytes!")
	c1Hash := chunk.ComputeHash(c1Data)
	c2Hash := chunk.ComputeHash(c2Data)

	if err := c.PutChunk(c1Hash, c1Data); err != nil {
		t.Fatal(err)
	}
	if err := c.PutChunk(c2Hash, c2Data); err != nil {
		t.Fatal(err)
	}

	manifest := &v1.ArtifactManifest{
		Name:      "test-model",
		Version:   "1.0",
		ChunkSize: 16,
		Files: []v1.FileEntry{
			{
				Path: "weights.bin",
				Size: 32,
				Mode: "0644",
				Chunks: []v1.ChunkRef{
					{Hash: c1Hash, Offset: 0, Size: 16},
					{Hash: c2Hash, Offset: 16, Size: 16},
				},
			},
			{
				Path: "config.json",
				Size: 16,
				Mode: "0644",
				Chunks: []v1.ChunkRef{
					{Hash: c1Hash, Offset: 0, Size: 16},
				},
			},
		},
	}
	if err := manifest.Finalize(); err != nil {
		t.Fatal(err)
	}

	// Materialize directory
	mat := materializer.NewMaterializer(materializer.DefaultOptions())
	ctx := context.Background()
	if err := mat.Materialize(ctx, manifest, c, matDir); err != nil {
		t.Fatal(err)
	}

	// 1. Verify clean materialized directory
	report, err := VerifyMaterializedDirectory(ctx, manifest, matDir)
	if err != nil {
		t.Fatalf("VerifyMaterializedDirectory failed: %v", err)
	}
	if !report.AllValid || report.ValidFiles != 2 || report.CorruptFiles != 0 {
		t.Fatalf("Expected all files valid, got %+v", report)
	}

	// 2. Tamper with one byte in weights.bin
	weightsPath := filepath.Join(matDir, "weights.bin")
	weightsData, err := os.ReadFile(weightsPath)
	if err != nil {
		t.Fatal(err)
	}
	weightsData[20] = 'X' // tamper byte in second chunk
	if err := os.WriteFile(weightsPath, weightsData, 0644); err != nil {
		t.Fatal(err)
	}

	// 3. Verify tampered detection
	report, err = VerifyMaterializedDirectory(ctx, manifest, matDir)
	if err != nil {
		t.Fatal(err)
	}
	if report.AllValid {
		t.Fatal("Expected report.AllValid to be false after tampering")
	}
	if report.CorruptFiles != 1 || report.ValidFiles != 1 {
		t.Fatalf("Expected 1 corrupt file and 1 valid file, got %+v", report)
	}

	// 4. Test missing file
	_ = os.Remove(filepath.Join(matDir, "config.json"))
	report, err = VerifyMaterializedDirectory(ctx, manifest, matDir)
	if err != nil {
		t.Fatal(err)
	}
	if report.MissingFiles != 1 {
		t.Fatalf("Expected 1 missing file, got %d", report.MissingFiles)
	}
}

func TestVerifyMaterializedDirectorySizeMismatch(t *testing.T) {
	tempDir := t.TempDir()
	matDir := filepath.Join(tempDir, "materialized")
	if err := os.MkdirAll(matDir, 0755); err != nil {
		t.Fatal(err)
	}

	payload := []byte("twelve bytes")
	h := chunk.ComputeHash(payload)
	if err := os.WriteFile(filepath.Join(matDir, "truncated.bin"), payload[:8], 0644); err != nil {
		t.Fatal(err)
	}

	manifest := &v1.ArtifactManifest{
		Name:      "size-check",
		Version:   "1.0",
		ChunkSize: int64(len(payload)),
		Files: []v1.FileEntry{
			{
				Path: "truncated.bin",
				Size: int64(len(payload)),
				Mode: "0644",
				Chunks: []v1.ChunkRef{
					{Hash: h, Offset: 0, Size: int64(len(payload))},
				},
			},
		},
	}
	if err := manifest.Finalize(); err != nil {
		t.Fatal(err)
	}

	report, err := VerifyMaterializedDirectory(context.Background(), manifest, matDir)
	if err != nil {
		t.Fatal(err)
	}
	if report.AllValid || report.CorruptFiles != 1 {
		t.Fatalf("expected size mismatch to mark file corrupt, got %+v", report)
	}
}

func TestVerifyHashHelper(t *testing.T) {
	data := []byte("hash helper payload")
	h := chunk.ComputeHash(data)
	if err := chunk.VerifyHash(bytes.NewReader(data), h); err != nil {
		t.Fatalf("expected matching hash: %v", err)
	}
	if err := chunk.VerifyHash(bytes.NewReader(data), chunk.ComputeHash([]byte("other"))); err == nil {
		t.Fatal("expected mismatch error")
	}
}
