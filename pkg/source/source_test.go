package source

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestFilesystemSource(t *testing.T) {
	tempDir := t.TempDir()
	subDir := filepath.Join(tempDir, "models", "v1")
	if err := os.MkdirAll(subDir, 0755); err != nil {
		t.Fatal(err)
	}

	f1Path := filepath.Join(subDir, "weights.bin")
	f1Data := []byte("0123456789ABCDEF0123456789ABCDEF")
	if err := os.WriteFile(f1Path, f1Data, 0644); err != nil {
		t.Fatal(err)
	}

	src, err := NewFilesystemSource(tempDir)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	files, err := src.ListFiles(ctx, "")
	if err != nil {
		t.Fatalf("ListFiles failed: %v", err)
	}

	if len(files) != 1 || files[0].Path != "models/v1/weights.bin" {
		t.Fatalf("Unexpected files list: %+v", files)
	}

	// Test byte-range read chunk
	chunkBytes, err := src.ReadChunk(ctx, "models/v1/weights.bin", 10, 6)
	if err != nil {
		t.Fatalf("ReadChunk failed: %v", err)
	}
	if string(chunkBytes) != "ABCDEF" {
		t.Fatalf("Expected 'ABCDEF', got %q", string(chunkBytes))
	}

	var streamed bytes.Buffer
	n, err := src.ReadChunkTo(ctx, "models/v1/weights.bin", 10, 6, &streamed)
	if err != nil {
		t.Fatalf("ReadChunkTo failed: %v", err)
	}
	if n != 6 || streamed.String() != "ABCDEF" {
		t.Fatalf("ReadChunkTo: n=%d data=%q", n, streamed.String())
	}

	srcFile, err := NewPathSource(f1Path)
	if err != nil {
		t.Fatal(err)
	}
	only, err := srcFile.ListFiles(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(only) != 1 || only[0].Path != "weights.bin" || only[0].Size != int64(len(f1Data)) {
		t.Fatalf("single file origin: %+v", only)
	}
}
