package source

import (
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
}
