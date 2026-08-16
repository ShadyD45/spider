package engine

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"spider/pkg/cache"
	"spider/pkg/source"
)

func TestPublishStreamsChunksToCache(t *testing.T) {
	dir := t.TempDir()
	srcDir := filepath.Join(dir, "src")
	if err := os.MkdirAll(srcDir, 0755); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("P"), 250)
	if err := os.WriteFile(filepath.Join(srcDir, "blob.bin"), payload, 0644); err != nil {
		t.Fatal(err)
	}
	src, err := source.NewFilesystemSource(srcDir)
	if err != nil {
		t.Fatal(err)
	}
	c, err := cache.NewChunkStore(filepath.Join(dir, "cache"))
	if err != nil {
		t.Fatal(err)
	}
	pub := NewPublisher(c, 64)
	mf, err := pub.Publish(context.Background(), src, "", "stream-art", "1")
	if err != nil {
		t.Fatal(err)
	}
	if len(mf.Files) != 1 || len(mf.Files[0].Chunks) < 3 {
		t.Fatalf("expected streamed chunk refs, got %+v", mf.Files)
	}
	for _, ref := range mf.Files[0].Chunks {
		if !c.HasChunk(ref.Hash) {
			t.Fatalf("missing committed chunk %s", ref.Hash)
		}
	}
}
