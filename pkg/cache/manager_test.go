package cache

import (
	"os"
	"path/filepath"
	"testing"

	v1 "spider/api/v1"
	"spider/pkg/chunk"
)

func TestManagerEvictsUnpinnedOnly(t *testing.T) {
	dir := t.TempDir()
	c, err := NewCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	a := []byte("aaaaaaaaaaaaaaaa")
	b := []byte("bbbbbbbbbbbbbbbb")
	ha := chunk.ComputeHash(a)
	hb := chunk.ComputeHash(b)
	if err := c.PutChunk(ha, a); err != nil {
		t.Fatal(err)
	}
	if err := c.PutChunk(hb, b); err != nil {
		t.Fatal(err)
	}

	mgr, err := NewManager(c, 40, 0.5, 0.6)
	if err != nil {
		t.Fatal(err)
	}
	mgr.Touch(ha, int64(len(a)))
	mgr.Touch(hb, int64(len(b)))

	mf := &v1.ArtifactManifest{
		ArtifactID:    "sha256:1111111111111111111111111111111111111111111111111111111111111111",
		SchemaVersion: 1,
		Name:          "pinme",
		Version:       "1",
		ChunkSize:     int64(len(a)),
		TotalSize:     int64(len(a)),
		Files: []v1.FileEntry{{
			Path: "a.bin", Size: int64(len(a)),
			Chunks: []v1.ChunkRef{{Hash: ha, Size: int64(len(a))}},
		}},
	}
	if err := mgr.Pin(mf); err != nil {
		t.Fatal(err)
	}
	n, err := mgr.MaybeEvict()
	if err != nil {
		t.Fatal(err)
	}
	if n == 0 {
		// maxBytes 40, two 16-byte chunks = 32, high watermark 0.6 * 40 = 24, so should evict
		t.Fatalf("expected eviction, used=%d", mgr.UsedBytes())
	}
	if !c.HasChunk(ha) {
		t.Fatal("pinned chunk evicted")
	}
	if c.HasChunk(hb) {
		t.Fatal("unpinned chunk should be evicted")
	}
	_ = os.Remove(filepath.Join(dir, "index.json"))
}
