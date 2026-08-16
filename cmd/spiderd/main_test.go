package main

import (
	"testing"

	v1 "spider/api/v1"
	"spider/pkg/cache"
	"spider/pkg/chunk"
)

func TestAfterSuccessfulSyncPinsOnlyConfiguredArtifacts(t *testing.T) {
	dir := t.TempDir()
	c, err := cache.NewChunkStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("sync-pin-test-data")
	h := chunk.ComputeHash(data)
	if err := c.PutChunk(h, data); err != nil {
		t.Fatal(err)
	}
	mgr, err := cache.NewQuotaManager(c, 10_000, 0.5, 0.9)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mgr.Close() })

	mf := &v1.ArtifactManifest{
		ArtifactID:    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		SchemaVersion: 1,
		Name:          "art",
		Version:       "1",
		ChunkSize:     int64(len(data)),
		TotalSize:     int64(len(data)),
		Files: []v1.FileEntry{{
			Path: "f.bin", Size: int64(len(data)),
			Chunks: []v1.ChunkRef{{Hash: h, Size: int64(len(data))}},
		}},
	}

	hnd := &daemonSyncHandler{cache: c, mgr: mgr, pinned: map[string]struct{}{}}
	hnd.afterSuccessfulSync(mf)
	if mgr.RefCount(h) != 0 {
		t.Fatalf("regular sync must not pin, ref=%d", mgr.RefCount(h))
	}

	hnd.pinned[mf.ArtifactID] = struct{}{}
	hnd.afterSuccessfulSync(mf)
	if mgr.RefCount(h) != 1 {
		t.Fatalf("configured pin list should pin once, ref=%d", mgr.RefCount(h))
	}
}
