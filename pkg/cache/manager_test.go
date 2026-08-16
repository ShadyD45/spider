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
	t.Cleanup(func() { _ = mgr.Close() })
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

func TestMaybeEvictReclaimsUnpinnedAfterTwoSyncs(t *testing.T) {
	dir := t.TempDir()
	c, err := NewCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	makeChunk := func(b byte, n int) (string, []byte) {
		data := bytesRepeat(b, n)
		h := chunk.ComputeHash(data)
		if err := c.PutChunk(h, data); err != nil {
			t.Fatal(err)
		}
		return h, data
	}
	h1, d1 := makeChunk('1', 20)
	h2, d2 := makeChunk('2', 20)

	mgr, err := NewManager(c, 50, 0.4, 0.5)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mgr.Close() })
	c.SetOnCommit(mgr.Touch)
	mgr.Touch(h1, int64(len(d1)))
	mgr.Touch(h2, int64(len(d2)))

	if mgr.RefCount(h1) != 0 || mgr.RefCount(h2) != 0 {
		t.Fatal("leeched content must not be pinned")
	}
	n, err := mgr.MaybeEvict()
	if err != nil {
		t.Fatal(err)
	}
	if n == 0 {
		t.Fatalf("expected eviction of unpinned chunks, used=%d", mgr.UsedBytes())
	}
	if c.HasChunk(h1) && c.HasChunk(h2) {
		t.Fatal("expected at least one unpinned chunk evicted")
	}
}

func TestPinFillsZeroSizeAndIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	c, err := NewCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("pin-size-chunk!!")
	h := chunk.ComputeHash(data)
	if err := c.PutChunk(h, data); err != nil {
		t.Fatal(err)
	}
	mgr, err := NewManager(c, 1000, 0.5, 0.9)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mgr.Close() })

	mf := &v1.ArtifactManifest{
		ArtifactID:    "sha256:2222222222222222222222222222222222222222222222222222222222222222",
		SchemaVersion: 1,
		Name:          "p",
		Version:       "1",
		ChunkSize:     int64(len(data)),
		TotalSize:     int64(len(data)),
		Files: []v1.FileEntry{{
			Path: "a.bin", Size: int64(len(data)),
			Chunks: []v1.ChunkRef{{Hash: h, Size: int64(len(data))}},
		}},
	}
	if err := mgr.Pin(mf); err != nil {
		t.Fatal(err)
	}
	if mgr.UsedBytes() < int64(len(data)) {
		t.Fatalf("pin should record chunk size, used=%d", mgr.UsedBytes())
	}
	if err := mgr.Pin(mf); err != nil {
		t.Fatal(err)
	}
	if mgr.RefCount(h) != 1 {
		t.Fatalf("pin must be idempotent, ref=%d", mgr.RefCount(h))
	}
}

func TestTouchDebouncesIndexSave(t *testing.T) {
	dir := t.TempDir()
	c, err := NewCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	mgr, err := NewManager(c, 10000, 0.5, 0.9)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mgr.Close() })

	indexPath := filepath.Join(dir, "index.json")
	st1, err := os.Stat(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		mgr.Touch(fmtHash(i), 8)
	}
	st2, err := os.Stat(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	if st2.Size() != st1.Size() {
		t.Fatalf("Touch must debounce saves, index grew %d -> %d", st1.Size(), st2.Size())
	}
	if err := mgr.Flush(); err != nil {
		t.Fatal(err)
	}
	st3, err := os.Stat(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	if st3.Size() <= st1.Size() {
		t.Fatalf("flush should persist touches, before=%d after=%d", st1.Size(), st3.Size())
	}
}

func bytesRepeat(b byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}

func fmtHash(i int) string {
	return chunk.ComputeHash([]byte{byte(i), byte(i + 1), 7, 9})
}
