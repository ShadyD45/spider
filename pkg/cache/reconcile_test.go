package cache

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	v1 "spider/api/v1"
)

func TestManifestCachedHashesValidOnly(t *testing.T) {
	c, err := NewCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("12345678")
	hash := hashOf(data)

	manifest := &v1.ArtifactManifest{
		Name:    "m",
		Version: "1",
		Files: []v1.FileEntry{{
			Path: "f.bin",
			Chunks: []v1.ChunkRef{{
				Hash: hash,
				Size: int64(len(data)),
			}},
		}},
	}
	if err := manifest.Finalize(); err != nil {
		t.Fatal(err)
	}

	if err := c.AppendPartial(hash, bytes.NewReader([]byte("partial"))); err != nil {
		t.Fatal(err)
	}
	if got := ManifestCachedHashes(c, manifest); len(got) != 0 {
		t.Fatalf("partial should not be advertised, got %v", got)
	}

	hexName := hash[len("sha256:"):]
	committed := filepath.Join(c.RootDir(), "chunks", hexName)
	if err := os.MkdirAll(filepath.Dir(committed), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(committed, []byte("123456789"), 0644); err != nil {
		t.Fatal(err)
	}
	if got := ManifestCachedHashes(c, manifest); len(got) != 0 {
		t.Fatalf("wrong-size chunk should not be advertised, got %v", got)
	}
	_ = os.Remove(committed)

	if err := c.PutChunk(hash, data); err != nil {
		t.Fatal(err)
	}
	got := ManifestCachedHashes(c, manifest)
	if len(got) != 1 || got[0] != hash {
		t.Fatalf("expected valid cached hash, got %v", got)
	}

	otherData := []byte("other123")
	other := hashOf(otherData)
	if err := c.PutChunk(other, otherData); err != nil {
		t.Fatal(err)
	}
	got = ManifestCachedHashes(c, manifest)
	if len(got) != 1 {
		t.Fatalf("unrelated cache entry must not be included, got %v", got)
	}
}
