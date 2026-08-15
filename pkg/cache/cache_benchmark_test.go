package cache

import (
	"crypto/rand"
	"path/filepath"
	"testing"

	"spider/pkg/chunk"
)

func BenchmarkCacheAtomicPut4MiB(b *testing.B) {
	tempDir := b.TempDir()
	c, err := NewCache(filepath.Join(tempDir, "cache"))
	if err != nil {
		b.Fatal(err)
	}

	data := make([]byte, 4*1024*1024)
	_, _ = rand.Read(data)
	h := chunk.ComputeHash(data)

	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// PutChunk will atomically stage, verify, and commit
		if err := c.PutChunk(h, data); err != nil {
			b.Fatalf("PutChunk failed: %v", err)
		}
	}
}
