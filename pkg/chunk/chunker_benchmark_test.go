package chunk

import (
	"bytes"
	"crypto/rand"
	"testing"
)

func BenchmarkChunker4MiB(b *testing.B) {
	data := make([]byte, 16*1024*1024) // 16 MiB
	_, _ = rand.Read(data)
	chunker := NewChunker(4 * 1024 * 1024)

	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r := bytes.NewReader(data)
		chunks, err := chunker.ChunkReader(r)
		if err != nil || len(chunks) != 4 {
			b.Fatalf("ChunkReader failed: %v", err)
		}
	}
}

func BenchmarkSHA256HashCalculation(b *testing.B) {
	data := make([]byte, 4*1024*1024) // 4 MiB chunk
	_, _ = rand.Read(data)

	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = ComputeHash(data)
	}
}
