package engine

import (
	"bytes"
	"context"
	"fmt"
	"log"

	v1 "spider/api/v1"
	"spider/pkg/cache"
	"spider/pkg/chunk"
	"spider/pkg/source"
)

// Publisher handles turning source data trees into immutable manifests and cached chunks.
type Publisher struct {
	cache     *cache.Cache
	chunkSize int64
}

// NewPublisher creates a new artifact publisher.
func NewPublisher(c *cache.Cache, chunkSize int64) *Publisher {
	if chunkSize <= 0 {
		chunkSize = v1.DefaultChunkSize
	}
	return &Publisher{
		cache:     c,
		chunkSize: chunkSize,
	}
}

// Publish scans a storage source under prefix, chunks all files, stores chunks into local cache, and generates the canonical manifest.
func (p *Publisher) Publish(ctx context.Context, src source.Source, prefix string, name string, version string) (*v1.ArtifactManifest, error) {
	files, err := src.ListFiles(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to list files from source: %w", err)
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no files found under source prefix %q", prefix)
	}

	manifest := &v1.ArtifactManifest{
		SchemaVersion: v1.CurrentSchema,
		Name:          name,
		Version:       version,
		ChunkSize:     p.chunkSize,
	}

	chunker := chunk.NewChunker(p.chunkSize)

	for _, fileInfo := range files {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		log.Printf("[Publisher] Processing %s (%d bytes)...", fileInfo.Path, fileInfo.Size)

		r, err := src.Open(ctx, fileInfo.Path)
		if err != nil {
			return nil, fmt.Errorf("failed to open file %s: %w", fileInfo.Path, err)
		}

		chunks, err := chunker.ChunkReader(r)
		_ = r.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to chunk file %s: %w", fileInfo.Path, err)
		}

		var chunkRefs []v1.ChunkRef
		for _, ch := range chunks {
			// Persist chunk to cache
			if !p.cache.HasChunk(ch.Hash) {
				if err := p.cache.PutChunk(ch.Hash, ch.Data); err != nil {
					return nil, fmt.Errorf("failed to cache chunk %s: %w", ch.Hash, err)
				}
			}

			chunkRefs = append(chunkRefs, v1.ChunkRef{
				Hash:   ch.Hash,
				Offset: ch.Offset,
				Size:   ch.Size,
			})
		}

		// Handle empty file case
		if len(chunks) == 0 && fileInfo.Size == 0 {
			emptyHash := chunk.ComputeHash([]byte{})
			if !p.cache.HasChunk(emptyHash) {
				_ = p.cache.PutChunk(emptyHash, []byte{})
			}
			chunkRefs = append(chunkRefs, v1.ChunkRef{
				Hash:   emptyHash,
				Offset: 0,
				Size:   0,
			})
		}

		mode := fileInfo.Mode
		if mode == "" {
			mode = "0644"
		}

		manifest.Files = append(manifest.Files, v1.FileEntry{
			Path:   fileInfo.Path,
			Size:   fileInfo.Size,
			Mode:   mode,
			Chunks: chunkRefs,
		})
	}

	if err := manifest.Finalize(); err != nil {
		return nil, fmt.Errorf("failed to finalize manifest: %w", err)
	}

	if err := p.cache.SaveManifest(manifest); err != nil {
		return nil, fmt.Errorf("failed to save manifest to cache: %w", err)
	}

	log.Printf("[Publisher] Successfully published %s@%s (Artifact ID: %s, %d files, total %d bytes)",
		manifest.Name, manifest.Version, manifest.ArtifactID, len(manifest.Files), manifest.TotalSize)

	return manifest, nil
}

// PublishFromMemory creates an artifact manifest directly from in-memory byte maps (useful for unit tests and fixtures).
func (p *Publisher) PublishFromMemory(ctx context.Context, name, version string, files map[string][]byte) (*v1.ArtifactManifest, error) {
	manifest := &v1.ArtifactManifest{
		SchemaVersion: v1.CurrentSchema,
		Name:          name,
		Version:       version,
		ChunkSize:     p.chunkSize,
	}

	chunker := chunk.NewChunker(p.chunkSize)

	for path, data := range files {
		chunks, err := chunker.ChunkReader(bytes.NewReader(data))
		if err != nil {
			return nil, err
		}

		var chunkRefs []v1.ChunkRef
		for _, ch := range chunks {
			if !p.cache.HasChunk(ch.Hash) {
				if err := p.cache.PutChunk(ch.Hash, ch.Data); err != nil {
					return nil, err
				}
			}
			chunkRefs = append(chunkRefs, v1.ChunkRef{
				Hash:   ch.Hash,
				Offset: ch.Offset,
				Size:   ch.Size,
			})
		}

		manifest.Files = append(manifest.Files, v1.FileEntry{
			Path:   path,
			Size:   int64(len(data)),
			Mode:   "0644",
			Chunks: chunkRefs,
		})
	}

	if err := manifest.Finalize(); err != nil {
		return nil, err
	}
	if err := p.cache.SaveManifest(manifest); err != nil {
		return nil, err
	}
	return manifest, nil
}
