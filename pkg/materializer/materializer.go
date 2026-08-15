package materializer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"

	v1 "spider/api/v1"
	"spider/pkg/cache"
	"spider/pkg/chunk"
)

type Strategy string

const (
	StrategyCopy     Strategy = "copy"
	StrategyHardLink Strategy = "hardlink"
)

// Options configure the materializer.
type Options struct {
	Strategy      Strategy
	Overwrite     bool
	PreservePerms bool
}

// DefaultOptions returns standard safe defaults.
func DefaultOptions() Options {
	return Options{
		Strategy:      StrategyCopy,
		Overwrite:     true,
		PreservePerms: true,
	}
}

// Materializer assembles physical directories and files from cached chunks.
type Materializer struct {
	opts Options
}

// NewMaterializer creates a new materializer instance.
func NewMaterializer(opts Options) *Materializer {
	if opts.Strategy == "" {
		opts.Strategy = StrategyCopy
	}
	return &Materializer{opts: opts}
}

// Materialize constructs the full directory tree for an artifact manifest into destDir.
func (m *Materializer) Materialize(ctx context.Context, manifest *v1.ArtifactManifest, c *cache.Cache, destDir string) error {
	if err := manifest.Validate(); err != nil {
		return fmt.Errorf("invalid manifest: %w", err)
	}

	if err := os.MkdirAll(destDir, 0755); err != nil {
		return fmt.Errorf("failed to create destination directory %s: %w", destDir, err)
	}

	for _, fileEntry := range manifest.Files {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		targetFilePath, err := SafeJoin(destDir, fileEntry.Path)
		if err != nil {
			return err
		}

		// Ensure parent directory exists
		if err := os.MkdirAll(filepath.Dir(targetFilePath), 0755); err != nil {
			return fmt.Errorf("failed to create directory for %s: %w", fileEntry.Path, err)
		}

		// Handle single-chunk hardlink optimization if enabled and chunk matches entire file
		if m.opts.Strategy == StrategyHardLink && len(fileEntry.Chunks) == 1 && fileEntry.Chunks[0].Size == fileEntry.Size {
			chunkRef := fileEntry.Chunks[0]
			if err := verifyCachedChunk(c, chunkRef.Hash); err != nil {
				return fmt.Errorf("refusing to hardlink unverified chunk %s for %s: %w", chunkRef.Hash, fileEntry.Path, err)
			}
			chunkPath, err := c.GetChunkPath(chunkRef.Hash)
			if err == nil {
				if m.opts.Overwrite {
					_ = os.Remove(targetFilePath)
				}
				if err := os.Link(chunkPath, targetFilePath); err == nil {
					continue
				}
				// Fall back to copy if hardlink fails (e.g. cross-device link)
			}
		}

		if err := m.materializeFile(fileEntry, c, targetFilePath); err != nil {
			return fmt.Errorf("failed to materialize file %s: %w", fileEntry.Path, err)
		}

		// Set file permissions
		if m.opts.PreservePerms && fileEntry.Mode != "" {
			if perm, err := strconv.ParseUint(fileEntry.Mode, 8, 32); err == nil {
				_ = os.Chmod(targetFilePath, os.FileMode(perm))
			}
		}
	}

	return nil
}

func (m *Materializer) materializeFile(entry v1.FileEntry, c *cache.Cache, targetFilePath string) error {
	if m.opts.Overwrite {
		_ = os.Remove(targetFilePath)
	}

	outFile, err := os.OpenFile(targetFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to open target file %s: %w", targetFilePath, err)
	}
	defer outFile.Close()

	var bytesWritten int64
	for i, chunkRef := range entry.Chunks {
		r, _, err := c.GetChunkReader(chunkRef.Hash)
		if err != nil {
			return fmt.Errorf("missing chunk %s (file %s, index %d): %w", chunkRef.Hash, entry.Path, i, err)
		}

		hasher := sha256.New()
		src := io.TeeReader(io.LimitReader(r, chunkRef.Size), hasher)
		n, err := io.Copy(outFile, src)
		r.Close()
		if err != nil {
			return fmt.Errorf("failed writing chunk %s to %s: %w", chunkRef.Hash, targetFilePath, err)
		}
		if n != chunkRef.Size {
			return fmt.Errorf("short write for chunk %s: expected %d bytes, wrote %d", chunkRef.Hash, chunkRef.Size, n)
		}

		actualHash := "sha256:" + hex.EncodeToString(hasher.Sum(nil))
		if actualHash != chunkRef.Hash {
			_ = outFile.Close()
			_ = os.Remove(targetFilePath)
			return fmt.Errorf("integrity check failed for chunk %s in %s: expected %s, got %s", chunkRef.Hash, entry.Path, chunkRef.Hash, actualHash)
		}
		bytesWritten += n
	}

	if bytesWritten != entry.Size {
		return fmt.Errorf("total file size mismatch for %s: expected %d, materialized %d", entry.Path, entry.Size, bytesWritten)
	}

	return nil
}

func verifyCachedChunk(c *cache.Cache, chunkHash string) error {
	r, _, err := c.GetChunkReader(chunkHash)
	if err != nil {
		return err
	}
	defer r.Close()
	return chunk.VerifyHash(r, chunkHash)
}
