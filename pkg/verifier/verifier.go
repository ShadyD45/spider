package verifier

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"

	v1 "spider/api/v1"
	"spider/pkg/cache"
	"spider/pkg/chunk"
	"spider/pkg/materializer"
)

// FileVerificationResult details verification for an individual file.
type FileVerificationResult struct {
	Path           string   `json:"path"`
	ExpectedSize   int64    `json:"expectedSize"`
	ActualSize     int64    `json:"actualSize"`
	ExpectedMode   string   `json:"expectedMode"`
	ActualMode     string   `json:"actualMode"`
	TotalChunks    int      `json:"totalChunks"`
	VerifiedChunks int      `json:"verifiedChunks"`
	CorruptChunks  []string `json:"corruptChunks,omitempty"`
	Valid          bool     `json:"valid"`
	Error          string   `json:"error,omitempty"`
}

// VerificationReport contains aggregated integrity status.
type VerificationReport struct {
	ArtifactID      string                   `json:"artifactId"`
	ArtifactName    string                   `json:"artifactName"`
	ArtifactVersion string                   `json:"artifactVersion"`
	TotalFiles      int                      `json:"totalFiles"`
	ValidFiles      int                      `json:"validFiles"`
	CorruptFiles    int                      `json:"corruptFiles"`
	MissingFiles    int                      `json:"missingFiles"`
	TotalChunks     int                      `json:"totalChunks"`
	VerifiedChunks  int                      `json:"verifiedChunks"`
	CorruptChunks   int                      `json:"corruptChunks"`
	FileResults     []FileVerificationResult `json:"fileResults"`
	AllValid        bool                     `json:"allValid"`
}

// VerifyChunk reads a chunk from cache and recomputes its cryptographic SHA-256 hash.
func VerifyChunk(c *cache.Cache, chunkHash string) (bool, error) {
	r, _, err := c.GetChunkReader(chunkHash)
	if err != nil {
		return false, fmt.Errorf("failed to open chunk %s: %w", chunkHash, err)
	}
	defer r.Close()
	if err := chunk.VerifyHash(r, chunkHash); err != nil {
		return false, fmt.Errorf("chunk corruption: %w", err)
	}
	return true, nil
}

// VerifyCache scans all chunks in cache and verifies their cryptographic integrity against bit-rot.
func VerifyCache(c *cache.Cache) (int, []string, error) {
	chunks, err := c.ListChunks()
	if err != nil {
		return 0, nil, err
	}

	var corruptHashes []string
	verifiedCount := 0

	for _, h := range chunks {
		valid, err := VerifyChunk(c, h)
		if !valid || err != nil {
			corruptHashes = append(corruptHashes, h)
		} else {
			verifiedCount++
		}
	}

	return verifiedCount, corruptHashes, nil
}

// VerifyMaterializedDirectory verifies that files in targetDir match the manifest in size, permissions, and chunk hashes.
func VerifyMaterializedDirectory(ctx context.Context, manifest *v1.ArtifactManifest, targetDir string) (*VerificationReport, error) {
	if err := manifest.Validate(); err != nil {
		return nil, fmt.Errorf("invalid manifest: %w", err)
	}

	report := &VerificationReport{
		ArtifactID:      manifest.ArtifactID,
		ArtifactName:    manifest.Name,
		ArtifactVersion: manifest.Version,
		TotalFiles:      len(manifest.Files),
		AllValid:        true,
	}

	for _, fileEntry := range manifest.Files {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		res := FileVerificationResult{
			Path:         fileEntry.Path,
			ExpectedSize: fileEntry.Size,
			ExpectedMode: fileEntry.Mode,
			TotalChunks:  len(fileEntry.Chunks),
		}

		filePath, joinErr := materializer.SafeJoin(targetDir, fileEntry.Path)
		if joinErr != nil {
			res.Error = joinErr.Error()
			report.CorruptFiles++
			report.AllValid = false
			report.FileResults = append(report.FileResults, res)
			continue
		}
		info, err := os.Stat(filePath)
		if err != nil {
			if os.IsNotExist(err) {
				res.Error = "file missing"
				report.MissingFiles++
			} else {
				res.Error = err.Error()
				report.CorruptFiles++
			}
			report.AllValid = false
			report.FileResults = append(report.FileResults, res)
			continue
		}

		res.ActualSize = info.Size()
		res.ActualMode = fmt.Sprintf("%04o", info.Mode().Perm())

		if res.ActualSize != res.ExpectedSize {
			res.Error = fmt.Sprintf("size mismatch: expected %d bytes, got %d", res.ExpectedSize, res.ActualSize)
			report.CorruptFiles++
			report.AllValid = false
			report.FileResults = append(report.FileResults, res)
			continue
		}

		// Verify chunk-by-chunk hashes inside the materialized file
		f, err := os.Open(filePath)
		if err != nil {
			res.Error = fmt.Sprintf("cannot open file: %v", err)
			report.CorruptFiles++
			report.AllValid = false
			report.FileResults = append(report.FileResults, res)
			continue
		}

		var fileValid = true
		for ci, chunkRef := range fileEntry.Chunks {
			report.TotalChunks++
			if _, err := f.Seek(chunkRef.Offset, io.SeekStart); err != nil {
				res.Error = fmt.Sprintf("failed to seek chunk %d (offset %d): %v", ci, chunkRef.Offset, err)
				res.CorruptChunks = append(res.CorruptChunks, chunkRef.Hash)
				fileValid = false
				break
			}

			chunkBuf := make([]byte, chunkRef.Size)
			n, err := io.ReadFull(f, chunkBuf)
			if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
				res.Error = fmt.Sprintf("failed to read chunk %d: %v", ci, err)
				res.CorruptChunks = append(res.CorruptChunks, chunkRef.Hash)
				fileValid = false
				break
			}

			actualHash := chunk.ComputeHash(chunkBuf[:n])
			if actualHash != chunkRef.Hash {
				res.CorruptChunks = append(res.CorruptChunks, chunkRef.Hash)
				res.Error = fmt.Sprintf("chunk %d hash mismatch: expected %s, got %s", ci, chunkRef.Hash, actualHash)
				fileValid = false
				report.CorruptChunks++
			} else {
				res.VerifiedChunks++
				report.VerifiedChunks++
			}
		}
		_ = f.Close()

		if fileValid {
			res.Valid = true
			report.ValidFiles++
		} else {
			report.CorruptFiles++
			report.AllValid = false
		}

		// Verify mode warning if specified
		if fileEntry.Mode != "" {
			if perm, err := strconv.ParseUint(fileEntry.Mode, 8, 32); err == nil {
				expectedPerm := os.FileMode(perm)
				if info.Mode().Perm() != expectedPerm {
					// Mode difference does not fail byte integrity, but recorded in result
					if res.Error == "" {
						res.Error = fmt.Sprintf("note: file perm is %04o (manifest specifies %04o)", info.Mode().Perm(), expectedPerm)
					}
				}
			}
		}

		report.FileResults = append(report.FileResults, res)
	}

	return report, nil
}
