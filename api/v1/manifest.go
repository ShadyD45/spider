package v1

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
)

const (
	DefaultChunkSize = 4 * 1024 * 1024 // 4 MiB
	CurrentSchema    = 1
)

// ChunkRef represents a slice of a file mapped to a content-addressed chunk hash.
type ChunkRef struct {
	Hash   string `json:"hash"`   // sha256:<hex>
	Offset int64  `json:"offset"` // byte offset in file
	Size   int64  `json:"size"`   // byte size of this chunk slice
}

// FileEntry represents a file in an artifact.
type FileEntry struct {
	Path   string     `json:"path"`   // normalized relative path with /
	Size   int64      `json:"size"`   // file size in bytes
	Mode   string     `json:"mode"`   // file permission mode string e.g. 0644
	Chunks []ChunkRef `json:"chunks"` // ordered chunks composing this file
}

// ArtifactManifest describes an immutable directory tree or file collection.
type ArtifactManifest struct {
	SchemaVersion int         `json:"schemaVersion"`
	ArtifactID    string      `json:"artifactId"`
	Name          string      `json:"name"`
	Version       string      `json:"version"`
	ChunkSize     int64       `json:"chunkSize"`
	TotalSize     int64       `json:"totalSize"`
	Files         []FileEntry `json:"files"`
}

// NormalizePath normalizes a file path to use forward slashes and removes leading/trailing slashes.
func NormalizePath(p string) string {
	clean := filepath.ToSlash(filepath.Clean(p))
	clean = strings.TrimPrefix(clean, "/")
	clean = strings.TrimPrefix(clean, "./")
	return clean
}

// ComputeID calculates the canonical SHA-256 artifact ID based on the manifest content.
func (m *ArtifactManifest) ComputeID() (string, error) {
	clone := *m
	clone.ArtifactID = "" // exclude artifactId when hashing

	// Sort files by path for deterministic canonical output
	sort.Slice(clone.Files, func(i, j int) bool {
		return clone.Files[i].Path < clone.Files[j].Path
	})

	canonicalBytes, err := json.Marshal(clone)
	if err != nil {
		return "", fmt.Errorf("failed to marshal canonical manifest: %w", err)
	}

	h := sha256.Sum256(canonicalBytes)
	return fmt.Sprintf("sha256:%s", hex.EncodeToString(h[:])), nil
}

// Finalize sorts files, calculates total size and sets the canonical ArtifactID.
func (m *ArtifactManifest) Finalize() error {
	if m.SchemaVersion == 0 {
		m.SchemaVersion = CurrentSchema
	}
	if m.ChunkSize == 0 {
		m.ChunkSize = DefaultChunkSize
	}

	var totalSize int64
	for i := range m.Files {
		m.Files[i].Path = NormalizePath(m.Files[i].Path)
		totalSize += m.Files[i].Size
	}
	m.TotalSize = totalSize

	sort.Slice(m.Files, func(i, j int) bool {
		return m.Files[i].Path < m.Files[j].Path
	})

	id, err := m.ComputeID()
	if err != nil {
		return err
	}
	m.ArtifactID = id
	return nil
}

// Validate checks manifest integrity and constraints.
func (m *ArtifactManifest) Validate() error {
	if m.SchemaVersion != CurrentSchema {
		return fmt.Errorf("unsupported schema version: %d", m.SchemaVersion)
	}
	if strings.TrimSpace(m.Name) == "" {
		return errors.New("artifact name is required")
	}
	if strings.TrimSpace(m.Version) == "" {
		return errors.New("artifact version is required")
	}
	if m.ChunkSize <= 0 {
		return errors.New("chunk size must be greater than zero")
	}
	if len(m.Files) == 0 {
		return errors.New("artifact must contain at least one file")
	}

	var calculatedTotal int64
	seenPaths := make(map[string]struct{})

	for i, f := range m.Files {
		if f.Path == "" || strings.HasPrefix(f.Path, "..") || strings.HasPrefix(f.Path, "/") {
			return fmt.Errorf("invalid file path at index %d: %q", i, f.Path)
		}
		if _, exists := seenPaths[f.Path]; exists {
			return fmt.Errorf("duplicate file path: %q", f.Path)
		}
		seenPaths[f.Path] = struct{}{}
		calculatedTotal += f.Size

		var fileChunkSize int64
		for ci, c := range f.Chunks {
			if !strings.HasPrefix(c.Hash, "sha256:") || len(c.Hash) != 71 {
				return fmt.Errorf("invalid chunk hash in file %q chunk %d: %q", f.Path, ci, c.Hash)
			}
			if c.Size < 0 {
				return fmt.Errorf("invalid chunk size in file %q chunk %d: %d", f.Path, ci, c.Size)
			}
			if c.Offset != fileChunkSize {
				return fmt.Errorf("chunk offset mismatch in file %q chunk %d: expected %d, got %d", f.Path, ci, fileChunkSize, c.Offset)
			}
			fileChunkSize += c.Size
		}

		if fileChunkSize != f.Size {
			return fmt.Errorf("file size mismatch for %q: declared %d, chunks total %d", f.Path, f.Size, fileChunkSize)
		}
	}

	if calculatedTotal != m.TotalSize {
		return fmt.Errorf("manifest totalSize mismatch: declared %d, calculated %d", m.TotalSize, calculatedTotal)
	}

	expectedID, err := m.ComputeID()
	if err != nil {
		return err
	}
	if m.ArtifactID != expectedID {
		return fmt.Errorf("artifactId mismatch: expected %q, got %q", expectedID, m.ArtifactID)
	}

	return nil
}

// AllChunkHashes returns a deduplicated list of all chunk hashes in the manifest.
func (m *ArtifactManifest) AllChunkHashes() []string {
	seen := make(map[string]struct{})
	var hashes []string
	for _, f := range m.Files {
		for _, c := range f.Chunks {
			if _, exists := seen[c.Hash]; !exists {
				seen[c.Hash] = struct{}{}
				hashes = append(hashes, c.Hash)
			}
		}
	}
	return hashes
}

// ParseManifest parses and validates a JSON manifest.
func ParseManifest(data []byte) (*ArtifactManifest, error) {
	var m ArtifactManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("failed to parse manifest JSON: %w", err)
	}
	if err := m.Validate(); err != nil {
		return nil, fmt.Errorf("manifest validation failed: %w", err)
	}
	return &m, nil
}
