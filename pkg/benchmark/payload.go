package benchmark

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// DefaultPayloadRel is the in-repo transfer payload (gitignored).
const DefaultPayloadRel = "tmp/origin/payload.bin"

// DefaultWorkRel is scratch space for seeder/worker caches during a run.
const DefaultWorkRel = "tmp/work"

// DefaultPayloadPath returns DefaultPayloadRel resolved against the working directory.
func DefaultPayloadPath() string {
	return filepath.Clean(DefaultPayloadRel)
}

// IsDefaultPayload reports whether path is the in-repo payload (relative or absolute).
func IsDefaultPayload(path string) bool {
	if path == "" {
		return true
	}
	got, err1 := filepath.Abs(path)
	want, err2 := filepath.Abs(DefaultPayloadPath())
	if err1 != nil || err2 != nil {
		return filepath.Clean(path) == DefaultPayloadPath()
	}
	if runtime.GOOS == "windows" {
		return strings.EqualFold(got, want)
	}
	return got == want
}

// EnsurePayloadFile creates or resizes path to exactly sizeBytes of random data.
// Existing files of the right size are left unchanged.
func EnsurePayloadFile(path string, sizeBytes int64) error {
	if sizeBytes <= 0 {
		return fmt.Errorf("payload size must be positive")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	if st, err := os.Stat(path); err == nil && st.Mode().IsRegular() && st.Size() == sizeBytes {
		return nil
	}
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create payload %s: %w", path, err)
	}
	defer f.Close()
	buf := make([]byte, 1024*1024)
	var written int64
	for written < sizeBytes {
		n := int64(len(buf))
		if sizeBytes-written < n {
			n = sizeBytes - written
		}
		if _, err := rand.Read(buf[:n]); err != nil {
			return fmt.Errorf("fill payload: %w", err)
		}
		if _, err := f.Write(buf[:n]); err != nil {
			return fmt.Errorf("write payload: %w", err)
		}
		written += n
	}
	return f.Truncate(sizeBytes)
}

// PrepareOrigin returns the origin file or directory to publish.
// When managed is true (default in-repo payload), a missing or wrong-sized file is generated.
// When managed is false, path must already exist and is never overwritten.
func PrepareOrigin(path string, sizeBytes int64, managed bool) (string, error) {
	if path == "" {
		path = DefaultPayloadPath()
		managed = true
	}
	st, err := os.Stat(path)
	if err == nil {
		if st.IsDir() {
			return path, nil
		}
		if managed && st.Size() != sizeBytes {
			if err := EnsurePayloadFile(path, sizeBytes); err != nil {
				return "", err
			}
		}
		return path, nil
	}
	if !os.IsNotExist(err) {
		return "", err
	}
	if !managed {
		return "", fmt.Errorf("benchmark file %s not found", path)
	}
	if err := EnsurePayloadFile(path, sizeBytes); err != nil {
		return "", err
	}
	return path, nil
}
