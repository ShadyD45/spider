package materializer

import (
	"fmt"
	"path/filepath"
	"strings"
)

// SafeJoin resolves rel against base and rejects path escape.
func SafeJoin(baseDir, rel string) (string, error) {
	cleanRel := filepath.Clean(filepath.FromSlash(rel))
	if filepath.IsAbs(cleanRel) {
		return "", fmt.Errorf("security violation: absolute path rejected: %s", rel)
	}
	if cleanRel == ".." || strings.HasPrefix(cleanRel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("security violation: path escape detected: %s", rel)
	}
	base := filepath.Clean(baseDir)
	joined := filepath.Join(base, cleanRel)
	sep := string(filepath.Separator)
	if joined != base && !strings.HasPrefix(joined, base+sep) {
		return "", fmt.Errorf("security violation: path escape detected: %s", rel)
	}
	return joined, nil
}
