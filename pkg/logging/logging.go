package logging

import (
	"io"
	"log/slog"
	"os"
	"strings"
)

// New returns a slog logger. format is "json" or "text".
func New(format string, w io.Writer) *slog.Logger {
	if w == nil {
		w = os.Stderr
	}
	opts := &slog.HandlerOptions{Level: slog.LevelInfo}
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "json":
		return slog.New(slog.NewJSONHandler(w, opts))
	default:
		return slog.New(slog.NewTextHandler(w, opts))
	}
}

// SetDefault installs the process default slog logger.
func SetDefault(format string) *slog.Logger {
	l := New(format, os.Stderr)
	slog.SetDefault(l)
	return l
}
