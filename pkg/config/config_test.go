package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadFileDefaultsAndOverride(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "spider.yaml")
	if err := os.WriteFile(p, []byte("store:\n  driver: memory\ncache:\n  driver: none\n  ttl: 5s\n"), 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Store.Driver != "memory" || cfg.Cache.Driver != "none" {
		t.Fatalf("%+v", cfg)
	}
	if cfg.Cache.TTL != 5*time.Second {
		t.Fatalf("ttl %v", cfg.Cache.TTL)
	}
}
