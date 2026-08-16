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

func TestLoadFilePreferredKeysAndEnv(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "spider.yaml")
	body := []byte("metaCache:\n  driver: none\nchunkCache:\n  dir: /tmp/chunks\nadvertisement:\n  batchSize: 4\ndownload:\n  maxConcurrency: 2\n")
	if err := os.WriteFile(p, body, 0644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SPIDER_STORE_DRIVER", "memory")
	cfg, err := LoadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MetaCache.Driver != "none" || cfg.Cache.Driver != "none" {
		t.Fatalf("meta alias %+v", cfg.MetaCache)
	}
	if cfg.ChunkCache.Dir != "/tmp/chunks" || cfg.DiskCache.Dir != "/tmp/chunks" {
		t.Fatalf("chunk alias %+v", cfg.ChunkCache)
	}
	if cfg.Store.Driver != "memory" {
		t.Fatalf("env overlay %s", cfg.Store.Driver)
	}
	if cfg.Advertisement.BatchSize != 4 || cfg.Download.MaxConcurrency != 2 {
		t.Fatalf("data plane %+v %+v", cfg.Advertisement, cfg.Download)
	}
}

func TestExpandEnvInDSN(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "spider.yaml")
	t.Setenv("SPIDER_STORE_DSN", "postgres://u:p@db:5432/spider?sslmode=require")
	if err := os.WriteFile(p, []byte("store:\n  driver: postgres\n  dsn: ${SPIDER_STORE_DSN}\n"), 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Store.DSN != "postgres://u:p@db:5432/spider?sslmode=require" {
		t.Fatalf("dsn %s", cfg.Store.DSN)
	}
	if RedactedDSN(cfg.Store.DSN) == cfg.Store.DSN {
		t.Fatal("expected redacted dsn")
	}
}
