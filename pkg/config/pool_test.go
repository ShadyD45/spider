package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPoolYAMLAndDefaults(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "spider.yaml")
	body := []byte(`store:
  driver: postgres
  dsn: postgres://localhost/spider
  pool:
    maxOpenConns: 40
    maxIdleConns: 10
    connMaxLifetime: 10m
    connMaxIdleTime: 2m
cache:
  driver: redis
  redis:
    pool:
      maxOpenConns: 32
      minIdleConns: 4
      dialTimeout: 1s
`)
	if err := os.WriteFile(p, body, 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Store.Pool.MaxOpenConns != 40 || cfg.Store.Pool.ConnMaxLifetime != 10*time.Minute {
		t.Fatalf("store pool %+v", cfg.Store.Pool)
	}
	if cfg.Cache.Redis.Pool.MaxOpenConns != 32 || cfg.Cache.Redis.Pool.DialTimeout != time.Second {
		t.Fatalf("redis pool %+v", cfg.Cache.Redis.Pool)
	}

	sql := PoolConfig{}.ApplySQLDefaults("sqlite")
	if sql.MaxOpenConns != 8 {
		t.Fatalf("sqlite defaults %+v", sql)
	}
	pg := PoolConfig{}.ApplySQLDefaults("postgres")
	if pg.MaxOpenConns != 25 || pg.MaxIdleConns != 5 {
		t.Fatalf("postgres defaults %+v", pg)
	}
	c := PoolConfig{}.ApplyCacheDefaults()
	if c.MaxOpenConns != 16 || c.DialTimeout != 3*time.Second {
		t.Fatalf("cache defaults %+v", c)
	}
}
