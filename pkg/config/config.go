package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

const DefaultDataDir = "/var/lib/spider"

// Config is the shared YAML surface for tracker and spiderd.
type Config struct {
	HTTPAddr  string          `yaml:"httpAddr"`
	LogFormat string          `yaml:"logFormat"`
	Store     StoreConfig     `yaml:"store"`
	Cache     CacheConfig     `yaml:"cache"`
	DiskCache DiskCacheConfig `yaml:"diskCache"`
	Node      NodeConfig      `yaml:"node"`
}

type StoreConfig struct {
	Driver string     `yaml:"driver"` // memory | sqlite | postgres | <registered>
	DSN    string     `yaml:"dsn"`
	Pool   PoolConfig `yaml:"pool"`
}

type CacheConfig struct {
	Driver string        `yaml:"driver"` // none | memory | redis | <registered>
	TTL    time.Duration `yaml:"ttl"`
	Redis  RedisConfig   `yaml:"redis"`
}

type RedisConfig struct {
	Addr     string     `yaml:"addr"`
	Password string     `yaml:"password"`
	DB       int        `yaml:"db"`
	Prefix   string     `yaml:"prefix"`
	Pool     PoolConfig `yaml:"pool"`
}

type DiskCacheConfig struct {
	Dir             string   `yaml:"dir"`
	MaxBytes        int64    `yaml:"maxBytes"`
	LowWatermark    float64  `yaml:"lowWatermark"`
	HighWatermark   float64  `yaml:"highWatermark"`
	PinnedArtifacts []string `yaml:"pinnedArtifacts"`
}

type NodeConfig struct {
	ID            string `yaml:"id"`
	Port          int    `yaml:"port"`
	AdvertiseAddr string `yaml:"advertiseAddr"`
	TrackerAddr   string `yaml:"tracker"`
	Region        string `yaml:"region"`
	Zone          string `yaml:"zone"`
	Rack          string `yaml:"rack"`
	Host          string `yaml:"host"`
	HTTPAddr      string `yaml:"httpAddr"`
}

// Defaults returns production-safe defaults.
func Defaults() Config {
	return Config{
		HTTPAddr:  ":9090",
		LogFormat: "text",
		Store: StoreConfig{
			Driver: "sqlite",
			DSN:    DefaultDataDir + "/tracker.db",
			Pool:   PoolConfig{}, // driver defaults via ApplySQLDefaults
		},
		Cache: CacheConfig{
			Driver: "memory",
			TTL:    10 * time.Second,
			Redis: RedisConfig{
				Addr:   "127.0.0.1:6379",
				Prefix: "spider:",
			},
		},
		DiskCache: DiskCacheConfig{
			Dir:           DefaultDataDir,
			MaxBytes:      500 * 1024 * 1024 * 1024,
			LowWatermark:  0.80,
			HighWatermark: 0.90,
		},
		Node: NodeConfig{
			Port:        50052,
			TrackerAddr: "127.0.0.1:50051",
			Region:      "us-east-1",
			Zone:        "zone-a",
			Rack:        "rack-1",
		},
	}
}

// LoadFile reads YAML; missing file is not an error (returns defaults).
func LoadFile(path string) (Config, error) {
	cfg := Defaults()
	if path == "" {
		return cfg, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return cfg, fmt.Errorf("read config %s: %w", path, err)
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("parse config %s: %w", path, err)
	}
	if cfg.Cache.TTL == 0 {
		cfg.Cache.TTL = 10 * time.Second
	}
	if cfg.Store.Driver == "" {
		cfg.Store.Driver = "sqlite"
	}
	if cfg.Cache.Driver == "" {
		cfg.Cache.Driver = "memory"
	}
	return cfg, nil
}
