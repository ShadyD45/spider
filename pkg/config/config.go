package config

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const DefaultDataDir = "/var/lib/spider"

// Config is the shared YAML surface for tracker and spiderd.
type Config struct {
	HTTPAddr      string              `yaml:"httpAddr"`
	LogFormat     string              `yaml:"logFormat"`
	Store         StoreConfig         `yaml:"store"`
	MetaCache     MetaCacheConfig     `yaml:"metaCache"`
	Cache         MetaCacheConfig     `yaml:"cache"`
	ChunkCache    ChunkCacheConfig    `yaml:"chunkCache"`
	DiskCache     ChunkCacheConfig    `yaml:"diskCache"`
	Node          NodeConfig          `yaml:"node"`
	Advertisement AdvertisementConfig `yaml:"advertisement"`
	PeerDiscovery PeerDiscoveryConfig `yaml:"peerDiscovery"`
	Download      DownloadConfig      `yaml:"download"`
	Origin        OriginConfig        `yaml:"origin"`
	Upload        UploadConfig        `yaml:"upload"`
	PeerClient    PeerClientConfig    `yaml:"peerClient"`
	Retry         RetryConfig         `yaml:"retry"`
}

type StoreConfig struct {
	Driver string     `yaml:"driver"` // memory | sqlite | postgres | <registered>
	DSN    string     `yaml:"dsn"`
	Pool   PoolConfig `yaml:"pool"`
}

// MetaCacheConfig is the tracker metadata cache (memory / Redis / none).
type MetaCacheConfig struct {
	Driver string        `yaml:"driver"` // none | memory | redis | <registered>
	TTL    time.Duration `yaml:"ttl"`
	Redis  RedisConfig   `yaml:"redis"`
}

// CacheConfig is a compatibility alias for MetaCacheConfig.
type CacheConfig = MetaCacheConfig

type RedisConfig struct {
	URL      string     `yaml:"url"`
	Addr     string     `yaml:"addr"`
	Password string     `yaml:"password"`
	DB       int        `yaml:"db"`
	Prefix   string     `yaml:"prefix"`
	Pool     PoolConfig `yaml:"pool"`
}

// ChunkCacheConfig is the on-disk content-addressed chunk store.
type ChunkCacheConfig struct {
	Dir             string   `yaml:"dir"`
	MaxBytes        int64    `yaml:"maxBytes"`
	LowWatermark    float64  `yaml:"lowWatermark"`
	HighWatermark   float64  `yaml:"highWatermark"`
	PinnedArtifacts []string `yaml:"pinnedArtifacts"`
}

// DiskCacheConfig is a compatibility alias for ChunkCacheConfig.
type DiskCacheConfig = ChunkCacheConfig

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

type AdvertisementConfig struct {
	BatchSize      int           `yaml:"batchSize"`
	Interval       time.Duration `yaml:"interval"`
	MaxRetries     int           `yaml:"maxRetries"`
	RetryBackoff   time.Duration `yaml:"retryBackoff"`
	MaxRetryBackoff time.Duration `yaml:"maxRetryBackoff"`
}

type PeerDiscoveryConfig struct {
	RefreshInterval time.Duration `yaml:"refreshInterval"`
}

type DownloadConfig struct {
	MaxConcurrency int `yaml:"maxConcurrency"`
}

type OriginConfig struct {
	MaxConcurrency int `yaml:"maxConcurrency"`
}

type UploadConfig struct {
	MaxConcurrency   int `yaml:"maxConcurrency"`
	MaxBandwidthMbps int `yaml:"maxBandwidthMbps"`
	MaxQueueSize     int `yaml:"maxQueueSize"`
}

type PeerClientConfig struct {
	MaxConnections int           `yaml:"maxConnections"`
	IdleTimeout    time.Duration `yaml:"idleTimeout"`
}

type RetryConfig struct {
	MaxAttempts int           `yaml:"maxAttempts"`
	Backoff     BackoffConfig `yaml:"backoff"`
}

type BackoffConfig struct {
	Initial time.Duration `yaml:"initial"`
	Max     time.Duration `yaml:"max"`
}

type fileConfig struct {
	HTTPAddr      string               `yaml:"httpAddr"`
	LogFormat     string               `yaml:"logFormat"`
	Store         *StoreConfig         `yaml:"store"`
	MetaCache     *MetaCacheConfig     `yaml:"metaCache"`
	Cache         *MetaCacheConfig     `yaml:"cache"`
	ChunkCache    *ChunkCacheConfig    `yaml:"chunkCache"`
	DiskCache     *ChunkCacheConfig    `yaml:"diskCache"`
	Node          *NodeConfig          `yaml:"node"`
	Advertisement *AdvertisementConfig `yaml:"advertisement"`
	PeerDiscovery *PeerDiscoveryConfig `yaml:"peerDiscovery"`
	Download      *DownloadConfig      `yaml:"download"`
	Origin        *OriginConfig        `yaml:"origin"`
	Upload        *UploadConfig        `yaml:"upload"`
	PeerClient    *PeerClientConfig    `yaml:"peerClient"`
	Retry         *RetryConfig         `yaml:"retry"`
}

// Defaults returns production-safe defaults.
func Defaults() Config {
	cfg := Config{
		HTTPAddr:  ":9090",
		LogFormat: "text",
		Store: StoreConfig{
			Driver: "sqlite",
			DSN:    DefaultDataDir + "/tracker.db",
		},
		MetaCache: MetaCacheConfig{
			Driver: "memory",
			TTL:    10 * time.Second,
			Redis: RedisConfig{
				Addr:   "127.0.0.1:6379",
				Prefix: "spider:",
			},
		},
		ChunkCache: ChunkCacheConfig{
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
		Advertisement: AdvertisementConfig{BatchSize: 16, Interval: 100 * time.Millisecond, MaxRetries: 5, RetryBackoff: 100 * time.Millisecond, MaxRetryBackoff: 5 * time.Second},
		PeerDiscovery: PeerDiscoveryConfig{RefreshInterval: 500 * time.Millisecond},
		Download:      DownloadConfig{MaxConcurrency: 8},
		Origin:        OriginConfig{MaxConcurrency: 4},
		Upload:        UploadConfig{MaxConcurrency: 16, MaxQueueSize: 100},
		PeerClient:    PeerClientConfig{MaxConnections: 64, IdleTimeout: 2 * time.Minute},
		Retry: RetryConfig{
			MaxAttempts: 3,
			Backoff:     BackoffConfig{Initial: 100 * time.Millisecond, Max: 2 * time.Second},
		},
	}
	cfg.syncAliases()
	return cfg
}

func (c *Config) syncAliases() {
	c.Cache = c.MetaCache
	c.DiskCache = c.ChunkCache
}

// LoadFile reads YAML; missing file is not an error (returns defaults).
func LoadFile(path string) (Config, error) {
	cfg := Defaults()
	if path == "" {
		cfg.ExpandEnv()
		cfg.ApplyEnv()
		cfg.syncAliases()
		return cfg, cfg.Validate()
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			cfg.ExpandEnv()
			cfg.ApplyEnv()
			cfg.syncAliases()
			return cfg, cfg.Validate()
		}
		return cfg, fmt.Errorf("read config %s: %w", path, err)
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("parse config %s: %w", path, err)
	}
	var raw fileConfig
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return cfg, fmt.Errorf("parse config %s: %w", path, err)
	}
	if raw.MetaCache != nil {
		cfg.MetaCache = *raw.MetaCache
	} else {
		cfg.MetaCache = cfg.Cache
	}
	if raw.ChunkCache != nil {
		cfg.ChunkCache = *raw.ChunkCache
	} else {
		cfg.ChunkCache = cfg.DiskCache
	}
	cfg.ExpandEnv()
	cfg.ApplyEnv()
	cfg.applyZeroDefaults()
	cfg.syncAliases()
	if err := cfg.Validate(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func (c *Config) applyZeroDefaults() {
	if c.MetaCache.TTL == 0 {
		c.MetaCache.TTL = 10 * time.Second
	}
	if c.Store.Driver == "" {
		c.Store.Driver = "sqlite"
	}
	if c.MetaCache.Driver == "" {
		c.MetaCache.Driver = "memory"
	}
	if c.Advertisement.BatchSize <= 0 {
		c.Advertisement.BatchSize = 16
	}
	if c.Advertisement.Interval <= 0 {
		c.Advertisement.Interval = 100 * time.Millisecond
	}
	if c.Advertisement.MaxRetries <= 0 {
		c.Advertisement.MaxRetries = 5
	}
	if c.Advertisement.RetryBackoff <= 0 {
		c.Advertisement.RetryBackoff = 100 * time.Millisecond
	}
	if c.Advertisement.MaxRetryBackoff <= 0 {
		c.Advertisement.MaxRetryBackoff = 5 * time.Second
	}
	if c.PeerClient.MaxConnections <= 0 {
		c.PeerClient.MaxConnections = 64
	}
	if c.PeerClient.IdleTimeout <= 0 {
		c.PeerClient.IdleTimeout = 2 * time.Minute
	}
	if c.PeerDiscovery.RefreshInterval <= 0 {
		c.PeerDiscovery.RefreshInterval = 500 * time.Millisecond
	}
	if c.Download.MaxConcurrency <= 0 {
		c.Download.MaxConcurrency = 8
	}
	if c.Origin.MaxConcurrency <= 0 {
		c.Origin.MaxConcurrency = 4
	}
	if c.Upload.MaxConcurrency <= 0 {
		c.Upload.MaxConcurrency = 16
	}
	if c.Retry.MaxAttempts <= 0 {
		c.Retry.MaxAttempts = 3
	}
	if c.Retry.Backoff.Initial <= 0 {
		c.Retry.Backoff.Initial = 100 * time.Millisecond
	}
	if c.Retry.Backoff.Max <= 0 {
		c.Retry.Backoff.Max = 2 * time.Second
	}
	if c.ChunkCache.Dir == "" {
		c.ChunkCache.Dir = DefaultDataDir
	}
	if c.ChunkCache.MaxBytes <= 0 {
		c.ChunkCache.MaxBytes = 500 * 1024 * 1024 * 1024
	}
	if c.ChunkCache.LowWatermark <= 0 {
		c.ChunkCache.LowWatermark = 0.80
	}
	if c.ChunkCache.HighWatermark <= 0 {
		c.ChunkCache.HighWatermark = 0.90
	}
}

// ExpandEnv substitutes ${VAR} in connection strings and secrets.
func (c *Config) ExpandEnv() {
	c.Store.DSN = os.ExpandEnv(c.Store.DSN)
	c.MetaCache.Redis.URL = os.ExpandEnv(c.MetaCache.Redis.URL)
	c.MetaCache.Redis.Addr = os.ExpandEnv(c.MetaCache.Redis.Addr)
	c.MetaCache.Redis.Password = os.ExpandEnv(c.MetaCache.Redis.Password)
	c.MetaCache.Redis.Prefix = os.ExpandEnv(c.MetaCache.Redis.Prefix)
}

// ApplyEnv overlays SPIDER_* environment variables (they win over YAML).
func (c *Config) ApplyEnv() {
	if v := os.Getenv("SPIDER_STORE_DRIVER"); v != "" {
		c.Store.Driver = v
	}
	if v := os.Getenv("SPIDER_STORE_DSN"); v != "" {
		c.Store.DSN = v
	}
	if v := os.Getenv("SPIDER_CACHE_DRIVER"); v != "" {
		c.MetaCache.Driver = v
	}
	if v := os.Getenv("SPIDER_CACHE_REDIS_URL"); v != "" {
		c.MetaCache.Redis.URL = v
	}
	if v := os.Getenv("SPIDER_CACHE_REDIS_PASSWORD"); v != "" {
		c.MetaCache.Redis.Password = v
	}
	if v := os.Getenv("SPIDER_CACHE_REDIS_ADDR"); v != "" {
		c.MetaCache.Redis.Addr = v
	}
}

func (c Config) Validate() error {
	if c.Advertisement.BatchSize < 1 {
		return fmt.Errorf("advertisement.batchSize must be >= 1")
	}
	if c.Download.MaxConcurrency < 1 {
		return fmt.Errorf("download.maxConcurrency must be >= 1")
	}
	if c.Upload.MaxConcurrency < 1 {
		return fmt.Errorf("upload.maxConcurrency must be >= 1")
	}
	if c.Retry.MaxAttempts < 1 {
		return fmt.Errorf("retry.maxAttempts must be >= 1")
	}
	if c.ChunkCache.LowWatermark < 0 || c.ChunkCache.HighWatermark < 0 {
		return fmt.Errorf("chunkCache watermarks must be >= 0")
	}
	return nil
}

// RedactedDSN returns the store DSN with userinfo stripped for logs.
func RedactedDSN(dsn string) string {
	if dsn == "" {
		return ""
	}
	if u, err := url.Parse(dsn); err == nil && u.Scheme != "" && u.Host != "" {
		if u.User != nil {
			u.User = url.User("***")
		}
		return u.Redacted()
	}
	if i := strings.Index(dsn, "@"); i >= 0 {
		return "***" + dsn[i:]
	}
	return dsn
}

// RedisEndpoint describes the Redis target without secrets.
func (r RedisConfig) RedisEndpoint() string {
	if r.URL != "" {
		if u, err := url.Parse(r.URL); err == nil {
			if u.User != nil {
				u.User = url.User("***")
			}
			return u.Redacted()
		}
		return "(redis url)"
	}
	return r.Addr
}
