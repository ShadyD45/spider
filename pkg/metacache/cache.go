package metacache

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Cache is a namespaced TTL key-value store for tracker metadata.
type Cache interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
	DeletePrefix(ctx context.Context, prefix string) error
	Close() error
}

type opener func(opts Options) (Cache, error)

var (
	mu       sync.RWMutex
	openers  = map[string]opener{}
)

// Options configure a cache driver. New backends must honor Pool
// (zero values mean driver defaults).
type Options struct {
	TTL      time.Duration
	Addr     string
	Password string
	DB       int
	Prefix   string
	Pool     Pool
}

// Pool is the connection pool for a networked cache driver (Redis today).
type Pool struct {
	MaxOpenConns    int
	MaxIdleConns    int
	MinIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
	DialTimeout     time.Duration
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	PoolTimeout     time.Duration
}

// Register a cache driver. Name is cache.driver in YAML. New backends must
// honor Options.Pool with zero meaning driver defaults.
func Register(name string, fn opener) {
	mu.Lock()
	defer mu.Unlock()
	openers[name] = fn
}

// Open constructs a Cache by driver name.
func Open(driver string, opts Options) (Cache, error) {
	if opts.TTL <= 0 {
		opts.TTL = 10 * time.Second
	}
	if opts.Prefix == "" {
		opts.Prefix = "spider:"
	}
	mu.RLock()
	fn, ok := openers[driver]
	mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown cache driver %q (registered: memory, redis, none)", driver)
	}
	return fn(opts)
}

func init() {
	Register("none", func(Options) (Cache, error) { return Nop{}, nil })
	Register("memory", func(opts Options) (Cache, error) { return NewMemory(opts.TTL), nil })
}
