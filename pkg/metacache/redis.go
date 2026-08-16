package metacache

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

func init() {
	Register("redis", func(opts Options) (Cache, error) {
		return NewRedis(opts)
	})
}

// Redis is a shared metadata cache for multi-tracker deployments.
type Redis struct {
	client *redis.Client
	prefix string
	ttl    time.Duration
}

func redisClientOptions(opts Options) (*redis.Options, error) {
	target := opts.URL
	if target == "" {
		target = opts.Addr
	}
	var ro *redis.Options
	if strings.Contains(target, "://") {
		parsed, err := redis.ParseURL(target)
		if err != nil {
			return nil, fmt.Errorf("parse redis url: %w", err)
		}
		ro = parsed
	} else {
		addr := target
		if addr == "" {
			addr = "127.0.0.1:6379"
		}
		ro = &redis.Options{
			Addr:     addr,
			Password: opts.Password,
			DB:       opts.DB,
		}
	}
	if opts.Password != "" {
		ro.Password = opts.Password
	}
	if opts.DB != 0 {
		ro.DB = opts.DB
	}
	p := opts.Pool
	poolSize := p.MaxOpenConns
	if poolSize <= 0 {
		poolSize = 10 * runtime.GOMAXPROCS(0)
		if poolSize < 8 {
			poolSize = 8
		}
	}
	minIdle := p.MinIdleConns
	if minIdle <= 0 {
		minIdle = 2
	}
	maxIdle := p.MaxIdleConns
	if maxIdle <= 0 {
		maxIdle = poolSize / 2
		if maxIdle < minIdle {
			maxIdle = minIdle
		}
	}
	dial := p.DialTimeout
	if dial <= 0 {
		dial = 3 * time.Second
	}
	read := p.ReadTimeout
	if read <= 0 {
		read = 2 * time.Second
	}
	write := p.WriteTimeout
	if write <= 0 {
		write = 2 * time.Second
	}
	poolTimeout := p.PoolTimeout
	if poolTimeout <= 0 {
		poolTimeout = 4 * time.Second
	}
	idleTime := p.ConnMaxIdleTime
	if idleTime <= 0 {
		idleTime = 5 * time.Minute
	}
	ro.PoolSize = poolSize
	ro.MinIdleConns = minIdle
	ro.MaxIdleConns = maxIdle
	ro.DialTimeout = dial
	ro.ReadTimeout = read
	ro.WriteTimeout = write
	ro.PoolTimeout = poolTimeout
	ro.ConnMaxLifetime = p.ConnMaxLifetime
	ro.ConnMaxIdleTime = idleTime
	return ro, nil
}

// NewRedis connects to Redis with a bounded connection pool.
func NewRedis(opts Options) (*Redis, error) {
	ro, err := redisClientOptions(opts)
	if err != nil {
		return nil, err
	}
	c := redis.NewClient(ro)
	dial := ro.DialTimeout
	if dial <= 0 {
		dial = 3 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), dial)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("redis ping %s: %w", ro.Addr, err)
	}
	prefix := opts.Prefix
	if prefix == "" {
		prefix = "spider:"
	}
	ttl := opts.TTL
	if ttl <= 0 {
		ttl = 10 * time.Second
	}
	return &Redis{client: c, prefix: prefix, ttl: ttl}, nil
}

func (r *Redis) k(key string) string { return r.prefix + key }

func (r *Redis) Get(ctx context.Context, key string) ([]byte, bool, error) {
	b, err := r.client.Get(ctx, r.k(key)).Bytes()
	if err == redis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("redis get: %w", err)
	}
	return b, true, nil
}

func (r *Redis) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = r.ttl
	}
	if err := r.client.Set(ctx, r.k(key), value, ttl).Err(); err != nil {
		return fmt.Errorf("redis set: %w", err)
	}
	return nil
}

func (r *Redis) MGet(ctx context.Context, keys []string) (map[string][]byte, error) {
	out := make(map[string][]byte, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	rk := make([]string, len(keys))
	for i, k := range keys {
		rk[i] = r.k(k)
	}
	vals, err := r.client.MGet(ctx, rk...).Result()
	if err != nil {
		return nil, fmt.Errorf("redis mget: %w", err)
	}
	for i, v := range vals {
		if v == nil {
			continue
		}
		switch t := v.(type) {
		case string:
			out[keys[i]] = []byte(t)
		case []byte:
			out[keys[i]] = t
		}
	}
	return out, nil
}

func (r *Redis) MSet(ctx context.Context, items map[string][]byte, ttl time.Duration) error {
	if len(items) == 0 {
		return nil
	}
	if ttl <= 0 {
		ttl = r.ttl
	}
	pipe := r.client.Pipeline()
	for k, v := range items {
		pipe.Set(ctx, r.k(k), v, ttl)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis mset: %w", err)
	}
	return nil
}

func (r *Redis) Delete(ctx context.Context, key string) error {
	if err := r.client.Del(ctx, r.k(key)).Err(); err != nil {
		return fmt.Errorf("redis del: %w", err)
	}
	return nil
}

func (r *Redis) DeletePrefix(ctx context.Context, prefix string) error {
	var cursor uint64
	match := r.k(prefix) + "*"
	for {
		keys, next, err := r.client.Scan(ctx, cursor, match, 256).Result()
		if err != nil {
			return fmt.Errorf("redis scan: %w", err)
		}
		if len(keys) > 0 {
			pipe := r.client.Pipeline()
			for _, k := range keys {
				pipe.Unlink(ctx, k)
			}
			if _, err := pipe.Exec(ctx); err != nil {
				return fmt.Errorf("redis unlink: %w", err)
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return nil
}

func (r *Redis) Close() error { return r.client.Close() }
