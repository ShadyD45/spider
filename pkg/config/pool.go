package config

import "time"

// PoolConfig is the connection-pool surface for every durable backend
// (SQLite, Postgres, Redis, and any future driver). Zero values mean
// "use driver defaults" after ApplySQLDefaults / ApplyCacheDefaults.
type PoolConfig struct {
	MaxOpenConns    int           `yaml:"maxOpenConns"`
	MaxIdleConns    int           `yaml:"maxIdleConns"`
	MinIdleConns    int           `yaml:"minIdleConns"`
	ConnMaxLifetime time.Duration `yaml:"connMaxLifetime"`
	ConnMaxIdleTime time.Duration `yaml:"connMaxIdleTime"`
	DialTimeout     time.Duration `yaml:"dialTimeout"`
	ReadTimeout     time.Duration `yaml:"readTimeout"`
	WriteTimeout    time.Duration `yaml:"writeTimeout"`
	PoolTimeout     time.Duration `yaml:"poolTimeout"`
}

// ApplySQLDefaults fills zeros for sqlite or postgres (and future SQL drivers).
func (p PoolConfig) ApplySQLDefaults(driver string) PoolConfig {
	if driver == "sqlite" {
		if p.MaxOpenConns <= 0 {
			p.MaxOpenConns = 8
		}
		if p.MaxIdleConns <= 0 {
			p.MaxIdleConns = p.MaxOpenConns
		}
		return p
	}
	if p.MaxOpenConns <= 0 {
		p.MaxOpenConns = 25
	}
	if p.MaxIdleConns <= 0 {
		p.MaxIdleConns = 5
	}
	if p.ConnMaxLifetime == 0 {
		p.ConnMaxLifetime = 5 * time.Minute
	}
	if p.ConnMaxIdleTime == 0 {
		p.ConnMaxIdleTime = time.Minute
	}
	return p
}

// ApplyCacheDefaults fills zeros for Redis-style pooled caches.
func (p PoolConfig) ApplyCacheDefaults() PoolConfig {
	if p.MaxOpenConns <= 0 {
		p.MaxOpenConns = 16
	}
	if p.MinIdleConns <= 0 {
		p.MinIdleConns = 2
	}
	if p.MaxIdleConns <= 0 {
		p.MaxIdleConns = p.MaxOpenConns / 2
		if p.MaxIdleConns < p.MinIdleConns {
			p.MaxIdleConns = p.MinIdleConns
		}
	}
	if p.DialTimeout <= 0 {
		p.DialTimeout = 3 * time.Second
	}
	if p.ReadTimeout <= 0 {
		p.ReadTimeout = 2 * time.Second
	}
	if p.WriteTimeout <= 0 {
		p.WriteTimeout = 2 * time.Second
	}
	if p.PoolTimeout <= 0 {
		p.PoolTimeout = 4 * time.Second
	}
	if p.ConnMaxIdleTime <= 0 {
		p.ConnMaxIdleTime = 5 * time.Minute
	}
	return p
}
