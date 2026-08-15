package main

import (
	"spider/pkg/config"
	"spider/pkg/metacache"
	"spider/pkg/store"
)

func storePool(p config.PoolConfig) store.Pool {
	return store.Pool{
		MaxOpenConns:    p.MaxOpenConns,
		MaxIdleConns:    p.MaxIdleConns,
		ConnMaxLifetime: p.ConnMaxLifetime,
		ConnMaxIdleTime: p.ConnMaxIdleTime,
	}
}

func cachePool(p config.PoolConfig) metacache.Pool {
	return metacache.Pool{
		MaxOpenConns:    p.MaxOpenConns,
		MaxIdleConns:    p.MaxIdleConns,
		MinIdleConns:    p.MinIdleConns,
		ConnMaxLifetime: p.ConnMaxLifetime,
		ConnMaxIdleTime: p.ConnMaxIdleTime,
		DialTimeout:     p.DialTimeout,
		ReadTimeout:     p.ReadTimeout,
		WriteTimeout:    p.WriteTimeout,
		PoolTimeout:     p.PoolTimeout,
	}
}
