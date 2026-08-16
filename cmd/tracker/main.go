package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"spider/pkg/config"
	"spider/pkg/httpserver"
	"spider/pkg/logging"
	"spider/pkg/metacache"
	"spider/pkg/store"
	"spider/pkg/tracker"
)

func main() {
	configPath := flag.String("config", "", "Path to spider.yaml")
	port := flag.Int("port", 50051, "gRPC listen port")
	httpAddr := flag.String("http-addr", "", "HTTP listen addr for /metrics /healthz /readyz")
	expiry := flag.Duration("expiry", 30*time.Second, "Peer heartbeat expiry")
	storeDriver := flag.String("store-driver", "", "memory | sqlite | postgres")
	storeDSN := flag.String("store-dsn", "", "Store DSN")
	cacheDriver := flag.String("cache-driver", "", "none | memory | redis")
	cacheRedisAddr := flag.String("cache-redis-addr", "", "Redis host:port when cache-driver=redis")
	cacheRedisURL := flag.String("cache-redis-url", "", "Redis URL (redis:// or rediss://)")
	logFormat := flag.String("log-format", "", "text | json")
	flag.Parse()

	cfg, err := config.LoadFile(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		os.Exit(1)
	}
	if *logFormat != "" {
		cfg.LogFormat = *logFormat
	}
	if *httpAddr != "" {
		cfg.HTTPAddr = *httpAddr
	}
	if *storeDriver != "" {
		cfg.Store.Driver = *storeDriver
	}
	if *storeDSN != "" {
		cfg.Store.DSN = *storeDSN
	}
	if *cacheDriver != "" {
		cfg.MetaCache.Driver = *cacheDriver
	}
	if *cacheRedisAddr != "" {
		cfg.MetaCache.Redis.Addr = *cacheRedisAddr
	}
	if *cacheRedisURL != "" {
		cfg.MetaCache.Redis.URL = *cacheRedisURL
	}
	cfg.Cache = cfg.MetaCache
	logging.SetDefault(cfg.LogFormat)

	slog.Info("tracker backends",
		"store", cfg.Store.Driver,
		"dsn", config.RedactedDSN(cfg.Store.DSN),
		"metaCache", cfg.MetaCache.Driver,
		"redis", cfg.MetaCache.Redis.RedisEndpoint(),
	)

	st, err := store.Open(cfg.Store.Driver, store.Options{
		DSN:  cfg.Store.DSN,
		Pool: storePool(cfg.Store.Pool),
	})
	if err != nil {
		slog.Error("open store", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	mc, err := metacache.Open(cfg.MetaCache.Driver, metacache.Options{
		TTL:      cfg.MetaCache.TTL,
		URL:      cfg.MetaCache.Redis.URL,
		Addr:     cfg.MetaCache.Redis.Addr,
		Password: cfg.MetaCache.Redis.Password,
		DB:       cfg.MetaCache.Redis.DB,
		Prefix:   cfg.MetaCache.Redis.Prefix,
		Pool:     cachePool(cfg.MetaCache.Redis.Pool),
	})
	if err != nil {
		slog.Error("open metadata cache", "err", err)
		os.Exit(1)
	}
	st = store.Wrap(st, mc, cfg.MetaCache.TTL)

	reg := tracker.NewRegistryWithStore(st, *expiry)
	server := tracker.NewServer(reg)

	httpSrv := httpserver.Start(cfg.HTTPAddr, func(ctx context.Context) error {
		return server.Ping(ctx)
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ctx.Done()
		slog.Info("shutting down tracker")
		_ = httpSrv.Shutdown(context.Background())
		server.Stop()
		stop()
	}()

	if err := server.Start(*port); err != nil {
		fmt.Fprintf(os.Stderr, "Tracker server error: %v\n", err)
		os.Exit(1)
	}
}
