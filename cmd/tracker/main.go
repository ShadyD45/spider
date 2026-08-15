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
	cacheRedisAddr := flag.String("cache-redis-addr", "", "Redis address when cache-driver=redis")
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
		cfg.Cache.Driver = *cacheDriver
	}
	if *cacheRedisAddr != "" {
		cfg.Cache.Redis.Addr = *cacheRedisAddr
	}
	logging.SetDefault(cfg.LogFormat)

	st, err := store.Open(cfg.Store.Driver, store.Options{
		DSN:  cfg.Store.DSN,
		Pool: storePool(cfg.Store.Pool),
	})
	if err != nil {
		slog.Error("open store", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	mc, err := metacache.Open(cfg.Cache.Driver, metacache.Options{
		TTL:      cfg.Cache.TTL,
		Addr:     cfg.Cache.Redis.Addr,
		Password: cfg.Cache.Redis.Password,
		DB:       cfg.Cache.Redis.DB,
		Prefix:   cfg.Cache.Redis.Prefix,
		Pool:     cachePool(cfg.Cache.Redis.Pool),
	})
	if err != nil {
		slog.Error("open metadata cache", "err", err)
		os.Exit(1)
	}
	st = store.Wrap(st, mc, cfg.Cache.TTL)

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
