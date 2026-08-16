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

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	v1 "spider/api/v1"
	"spider/api/v1/proto"
	"spider/pkg/advertise"
	"spider/pkg/cache"
	"spider/pkg/config"
	"spider/pkg/engine"
	"spider/pkg/httpserver"
	"spider/pkg/logging"
	"spider/pkg/materializer"
	"spider/pkg/metrics"
	"spider/pkg/peer"
	"spider/pkg/scheduler"
	"spider/pkg/source"
	"spider/pkg/topology"
)

type daemonSyncHandler struct {
	nodeID string
	eng    *engine.Engine
	cache  *cache.ChunkStore
	mgr    *cache.QuotaManager
	s3     source.S3Config
	runCtx context.Context
}

func (h *daemonSyncHandler) HandleSync(ctx context.Context, manifestJSON, destPath, originType, originURI string) (string, error) {
	manifest, err := v1.ParseManifest([]byte(manifestJSON))
	if err != nil {
		return "", fmt.Errorf("invalid manifest JSON: %w", err)
	}
	jobID := fmt.Sprintf("job-%d", time.Now().UnixNano())

	var origin source.Source
	if originURI != "" {
		src, _, err := source.ParseSourceURI(ctx, originURI, h.s3.Endpoint, h.s3.Region, h.s3.AccessKey, h.s3.SecretKey)
		if err != nil {
			slog.Warn("origin uri", "uri", originURI, "err", err)
		} else {
			origin = src
		}
	} else if h.s3.Bucket != "" {
		if s3Src, err := source.NewS3Source(h.s3); err == nil {
			origin = s3Src
		}
	}

	go func() {
		metricsOut, err := h.eng.Sync(h.runCtx, jobID, manifest, destPath, origin)
		if err != nil {
			slog.Error("sync failed", "job", jobID, "err", err)
			return
		}
		slog.Info("sync ok", "job", jobID, "summary", metricsOut.FormatSummary())
		if h.mgr != nil {
			_ = h.mgr.Pin(manifest)
			_, _ = h.mgr.MaybeEvict()
			metrics.CacheUsedBytes.Set(float64(h.mgr.UsedBytes()))
		}
	}()
	return jobID, nil
}

func (h *daemonSyncHandler) GetStatus(jobID string) *proto.GetNodeStatusResponse {
	chunks, _ := h.cache.ListChunks()
	totalBytes, _ := h.cache.TotalCachedBytes()
	var activeJobs []*proto.SyncJobStatus
	for _, j := range h.eng.AllJobs() {
		if jobID == "" || j.JobID == jobID {
			activeJobs = append(activeJobs, &proto.SyncJobStatus{
				JobId:            j.JobID,
				ArtifactId:       j.ArtifactID,
				Status:           j.Status,
				TotalChunks:      j.TotalChunks,
				DownloadedChunks: j.DownloadedChunks,
				PeerChunks:       j.PeerChunks,
				OriginChunks:     j.OriginChunks,
				ErrorMessage:     j.ErrorMessage,
				SkippedChunks:    j.SkippedChunks,
				PeerBytes:        j.PeerBytes,
				OriginBytes:      j.OriginBytes,
			})
		}
	}
	return &proto.GetNodeStatusResponse{
		NodeId:           h.nodeID,
		CachedChunks:     int64(len(chunks)),
		TotalBytesCached: totalBytes,
		ActiveJobs:       activeJobs,
	}
}

func getEnvOrDefault(key, defVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defVal
}

func main() {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = fmt.Sprintf("node-%d", time.Now().Unix())
	}

	configPath := flag.String("config", "", "Path to spider.yaml")
	nodeID := flag.String("node-id", getEnvOrDefault("NODE_ID", hostname), "Node id")
	port := flag.Int("port", 50052, "Peer gRPC port")
	httpAddr := flag.String("http-addr", "", "HTTP addr for metrics/health")
	advertiseAddr := flag.String("advertise-addr", getEnvOrDefault("ADVERTISE_ADDR", ""), "Advertised address")
	trackerAddr := flag.String("tracker", getEnvOrDefault("TRACKER_ADDR", "127.0.0.1:50051"), "Tracker address")
	cacheDir := flag.String("cache-dir", getEnvOrDefault("CACHE_DIR", config.DefaultDataDir), "Chunk cache dir")
	region := flag.String("region", getEnvOrDefault("REGION", "us-east-1"), "Region")
	zone := flag.String("zone", getEnvOrDefault("ZONE", "zone-a"), "Zone")
	rack := flag.String("rack", getEnvOrDefault("RACK", "rack-1"), "Rack")
	host := flag.String("host", getEnvOrDefault("HOST", hostname), "Host")
	logFormat := flag.String("log-format", "", "text | json")
	s3Endpoint := flag.String("s3-endpoint", getEnvOrDefault("MINIO_ENDPOINT", getEnvOrDefault("S3_ENDPOINT", "")), "S3 endpoint")
	s3Bucket := flag.String("s3-bucket", getEnvOrDefault("S3_BUCKET", ""), "S3 bucket (enables S3 origin fallback when set)")
	s3Region := flag.String("s3-region", getEnvOrDefault("AWS_REGION", "us-east-1"), "S3 region")
	s3AccessKey := flag.String("s3-access-key", getEnvOrDefault("MINIO_ROOT_USER", getEnvOrDefault("AWS_ACCESS_KEY_ID", "minioadmin")), "S3 access key")
	s3SecretKey := flag.String("s3-secret-key", getEnvOrDefault("MINIO_ROOT_PASSWORD", getEnvOrDefault("AWS_SECRET_ACCESS_KEY", "minioadmin")), "S3 secret key")
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
	} else if cfg.Node.HTTPAddr != "" {
		cfg.HTTPAddr = cfg.Node.HTTPAddr
	}
	if *cacheDir != "" {
		cfg.ChunkCache.Dir = *cacheDir
	}
	logging.SetDefault(cfg.LogFormat)

	c, err := cache.NewChunkStore(cfg.ChunkCache.Dir)
	if err != nil {
		slog.Error("chunk store init", "err", err)
		os.Exit(1)
	}
	mgr, err := cache.NewQuotaManager(c, cfg.ChunkCache.MaxBytes, cfg.ChunkCache.LowWatermark, cfg.ChunkCache.HighWatermark)
	if err != nil {
		slog.Error("chunk quota manager", "err", err)
		os.Exit(1)
	}
	for _, id := range cfg.ChunkCache.PinnedArtifacts {
		if mf, err := c.GetManifest(id); err == nil {
			_ = mgr.Pin(mf)
			slog.Info("reconciled pin", "artifact", id)
		}
	}

	loc := topology.Locality{Region: *region, Zone: *zone, Rack: *rack, Host: *host}

	runCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	var trackerClient proto.TrackerServiceClient
	var trackerConn *grpc.ClientConn
	if *trackerAddr != "" {
		conn, err := grpc.Dial(*trackerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			slog.Warn("tracker dial", "addr", *trackerAddr, "err", err)
		} else {
			trackerConn = conn
			trackerClient = proto.NewTrackerServiceClient(conn)
		}
	}

	advAddress := *advertiseAddr
	if advAddress == "" {
		advAddress = fmt.Sprintf("%s:%d", *nodeID, *port)
	}

	clientPool := peer.NewClientPoolWithConfig(peer.PoolConfig{
		MaxConnections: cfg.PeerClient.MaxConnections,
		IdleTimeout:    cfg.PeerClient.IdleTimeout,
	})
	eng := engine.NewEngine(engine.Config{
		NodeID:             *nodeID,
		Locality:           loc,
		Cache:              c,
		TrackerClient:      trackerClient,
		ClientPool:         clientPool,
		Materializer:       materializer.NewMaterializer(materializer.DefaultOptions()),
		Scheduler:          scheduler.New(cfg.Download.MaxConcurrency),
		MaxPeerConcurrency:   cfg.Download.MaxConcurrency,
		MaxOriginConcurrency: cfg.Origin.MaxConcurrency,
		Advertisement:        cfg.Advertisement,
		PeerDiscovery:      cfg.PeerDiscovery,
		Retry:              cfg.Retry,
	})

	s3Cfg := source.S3Config{
		Bucket: *s3Bucket, Endpoint: *s3Endpoint, Region: *s3Region,
		AccessKey: *s3AccessKey, SecretKey: *s3SecretKey, UsePathStyle: true,
	}
	handler := &daemonSyncHandler{nodeID: *nodeID, eng: eng, cache: c, mgr: mgr, s3: s3Cfg, runCtx: runCtx}
	var chunkAdvertiser *advertise.Advertiser
	if trackerClient != nil {
		chunkAdvertiser = advertise.New(trackerClient, *nodeID, cfg.Advertisement)
	}
	peerServer := peer.NewServerWithLimits(*nodeID, c, handler, peer.UploadLimits{
		MaxConcurrency:   cfg.Upload.MaxConcurrency,
		MaxQueueSize:     cfg.Upload.MaxQueueSize,
		MaxBandwidthMbps: cfg.Upload.MaxBandwidthMbps,
	})

	httpSrv := httpserver.Start(cfg.HTTPAddr, func(ctx context.Context) error {
		dir := cfg.ChunkCache.Dir
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
		f, err := os.CreateTemp(dir, ".ready-*")
		if err != nil {
			return err
		}
		name := f.Name()
		_ = f.Close()
		return os.Remove(name)
	})

	if trackerClient != nil {
		go func() {
			regCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := trackerClient.RegisterPeer(regCtx, &proto.RegisterPeerRequest{
				Peer: &proto.PeerInfo{NodeId: *nodeID, Address: advAddress, Region: *region, Zone: *zone, Rack: *rack, Host: *host},
			})
			cancel()
			if err != nil {
				slog.Warn("tracker register", "err", err)
			} else {
				slog.Info("registered with tracker", "node", *nodeID, "addr", advAddress)
			}
			existingChunks, _ := cache.StartupReconcileHashes(c, cfg.ChunkCache.PinnedArtifacts)
			if len(existingChunks) > 0 && chunkAdvertiser != nil {
				chunkAdvertiser.Reconcile(existingChunks)
			}
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-runCtx.Done():
					dctx, dcancel := context.WithTimeout(context.Background(), 3*time.Second)
					_, _ = trackerClient.DeregisterPeer(dctx, &proto.DeregisterPeerRequest{NodeId: *nodeID})
					dcancel()
					return
				case <-ticker.C:
					hbCtx, hbCancel := context.WithTimeout(context.Background(), 3*time.Second)
					_, _ = trackerClient.Heartbeat(hbCtx, &proto.HeartbeatRequest{NodeId: *nodeID})
					hbCancel()
					_, _ = mgr.MaybeEvict()
					metrics.CacheUsedBytes.Set(float64(mgr.UsedBytes()))
				}
			}
		}()
	}

	go func() {
		<-runCtx.Done()
		slog.Info("shutting down spiderd")
		if chunkAdvertiser != nil {
			chunkAdvertiser.Stop()
		}
		_ = httpSrv.Shutdown(context.Background())
		peerServer.Stop()
		clientPool.Close()
		if trackerConn != nil {
			_ = trackerConn.Close()
		}
		stop()
		os.Exit(0)
	}()

	slog.Info("spiderd listening", "node", *nodeID, "port", *port, "chunkCache", cfg.ChunkCache.Dir)
	if err := peerServer.Start(*port); err != nil {
		slog.Error("peer server", "err", err)
		os.Exit(1)
	}
}
