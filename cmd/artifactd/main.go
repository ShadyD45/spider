package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	v1 "spider/api/v1"
	"spider/api/v1/proto"
	"spider/pkg/cache"
	"spider/pkg/engine"
	"spider/pkg/materializer"
	"spider/pkg/peer"
	"spider/pkg/source"
	"spider/pkg/topology"
)

type daemonSyncHandler struct {
	nodeID   string
	eng      *engine.Engine
	cache    *cache.Cache
	s3Config source.S3Config
}

func (h *daemonSyncHandler) HandleSync(ctx context.Context, manifestJSON, destPath, originType, originURI string) (string, error) {
	manifest, err := v1.ParseManifest([]byte(manifestJSON))
	if err != nil {
		return "", fmt.Errorf("invalid manifest JSON: %w", err)
	}

	jobID := fmt.Sprintf("job-%d", time.Now().UnixNano())

	// Resolve origin source
	var origin source.Source
	if originURI != "" {
		src, _, err := source.ParseSourceURI(ctx, originURI, h.s3Config.Endpoint, h.s3Config.Region, h.s3Config.AccessKey, h.s3Config.SecretKey)
		if err != nil {
			log.Printf("[Daemon] Warning parsing origin URI %s: %v", originURI, err)
		} else {
			origin = src
		}
	} else if h.s3Config.Bucket != "" || h.s3Config.Endpoint != "" {
		s3Src, err := source.NewS3Source(h.s3Config)
		if err == nil {
			origin = s3Src
		}
	}

	// Trigger async sync in goroutine
	go func() {
		bgCtx := context.Background()
		metrics, err := h.eng.Sync(bgCtx, jobID, manifest, destPath, origin)
		if err != nil {
			log.Printf("[Daemon] Sync job %s failed: %v", jobID, err)
		} else {
			log.Printf("[Daemon] Sync job %s succeeded: %d peer chunks, %d origin chunks in %v",
				jobID, metrics.PeerChunks, metrics.OriginChunks, metrics.Duration)
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

	nodeID := flag.String("node-id", getEnvOrDefault("NODE_ID", hostname), "Unique identifier for this node")
	port := flag.Int("port", 50052, "Port for peer chunk streaming gRPC service")
	advertiseAddr := flag.String("advertise-addr", getEnvOrDefault("ADVERTISE_ADDR", ""), "Address:port advertised to mesh peers")
	trackerAddr := flag.String("tracker", getEnvOrDefault("TRACKER_ADDR", "127.0.0.1:50051"), "Tracker gRPC address")
	cacheDir := flag.String("cache-dir", getEnvOrDefault("CACHE_DIR", "/var/lib/artifactd"), "Local disk cache directory")
	region := flag.String("region", getEnvOrDefault("REGION", "us-east-1"), "Topology region")
	zone := flag.String("zone", getEnvOrDefault("ZONE", "zone-a"), "Topology zone")
	rack := flag.String("rack", getEnvOrDefault("RACK", "rack-1"), "Topology rack")
	host := flag.String("host", getEnvOrDefault("HOST", hostname), "Topology host")

	s3Endpoint := flag.String("s3-endpoint", getEnvOrDefault("MINIO_ENDPOINT", getEnvOrDefault("S3_ENDPOINT", "")), "S3 / MinIO endpoint URL")
	s3Bucket := flag.String("s3-bucket", getEnvOrDefault("S3_BUCKET", "artifacts"), "S3 / MinIO default bucket")
	s3Region := flag.String("s3-region", getEnvOrDefault("AWS_REGION", "us-east-1"), "S3 region")
	s3AccessKey := flag.String("s3-access-key", getEnvOrDefault("MINIO_ROOT_USER", getEnvOrDefault("AWS_ACCESS_KEY_ID", "minioadmin")), "S3 access key")
	s3SecretKey := flag.String("s3-secret-key", getEnvOrDefault("MINIO_ROOT_PASSWORD", getEnvOrDefault("AWS_SECRET_ACCESS_KEY", "minioadmin")), "S3 secret key")
	flag.Parse()

	log.Printf("[Daemon] Initializing artifactd on node %s (port %d, cache: %s)...", *nodeID, *port, *cacheDir)

	c, err := cache.NewCache(*cacheDir)
	if err != nil {
		log.Fatalf("Failed to initialize cache: %v", err)
	}

	loc := topology.Locality{
		Region: *region,
		Zone:   *zone,
		Rack:   *rack,
		Host:   *host,
	}

	// Connect to Central Tracker if configured
	var trackerClient proto.TrackerServiceClient
	if *trackerAddr != "" {
		conn, err := grpc.Dial(*trackerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("[Daemon] Warning: could not connect to tracker at %s: %v", *trackerAddr, err)
		} else {
			defer conn.Close()
			trackerClient = proto.NewTrackerServiceClient(conn)
		}
	}

	// Build address advertised to peers
	advAddress := *advertiseAddr
	if advAddress == "" {
		advAddress = fmt.Sprintf("%s:%d", *nodeID, *port)
	}

	clientPool := peer.NewClientPool()
	defer clientPool.Close()

	eng := engine.NewEngine(engine.Config{
		NodeID:        *nodeID,
		Locality:      loc,
		Cache:         c,
		TrackerClient: trackerClient,
		ClientPool:    clientPool,
		Materializer:  materializer.NewMaterializer(materializer.DefaultOptions()),
	})

	s3Cfg := source.S3Config{
		Bucket:       *s3Bucket,
		Endpoint:     *s3Endpoint,
		Region:       *s3Region,
		AccessKey:    *s3AccessKey,
		SecretKey:    *s3SecretKey,
		UsePathStyle: true,
	}

	syncHandler := &daemonSyncHandler{
		nodeID:   *nodeID,
		eng:      eng,
		cache:    c,
		s3Config: s3Cfg,
	}

	peerServer := peer.NewServer(*nodeID, c, syncHandler)

	// Heartbeat and registration loop
	if trackerClient != nil {
		go func() {
			regCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := trackerClient.RegisterPeer(regCtx, &proto.RegisterPeerRequest{
				Peer: &proto.PeerInfo{
					NodeId:  *nodeID,
					Address: advAddress,
					Region:  *region,
					Zone:    *zone,
					Rack:    *rack,
					Host:    *host,
				},
			})
			cancel()
			if err != nil {
				log.Printf("[Daemon] Tracker initial registration failed: %v", err)
			} else {
				log.Printf("[Daemon] Successfully registered with Tracker as %s (%s)", *nodeID, advAddress)
			}

			// Report existing cached chunks
			existingChunks, _ := c.ListChunks()
			if len(existingChunks) > 0 {
				repCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_, _ = trackerClient.ReportChunks(repCtx, &proto.ReportChunksRequest{
					NodeId:      *nodeID,
					ChunkHashes: existingChunks,
				})
				cancel()
				log.Printf("[Daemon] Reported %d existing cached chunks to tracker", len(existingChunks))
			}

			// Periodic heartbeat
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for range ticker.C {
				hbCtx, hbCancel := context.WithTimeout(context.Background(), 3*time.Second)
				_, _ = trackerClient.Heartbeat(hbCtx, &proto.HeartbeatRequest{NodeId: *nodeID})
				hbCancel()
			}
		}()
	}

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("[Daemon] Shutting down artifactd gracefully...")
		peerServer.Stop()
		os.Exit(0)
	}()

	if err := peerServer.Start(*port); err != nil {
		log.Fatalf("Peer server failed: %v", err)
	}
}
