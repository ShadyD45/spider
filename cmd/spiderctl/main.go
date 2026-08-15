package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	v1 "spider/api/v1"
	"spider/api/v1/proto"
	"spider/pkg/benchmark"
	"spider/pkg/cache"
	"spider/pkg/engine"
	"spider/pkg/source"
	"spider/pkg/verifier"
)

var (
	rootCmd = &cobra.Command{
		Use:   "spiderctl",
		Short: "Spider Artifact Mesh - CLI control tool",
	}

	cacheDirFlag    string
	trackerAddrFlag string
	daemonAddrFlag  string
)

func init() {
	rootCmd.PersistentFlags().StringVar(&cacheDirFlag, "cache-dir", "/var/lib/artifactd", "Path to local cache directory")
	rootCmd.PersistentFlags().StringVar(&trackerAddrFlag, "tracker", "127.0.0.1:50051", "Central tracker address")
	rootCmd.PersistentFlags().StringVar(&daemonAddrFlag, "daemon", "127.0.0.1:50052", "Local spiderd daemon address")

	rootCmd.AddCommand(publishCmd)
	rootCmd.AddCommand(syncCmd)
	rootCmd.AddCommand(inspectCmd)
	rootCmd.AddCommand(statusCmd)
	rootCmd.AddCommand(peersCmd)
	rootCmd.AddCommand(cacheCmd)
	rootCmd.AddCommand(benchmarkCmd)
	rootCmd.AddCommand(verifyCmd)

	verifyCmd.AddCommand(verifyArtifactCmd)
	verifyCmd.AddCommand(verifyCacheCmd)
}

// publishCmd
var (
	pubSourceName string
	pubName       string
	pubVersion    string
	pubChunkSize  int64
	pubOutputFile string
)

var publishCmd = &cobra.Command{
	Use:   "publish",
	Short: "Chunk files from a source directory and create a verified manifest",
	RunE: func(cmd *cobra.Command, args []string) error {
		if pubSourceName == "" {
			return fmt.Errorf("--source is required")
		}
		if pubName == "" {
			return fmt.Errorf("--name is required")
		}
		if pubVersion == "" {
			return fmt.Errorf("--version is required")
		}

		c, err := cache.NewCache(cacheDirFlag)
		if err != nil {
			return fmt.Errorf("failed to open cache: %w", err)
		}

		ctx := context.Background()
		src, prefix, err := source.ParseSourceURI(ctx, pubSourceName, "", "", "", "")
		if err != nil {
			return fmt.Errorf("failed to open source %s: %w", pubSourceName, err)
		}

		pub := engine.NewPublisher(c, pubChunkSize)
		manifest, err := pub.Publish(ctx, src, prefix, pubName, pubVersion)
		if err != nil {
			return fmt.Errorf("publish failed: %w", err)
		}

		manifestJSON, err := json.MarshalIndent(manifest, "", "  ")
		if err != nil {
			return err
		}

		if pubOutputFile != "" {
			if err := os.WriteFile(pubOutputFile, manifestJSON, 0644); err != nil {
				return fmt.Errorf("failed to write output manifest file: %w", err)
			}
			fmt.Printf("Manifest written to %s\n", pubOutputFile)
		} else {
			fmt.Println(string(manifestJSON))
		}

		if trackerAddrFlag != "" {
			conn, err := grpc.Dial(trackerAddrFlag, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err == nil {
				defer conn.Close()
				trClient := proto.NewTrackerServiceClient(conn)
				trCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_, _ = trClient.ReportChunks(trCtx, &proto.ReportChunksRequest{
					NodeId:      "publisher",
					ChunkHashes: manifest.AllChunkHashes(),
				})
			}
		}

		return nil
	},
}

// syncCmd
var (
	syncManifestFile string
	syncDestDir      string
	syncOriginURI    string
)

var syncCmd = &cobra.Command{
	Use:   "sync",
	Short: "Synchronize an artifact to local destination directory",
	RunE: func(cmd *cobra.Command, args []string) error {
		if syncManifestFile == "" {
			return fmt.Errorf("--manifest is required")
		}
		if syncDestDir == "" {
			return fmt.Errorf("--dest is required")
		}

		manifestData, err := os.ReadFile(syncManifestFile)
		if err != nil {
			return fmt.Errorf("failed to read manifest file %s: %w", syncManifestFile, err)
		}

		conn, err := grpc.Dial(daemonAddrFlag, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("failed to connect to daemon at %s: %w", daemonAddrFlag, err)
		}
		defer conn.Close()

		client := proto.NewPeerServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		resp, err := client.SyncArtifact(ctx, &proto.SyncArtifactRequest{
			ManifestJson:    string(manifestData),
			DestinationPath: syncDestDir,
			OriginUri:       syncOriginURI,
		})
		if err != nil {
			return fmt.Errorf("SyncArtifact RPC error: %w", err)
		}

		fmt.Printf("Sync request submitted successfully. Job ID: %s (Status: %s)\n", resp.JobId, resp.Message)
		return nil
	},
}

// inspectCmd
var inspectManifestFile string

var inspectCmd = &cobra.Command{
	Use:   "inspect",
	Short: "Inspect an artifact manifest",
	RunE: func(cmd *cobra.Command, args []string) error {
		if inspectManifestFile == "" {
			return fmt.Errorf("--manifest is required")
		}

		data, err := os.ReadFile(inspectManifestFile)
		if err != nil {
			return err
		}

		m, err := v1.ParseManifest(data)
		if err != nil {
			return err
		}

		fmt.Printf("Artifact ID:  %s\n", m.ArtifactID)
		fmt.Printf("Name:         %s\n", m.Name)
		fmt.Printf("Version:      %s\n", m.Version)
		fmt.Printf("Chunk Size:   %d bytes\n", m.ChunkSize)
		fmt.Printf("Total Size:   %d bytes\n", m.TotalSize)
		fmt.Printf("Total Files:  %d\n", len(m.Files))
		fmt.Printf("Total Chunks: %d\n\n", len(m.AllChunkHashes()))

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "FILE PATH\tSIZE (BYTES)\tMODE\tCHUNKS")
		for _, f := range m.Files {
			fmt.Fprintf(w, "%s\t%d\t%s\t%d\n", f.Path, f.Size, f.Mode, len(f.Chunks))
		}
		return w.Flush()
	},
}

// statusCmd
var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Query status of local node daemon and sync jobs",
	RunE: func(cmd *cobra.Command, args []string) error {
		conn, err := grpc.Dial(daemonAddrFlag, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("failed to connect to daemon at %s: %w", daemonAddrFlag, err)
		}
		defer conn.Close()

		client := proto.NewPeerServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		resp, err := client.GetNodeStatus(ctx, &proto.GetNodeStatusRequest{})
		if err != nil {
			return fmt.Errorf("GetNodeStatus RPC error: %w", err)
		}

		fmt.Printf("Node ID:          %s\n", resp.NodeId)
		fmt.Printf("Cached Chunks:    %d\n", resp.CachedChunks)
		fmt.Printf("Total Bytes:      %d (%.2f MB)\n", resp.TotalBytesCached, float64(resp.TotalBytesCached)/(1024*1024))
		fmt.Printf("Active Jobs:      %d\n\n", len(resp.ActiveJobs))

		if len(resp.ActiveJobs) > 0 {
			w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
			fmt.Fprintln(w, "JOB ID\tARTIFACT ID\tSTATUS\tPROGRESS\tPEER CHUNKS\tORIGIN CHUNKS")
			for _, j := range resp.ActiveJobs {
				progress := fmt.Sprintf("%d/%d", j.DownloadedChunks, j.TotalChunks)
				fmt.Fprintf(w, "%s\t%.16s...\t%s\t%s\t%d\t%d\n",
					j.JobId, j.ArtifactId, j.Status, progress, j.PeerChunks, j.OriginChunks)
			}
			_ = w.Flush()
		}
		return nil
	},
}

// peersCmd
var peersCmd = &cobra.Command{
	Use:   "peers",
	Short: "Query active peers registered on the Tracker",
	RunE: func(cmd *cobra.Command, args []string) error {
		conn, err := grpc.Dial(trackerAddrFlag, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("failed to connect to tracker at %s: %w", trackerAddrFlag, err)
		}
		defer conn.Close()

		client := proto.NewTrackerServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		resp, err := client.ListPeers(ctx, &proto.ListPeersRequest{})
		if err != nil {
			return fmt.Errorf("ListPeers RPC error: %w", err)
		}

		fmt.Printf("Active Peers (%d):\n\n", len(resp.Peers))
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "NODE ID\tADDRESS\tRACK\tZONE\tREGION\tLAST SEEN")
		for _, p := range resp.Peers {
			lastSeen := time.Unix(p.LastSeenUnix, 0).Format("15:04:05")
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n",
				p.NodeId, p.Address, p.Rack, p.Zone, p.Region, lastSeen)
		}
		return w.Flush()
	},
}

// cacheCmd
var cacheCmd = &cobra.Command{
	Use:   "cache",
	Short: "Inspect local content-addressed chunk cache",
	RunE: func(cmd *cobra.Command, args []string) error {
		c, err := cache.NewCache(cacheDirFlag)
		if err != nil {
			return err
		}

		chunks, err := c.ListChunks()
		if err != nil {
			return err
		}

		totalBytes, _ := c.TotalCachedBytes()
		fmt.Printf("Cache directory: %s\n", cacheDirFlag)
		fmt.Printf("Total chunks:    %d\n", len(chunks))
		fmt.Printf("Total size:      %d bytes (%.2f MB)\n\n", totalBytes, float64(totalBytes)/(1024*1024))

		for i, ch := range chunks {
			if i >= 20 {
				fmt.Printf("... and %d more chunks\n", len(chunks)-20)
				break
			}
			fmt.Println(ch)
		}
		return nil
	},
}

// benchmarkCmd
var (
	benchSizeMB  int64
	benchWorkers int
	benchChunkMB int64
)

var benchmarkCmd = &cobra.Command{
	Use:   "benchmark",
	Short: "Run automated comparative benchmark between Direct Origin and Spider P2P Mesh",
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Println("========================================================================")
		fmt.Printf("  Spider Artifact Mesh — Automated Distribution Benchmark\n")
		fmt.Printf("  Model Size: %d MB | Workers: %d | Chunk Size: %d MB\n", benchSizeMB, benchWorkers, benchChunkMB)
		fmt.Println("========================================================================")

		suite := benchmark.NewSuite("")
		ctx := context.Background()

		artifactBytes := benchSizeMB * 1024 * 1024
		chunkBytes := benchChunkMB * 1024 * 1024

		originRes, meshRes, err := suite.RunComparison(ctx, artifactBytes, benchWorkers, chunkBytes)
		if err != nil {
			return fmt.Errorf("benchmark execution failed: %w", err)
		}

		fmt.Println()
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "METRIC\tDIRECT ORIGIN (BASELINE)\tSPIDER P2P MESH\tIMPROVEMENT")
		fmt.Fprintln(w, "------\t------------------------\t---------------\t-----------")
		fmt.Fprintf(w, "Duration\t%v\t%v\t%.2fx speedup\n",
			originRes.Duration.Round(time.Millisecond), meshRes.Duration.Round(time.Millisecond), meshRes.SpeedupFactor)
		fmt.Fprintf(w, "Fleet Throughput\t%.2f MB/s\t%.2f MB/s\t+%.1f%%\n",
			originRes.TotalThroughputMBs, meshRes.TotalThroughputMBs,
			((meshRes.TotalThroughputMBs-originRes.TotalThroughputMBs)/originRes.TotalThroughputMBs)*100.0)
		fmt.Fprintf(w, "Origin Data Transferred\t%.2f MB\t%.2f MB\t%.1f%% bandwidth saved\n",
			float64(originRes.OriginBytesTransferred)/(1024*1024),
			float64(meshRes.OriginBytesTransferred)/(1024*1024),
			meshRes.OriginBandwidthSavedPct)
		fmt.Fprintf(w, "Peer Data Transferred\t0.00 MB\t%.2f MB\t-\n",
			float64(meshRes.PeerBytesTransferred)/(1024*1024))
		_ = w.Flush()

		fmt.Println("\n------------------------------------------------------------------------")
		fmt.Printf("Summary: Spider P2P mesh saved %.1f%% origin storage bandwidth and achieved a %.2fx speedup across %d nodes.\n",
			meshRes.OriginBandwidthSavedPct, meshRes.SpeedupFactor, benchWorkers)
		fmt.Println("------------------------------------------------------------------------")

		return nil
	},
}

// verify commands
var (
	verifyManifestPath string
	verifyTargetPath   string
)

var verifyCmd = &cobra.Command{
	Use:   "verify",
	Short: "Cryptographically verify data integrity for cache or materialized artifacts",
}

var verifyArtifactCmd = &cobra.Command{
	Use:   "artifact",
	Short: "Verify materialized directory files and chunks against canonical manifest",
	RunE: func(cmd *cobra.Command, args []string) error {
		if verifyManifestPath == "" {
			return fmt.Errorf("--manifest is required")
		}
		if verifyTargetPath == "" {
			return fmt.Errorf("--dest is required")
		}

		data, err := os.ReadFile(verifyManifestPath)
		if err != nil {
			return fmt.Errorf("failed to read manifest %s: %w", verifyManifestPath, err)
		}

		manifest, err := v1.ParseManifest(data)
		if err != nil {
			return fmt.Errorf("invalid manifest: %w", err)
		}

		fmt.Printf("Verifying materialized directory: %s\n", verifyTargetPath)
		fmt.Printf("Artifact ID: %s (%s@%s)\n\n", manifest.ArtifactID, manifest.Name, manifest.Version)

		ctx := context.Background()
		report, err := verifier.VerifyMaterializedDirectory(ctx, manifest, verifyTargetPath)
		if err != nil {
			return fmt.Errorf("verification error: %w", err)
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "FILE PATH\tSTATUS\tCHUNKS VERIFIED\tDETAILS")
		fmt.Fprintln(w, "---------\t------\t---------------\t-------")
		for _, f := range report.FileResults {
			status := "VALID"
			if !f.Valid {
				status = "CORRUPT/MISSING"
			}
			chunkProgress := fmt.Sprintf("%d/%d", f.VerifiedChunks, f.TotalChunks)
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", f.Path, status, chunkProgress, f.Error)
		}
		_ = w.Flush()

		fmt.Printf("\nSummary: %d/%d files valid, %d corrupt, %d missing. Total chunks verified: %d/%d\n",
			report.ValidFiles, report.TotalFiles, report.CorruptFiles, report.MissingFiles, report.VerifiedChunks, report.TotalChunks)

		if !report.AllValid {
			return fmt.Errorf("cryptographic verification failed for artifact %s", manifest.ArtifactID)
		}

		fmt.Println("All files and chunk hashes successfully verified (100% SHA-256 integrity match).")
		return nil
	},
}

var verifyCacheCmd = &cobra.Command{
	Use:   "cache",
	Short: "Audit chunk cache for silent bit-rot or file corruption",
	RunE: func(cmd *cobra.Command, args []string) error {
		c, err := cache.NewCache(cacheDirFlag)
		if err != nil {
			return err
		}

		fmt.Printf("Auditing cache integrity at %s...\n", cacheDirFlag)
		verified, corrupt, err := verifier.VerifyCache(c)
		if err != nil {
			return err
		}

		fmt.Printf("Audit completed: %d chunks valid, %d corrupt.\n", verified, len(corrupt))
		if len(corrupt) > 0 {
			fmt.Println("Corrupted chunk hashes:")
			for _, h := range corrupt {
				fmt.Printf("  - %s\n", h)
			}
			return fmt.Errorf("cache integrity audit failed: %d corrupt chunks found", len(corrupt))
		}

		fmt.Println("All cached chunks passed SHA-256 integrity verification.")
		return nil
	},
}

func init() {
	publishCmd.Flags().StringVarP(&pubSourceName, "source", "s", "", "Source directory or URI (e.g. /models/v1 or s3://bucket/path)")
	publishCmd.Flags().StringVarP(&pubName, "name", "n", "", "Artifact name (e.g. llama-3-8b)")
	publishCmd.Flags().StringVarP(&pubVersion, "version", "v", "", "Artifact version (e.g. 1.0.0)")
	publishCmd.Flags().Int64Var(&pubChunkSize, "chunk-size", v1.DefaultChunkSize, "Chunk size in bytes (default 4 MiB)")
	publishCmd.Flags().StringVarP(&pubOutputFile, "output", "o", "", "Output manifest JSON file path")

	syncCmd.Flags().StringVarP(&syncManifestFile, "manifest", "m", "", "Path to artifact manifest JSON file")
	syncCmd.Flags().StringVarP(&syncDestDir, "dest", "d", "", "Destination directory path to materialize files")
	syncCmd.Flags().StringVar(&syncOriginURI, "origin", "", "Fallback origin URI (e.g. s3://bucket/path)")

	inspectCmd.Flags().StringVarP(&inspectManifestFile, "manifest", "m", "", "Path to artifact manifest JSON file")

	benchmarkCmd.Flags().Int64Var(&benchSizeMB, "size", 50, "Total synthetic model size in Megabytes (MB)")
	benchmarkCmd.Flags().IntVar(&benchWorkers, "workers", 4, "Number of concurrent simulated worker nodes")
	benchmarkCmd.Flags().Int64Var(&benchChunkMB, "chunk-size", 4, "Chunk size in Megabytes (MB)")

	verifyArtifactCmd.Flags().StringVarP(&verifyManifestPath, "manifest", "m", "", "Path to artifact manifest JSON file")
	verifyArtifactCmd.Flags().StringVarP(&verifyTargetPath, "dest", "d", "", "Path to materialized directory")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
