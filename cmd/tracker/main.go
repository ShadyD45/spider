package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"spider/pkg/tracker"
)

func main() {
	port := flag.Int("port", 50051, "Port for Tracker gRPC server to listen on")
	expiry := flag.Duration("expiry", 30*time.Second, "Heartbeat expiration threshold for mesh peers")
	flag.Parse()

	log.Printf("Starting Spider Central Tracker on port %d (peer expiry: %v)...", *port, *expiry)

	reg := tracker.NewRegistry(*expiry)
	server := tracker.NewServer(reg)

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutting down Tracker gracefully...")
		server.Stop()
		os.Exit(0)
	}()

	if err := server.Start(*port); err != nil {
		fmt.Fprintf(os.Stderr, "Tracker server error: %v\n", err)
		os.Exit(1)
	}
}
