package httpserver

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ReadyFunc returns nil when the process can serve traffic.
type ReadyFunc func(ctx context.Context) error

// Server is a small HTTP surface for health and metrics.
type Server struct {
	http *http.Server
}

// Start begins listening. ready may be nil (then /readyz always succeeds).
func Start(addr string, ready ReadyFunc) *Server {
	if addr == "" {
		addr = ":9090"
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if ready != nil {
			ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			defer cancel()
			if err := ready(ctx); err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})
	mux.Handle("/metrics", promhttp.Handler())

	s := &Server{http: &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}}
	go func() {
		slog.Info("http listen", "addr", addr)
		if err := s.http.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http server", "err", err)
		}
	}()
	return s
}

// Shutdown stops the HTTP server.
func (s *Server) Shutdown(ctx context.Context) error {
	if s == nil || s.http == nil {
		return nil
	}
	return s.http.Shutdown(ctx)
}
