package advertise

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"spider/api/v1/proto"
	"spider/pkg/config"
)

func TestAdvertiserRetriesOnFailure(t *testing.T) {
	var calls atomic.Int32
	client := &failingReporter{failUntil: 2, calls: &calls}
	ad := New(client, "node-a", config.AdvertisementConfig{
		BatchSize:    1,
		Interval:     20 * time.Millisecond,
		MaxRetries:   5,
		RetryBackoff: 10 * time.Millisecond,
	})
	ad.Enqueue("sha256:retry")
	time.Sleep(300 * time.Millisecond)
	ad.Stop()
	if calls.Load() < 2 {
		t.Fatalf("expected retries, got %d calls", calls.Load())
	}
}

func TestAdvertiserFlushesBeforeStop(t *testing.T) {
	var mu sync.Mutex
	var reports [][]string
	client := &reportSpy{onReport: func(hashes []string) {
		mu.Lock()
		reports = append(reports, append([]string(nil), hashes...))
		mu.Unlock()
	}}
	ad := New(client, "node-a", config.AdvertisementConfig{
		BatchSize:    4,
		Interval:     50 * time.Millisecond,
		MaxRetries:   5,
		RetryBackoff: 10 * time.Millisecond,
	})
	ad.Enqueue("sha256:aa")
	ad.Enqueue("sha256:bb")
	ad.Stop()

	mu.Lock()
	defer mu.Unlock()
	if len(reports) == 0 {
		t.Fatal("expected at least one ReportChunks flush before stop")
	}
}

type failingReporter struct {
	failUntil int32
	calls     *atomic.Int32
}

func (f *failingReporter) ReportChunks(_ context.Context, _ *proto.ReportChunksRequest, _ ...grpc.CallOption) (*proto.ReportChunksResponse, error) {
	n := f.calls.Add(1)
	if n <= f.failUntil {
		return nil, fmt.Errorf("tracker unavailable")
	}
	return &proto.ReportChunksResponse{ChunksRecorded: 1}, nil
}

type reportSpy struct {
	onReport func([]string)
}

func (r *reportSpy) ReportChunks(_ context.Context, req *proto.ReportChunksRequest, _ ...grpc.CallOption) (*proto.ReportChunksResponse, error) {
	if r.onReport != nil {
		r.onReport(req.GetChunkHashes())
	}
	return &proto.ReportChunksResponse{ChunksRecorded: int64(len(req.GetChunkHashes()))}, nil
}
