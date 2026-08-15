package metacache

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func TestRedisRoundTripAndInvalidation(t *testing.T) {
	mr := miniredis.RunT(t)
	c, err := NewRedis(Options{Addr: mr.Addr(), TTL: time.Minute, Prefix: "spider:"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })

	ctx := context.Background()
	if err := c.Set(ctx, "peer:n1", []byte(`{"nodeId":"n1"}`), time.Minute); err != nil {
		t.Fatal(err)
	}
	got, ok, err := c.Get(ctx, "peer:n1")
	if err != nil || !ok || string(got) != `{"nodeId":"n1"}` {
		t.Fatalf("get: ok=%v err=%v val=%s", ok, err, got)
	}
	if err := c.Set(ctx, "seeds:a", []byte("x"), time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := c.DeletePrefix(ctx, "seeds:"); err != nil {
		t.Fatal(err)
	}
	_, ok, err = c.Get(ctx, "seeds:a")
	if err != nil || ok {
		t.Fatalf("expected prefix delete, ok=%v err=%v", ok, err)
	}
}

func TestMemoryTTLExpiry(t *testing.T) {
	c := NewMemory(20 * time.Millisecond)
	ctx := context.Background()
	if err := c.Set(ctx, "k", []byte("v"), 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	_, ok, err := c.Get(ctx, "k")
	if err != nil || ok {
		t.Fatalf("expected ttl miss ok=%v err=%v", ok, err)
	}
}
