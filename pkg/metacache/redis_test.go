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
	if err := c.MSet(ctx, map[string][]byte{"a": []byte("1"), "b": []byte("2")}, time.Minute); err != nil {
		t.Fatal(err)
	}
	gotMap, err := c.MGet(ctx, []string{"a", "b", "missing"})
	if err != nil || string(gotMap["a"]) != "1" || string(gotMap["b"]) != "2" {
		t.Fatalf("mget %v err=%v", gotMap, err)
	}
	if _, ok := gotMap["missing"]; ok {
		t.Fatal("missing key should be absent")
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

func TestRedisURLParse(t *testing.T) {
	mr := miniredis.RunT(t)
	c, err := NewRedis(Options{URL: "redis://" + mr.Addr(), TTL: time.Minute, Prefix: "spider:"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()
	if err := c.Set(ctx, "k", []byte("v"), time.Minute); err != nil {
		t.Fatal(err)
	}
	got, ok, err := c.Get(ctx, "k")
	if err != nil || !ok || string(got) != "v" {
		t.Fatalf("url get ok=%v err=%v val=%s", ok, err, got)
	}
}
