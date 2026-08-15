package metacache

import (
	"context"
	"strings"
	"sync"
	"time"
)

type memEntry struct {
	value     []byte
	expiresAt time.Time
}

// Memory is a process-local LRU-ish map with TTL (no strict LRU cap; bounded by key set).
type Memory struct {
	mu      sync.RWMutex
	items      map[string]memEntry
	defaultTTL time.Duration
}

// NewMemory creates an in-process metadata cache.
func NewMemory(ttl time.Duration) *Memory {
	if ttl <= 0 {
		ttl = 10 * time.Second
	}
	m := &Memory{items: make(map[string]memEntry), defaultTTL: ttl}
	go m.janitor()
	return m
}

func (m *Memory) janitor() {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for range t.C {
		now := time.Now()
		m.mu.Lock()
		for k, e := range m.items {
			if !e.expiresAt.IsZero() && now.After(e.expiresAt) {
				delete(m.items, k)
			}
		}
		m.mu.Unlock()
	}
}

func (m *Memory) Get(_ context.Context, key string) ([]byte, bool, error) {
	m.mu.RLock()
	e, ok := m.items[key]
	m.mu.RUnlock()
	if !ok {
		return nil, false, nil
	}
	if !e.expiresAt.IsZero() && time.Now().After(e.expiresAt) {
		m.mu.Lock()
		delete(m.items, key)
		m.mu.Unlock()
		return nil, false, nil
	}
	out := make([]byte, len(e.value))
	copy(out, e.value)
	return out, true, nil
}

func (m *Memory) Set(_ context.Context, key string, value []byte, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = m.defaultTTL
	}
	cp := make([]byte, len(value))
	copy(cp, value)
	m.mu.Lock()
	m.items[key] = memEntry{value: cp, expiresAt: time.Now().Add(ttl)}
	m.mu.Unlock()
	return nil
}

func (m *Memory) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	delete(m.items, key)
	m.mu.Unlock()
	return nil
}

func (m *Memory) DeletePrefix(_ context.Context, prefix string) error {
	m.mu.Lock()
	for k := range m.items {
		if strings.HasPrefix(k, prefix) {
			delete(m.items, k)
		}
	}
	m.mu.Unlock()
	return nil
}

func (m *Memory) Close() error { return nil }
