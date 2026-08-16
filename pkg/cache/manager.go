package cache

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	v1 "spider/api/v1"
)

type chunkMeta struct {
	Hash       string `json:"hash"`
	Size       int64  `json:"size"`
	LastAccess int64  `json:"lastAccess"`
	RefCount   int    `json:"refCount"`
}

type indexFile struct {
	Chunks map[string]chunkMeta `json:"chunks"`
	Pins   map[string][]string  `json:"pins"` // artifactID -> chunk hashes
}

const defaultIndexFlushInterval = time.Second

// QuotaManager enforces disk quotas with refcounted LRU eviction and artifact pins.
type QuotaManager struct {
	cache         *ChunkStore
	maxBytes      int64
	lowWatermark  float64
	highWatermark float64
	mu            sync.Mutex
	idx           indexFile
	path          string
	dirty         bool
	done          chan struct{}
	closeOnce     sync.Once
}

func NewQuotaManager(c *ChunkStore, maxBytes int64, low, high float64) (*QuotaManager, error) {
	if c == nil {
		return nil, os.ErrInvalid
	}
	if maxBytes <= 0 {
		maxBytes = 500 * 1024 * 1024 * 1024
	}
	if low <= 0 {
		low = 0.80
	}
	if high <= 0 {
		high = 0.90
	}
	m := &QuotaManager{
		cache:         c,
		maxBytes:      maxBytes,
		lowWatermark:  low,
		highWatermark: high,
		path:          filepath.Join(c.rootDir, "index.json"),
		idx: indexFile{
			Chunks: make(map[string]chunkMeta),
			Pins:   make(map[string][]string),
		},
		done: make(chan struct{}),
	}
	_ = m.load()
	_ = m.reconcileFromDisk()
	return m, nil
}

// Manager is a compatibility alias for QuotaManager.
type Manager = QuotaManager

// NewManager is a compatibility alias for NewQuotaManager.
func NewManager(c *ChunkStore, maxBytes int64, low, high float64) (*QuotaManager, error) {
	return NewQuotaManager(c, maxBytes, low, high)
}

func (m *QuotaManager) load() error {
	data, err := os.ReadFile(m.path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, &m.idx)
}

func (m *QuotaManager) save() error {
	data, err := json.MarshalIndent(m.idx, "", "  ")
	if err != nil {
		return err
	}
	tmp := m.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	if err := os.Rename(tmp, m.path); err != nil {
		return err
	}
	m.dirty = false
	return nil
}

func (m *QuotaManager) reconcileFromDisk() error {
	hashes, err := m.cache.ListChunks()
	if err != nil {
		return err
	}
	now := time.Now().Unix()
	for _, h := range hashes {
		if _, ok := m.idx.Chunks[h]; ok {
			continue
		}
		p, err := m.cache.GetChunkPath(h)
		if err != nil {
			continue
		}
		st, err := os.Stat(p)
		if err != nil {
			continue
		}
		m.idx.Chunks[h] = chunkMeta{Hash: h, Size: st.Size(), LastAccess: now, RefCount: 0}
	}
	return m.save()
}

// Start begins a background flusher for Touch() mutations.
func (m *QuotaManager) Start() {
	go m.flushLoop(defaultIndexFlushInterval)
}

func (m *QuotaManager) flushLoop(interval time.Duration) {
	if interval <= 0 {
		interval = defaultIndexFlushInterval
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			_ = m.Flush()
		case <-m.done:
			return
		}
	}
}

// Flush writes a dirty index to disk.
func (m *QuotaManager) Flush() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.dirty {
		return nil
	}
	return m.save()
}

// Close stops the background flusher and persists the index.
func (m *QuotaManager) Close() error {
	m.closeOnce.Do(func() {
		close(m.done)
	})
	return m.Flush()
}

func (m *QuotaManager) Touch(hash string, size int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	meta := m.idx.Chunks[hash]
	meta.Hash = hash
	if size > 0 {
		meta.Size = size
	}
	meta.LastAccess = time.Now().Unix()
	m.idx.Chunks[hash] = meta
	m.dirty = true
}

func (m *QuotaManager) fillSizeLocked(hash string, meta *chunkMeta) {
	if meta.Size > 0 {
		return
	}
	if sz, ok := m.cache.CommittedChunkSize(hash); ok {
		meta.Size = sz
	}
}

func (m *QuotaManager) Pin(manifest *v1.ArtifactManifest) error {
	if manifest == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, already := m.idx.Pins[manifest.ArtifactID]; already {
		return m.save()
	}
	hashes := manifest.AllChunkHashes()
	m.idx.Pins[manifest.ArtifactID] = hashes
	for _, h := range hashes {
		meta := m.idx.Chunks[h]
		meta.Hash = h
		m.fillSizeLocked(h, &meta)
		meta.RefCount++
		m.idx.Chunks[h] = meta
	}
	return m.save()
}

func (m *QuotaManager) Unpin(artifactID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	hashes := m.idx.Pins[artifactID]
	delete(m.idx.Pins, artifactID)
	for _, h := range hashes {
		meta := m.idx.Chunks[h]
		if meta.RefCount > 0 {
			meta.RefCount--
		}
		m.idx.Chunks[h] = meta
	}
	return m.save()
}

func (m *QuotaManager) Pinned() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var ids []string
	for id := range m.idx.Pins {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (m *QuotaManager) RefCount(hash string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.idx.Chunks[hash].RefCount
}

func (m *QuotaManager) UsedBytes() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	var n int64
	for _, c := range m.idx.Chunks {
		n += c.Size
	}
	return n
}

func (m *QuotaManager) MaybeEvict() (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	used := int64(0)
	for _, c := range m.idx.Chunks {
		used += c.Size
	}
	if used < int64(float64(m.maxBytes)*m.highWatermark) {
		return 0, nil
	}
	target := int64(float64(m.maxBytes) * m.lowWatermark)
	type cand struct {
		h    string
		meta chunkMeta
	}
	var list []cand
	for h, meta := range m.idx.Chunks {
		if meta.RefCount > 0 {
			continue
		}
		list = append(list, cand{h, meta})
	}
	sort.Slice(list, func(i, j int) bool { return list[i].meta.LastAccess < list[j].meta.LastAccess })
	evicted := 0
	for _, c := range list {
		if used <= target {
			break
		}
		if err := m.cache.DeleteChunk(c.h); err != nil && !os.IsNotExist(err) {
			continue
		}
		used -= c.meta.Size
		delete(m.idx.Chunks, c.h)
		evicted++
	}
	_ = m.save()
	return evicted, nil
}
