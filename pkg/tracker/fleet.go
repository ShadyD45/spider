package tracker

import (
	"sync"
	"time"

	"spider/pkg/metrics"
)

var fleetThresholds = []struct {
	label string
	pct   float64
}{
	{"first", 0},
	{"50", 50},
	{"90", 90},
	{"99", 99},
	{"100", 100},
}

type fleetState struct {
	publishedAt time.Time
	expected    int
	ready       map[string]struct{}
	recorded    map[string]bool
}

// FleetWatch records fleet-readiness milestones for published artifacts.
type FleetWatch struct {
	mu        sync.Mutex
	artifacts map[string]*fleetState
}

func NewFleetWatch() *FleetWatch {
	return &FleetWatch{artifacts: make(map[string]*fleetState)}
}

func (f *FleetWatch) BeginDistribution(artifactID string, expectedNodes int) {
	if f == nil || artifactID == "" {
		return
	}
	if expectedNodes < 1 {
		expectedNodes = 1
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.artifacts[artifactID] = &fleetState{
		publishedAt: time.Now(),
		expected:    expectedNodes,
		ready:       make(map[string]struct{}),
		recorded:    make(map[string]bool),
	}
	metrics.FleetReadyNodes.Set(0)
	for _, th := range fleetThresholds {
		metrics.FleetTimeToReadySeconds.WithLabelValues(th.label).Set(0)
	}
}

func (f *FleetWatch) NodeReady(artifactID, nodeID string) {
	if f == nil || artifactID == "" || nodeID == "" {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	st, ok := f.artifacts[artifactID]
	if !ok {
		return
	}
	if _, exists := st.ready[nodeID]; exists {
		return
	}
	st.ready[nodeID] = struct{}{}
	readyCount := len(st.ready)
	metrics.FleetReadyNodes.Set(float64(readyCount))

	pct := float64(readyCount) / float64(st.expected) * 100
	for _, th := range fleetThresholds {
		if st.recorded[th.label] {
			continue
		}
		thresholdMet := th.pct == 0 && readyCount >= 1
		if th.pct > 0 {
			thresholdMet = pct >= th.pct
		}
		if !thresholdMet {
			continue
		}
		st.recorded[th.label] = true
		elapsed := time.Since(st.publishedAt).Seconds()
		if elapsed <= 0 {
			elapsed = 1e-6
		}
		metrics.FleetTimeToReadySeconds.WithLabelValues(th.label).Set(elapsed)
	}
}
