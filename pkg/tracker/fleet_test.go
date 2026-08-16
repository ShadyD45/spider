package tracker

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"spider/pkg/metrics"
)

func TestFleetWatchRecordsMilestones(t *testing.T) {
	fw := NewFleetWatch()
	fw.BeginDistribution("art-1", 4)
	fw.NodeReady("art-1", "a")
	fw.NodeReady("art-1", "b")
	fw.NodeReady("art-1", "c")
	fw.NodeReady("art-1", "d")

	if v := testutil.ToFloat64(metrics.FleetReadyNodes); v != 4 {
		t.Fatalf("expected 4 ready nodes, got %v", v)
	}
	if v := testutil.ToFloat64(metrics.FleetTimeToReadySeconds.WithLabelValues("first")); v <= 0 {
		t.Fatalf("expected first-node milestone, got %v", v)
	}
	if v := testutil.ToFloat64(metrics.FleetTimeToReadySeconds.WithLabelValues("100")); v <= 0 {
		t.Fatalf("expected 100%% milestone, got %v", v)
	}
}

func TestFleetWatchIgnoresDuplicateReady(t *testing.T) {
	fw := NewFleetWatch()
	fw.BeginDistribution("art-1", 2)
	fw.NodeReady("art-1", "a")
	fw.NodeReady("art-1", "a")
	fw.NodeReady("art-1", "b")
	if v := testutil.ToFloat64(metrics.FleetReadyNodes); v != 2 {
		t.Fatalf("expected 2 ready nodes, got %v", v)
	}
}
