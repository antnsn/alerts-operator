package metrics

import (
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestObserve(t *testing.T) {
	Observe("t1", TargetMimirRules, nil)
	Observe("t1", TargetMimirRules, nil)
	Observe("t1", TargetMimirRules, errors.New("boom"))
	if got := testutil.ToFloat64(SyncTotal.WithLabelValues("t1", TargetMimirRules, "ok")); got != 2 {
		t.Fatalf("ok=%v", got)
	}
	if got := testutil.ToFloat64(SyncTotal.WithLabelValues("t1", TargetMimirRules, "error")); got != 1 {
		t.Fatalf("error=%v", got)
	}
	if got := testutil.ToFloat64(LastSyncTimestamp.WithLabelValues("t1", TargetMimirRules)); got <= 0 {
		t.Fatalf("timestamp not set: %v", got)
	}
	if got := testutil.ToFloat64(LastSyncTimestamp.WithLabelValues("t2", TargetLokiRules)); got != 0 {
		t.Fatalf("untouched gauge should be 0: %v", got)
	}
}

// TestDeleteTenant covers a Codex P2 finding: tenant is not actually a bounded label value over the
// operator's lifetime -- CounterVec/GaugeVec retain every label tuple ever created, so a cluster that
// creates and deletes uniquely-named Tenants over time would otherwise accumulate stale series
// forever. DeleteTenant is called from the Tenant finalizer once cleanup succeeds.
func TestDeleteTenant(t *testing.T) {
	Observe("del-t", TargetMimirRules, nil)
	if got := testutil.ToFloat64(SyncTotal.WithLabelValues("del-t", TargetMimirRules, "ok")); got != 1 {
		t.Fatalf("precondition: got %v", got)
	}
	DeleteTenant("del-t")
	// WithLabelValues lazily recreates a deleted series at zero, so re-reading it after DeleteTenant
	// distinguishes "deleted" (0) from "never deleted" (still 1).
	if got := testutil.ToFloat64(SyncTotal.WithLabelValues("del-t", TargetMimirRules, "ok")); got != 0 {
		t.Fatalf("series not deleted: got %v", got)
	}
}
