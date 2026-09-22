// Package metrics exposes operator metrics on the controller-runtime registry.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Target values for the "target" label on SyncTotal and LastSyncTimestamp.
const (
	TargetAlertmanager = "alertmanager"
	TargetMimirRules   = "mimir_rules"
	TargetLokiRules    = "loki_rules"
)

// SyncTotal counts sync attempts per tenant and target, labeled ok/error. tenant and target are
// both bounded (Tenant CR names and the three Target constants above), never an error string or
// other unbounded value, to keep cardinality in check.
var SyncTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "alerts_operator_sync_total",
	Help: "Sync attempts per tenant and target.",
}, []string{"tenant", "target", "result"})

// LastSyncTimestamp records the Unix time of the last successful sync per tenant and target.
var LastSyncTimestamp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "alerts_operator_last_sync_timestamp_seconds",
	Help: "Unix time of the last successful sync per tenant and target.",
}, []string{"tenant", "target"})

func init() {
	ctrlmetrics.Registry.MustRegister(SyncTotal, LastSyncTimestamp)
}

// Observe records one sync attempt. A nil err is success.
func Observe(tenant, target string, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	SyncTotal.WithLabelValues(tenant, target, result).Inc()
	if err == nil {
		LastSyncTimestamp.WithLabelValues(tenant, target).SetToCurrentTime()
	}
}

// DeleteTenant removes every SyncTotal/LastSyncTimestamp series for tenant. Call it once the
// Tenant's finalizer has finished (see internal/controller/tenant_controller.go): without it, a
// cluster that creates and deletes Tenants with unique names over time would accumulate one stale
// series per name for the lifetime of the process, unbounded by anything the running cluster
// actually has today.
func DeleteTenant(tenant string) {
	SyncTotal.DeletePartialMatch(prometheus.Labels{"tenant": tenant})
	LastSyncTimestamp.DeletePartialMatch(prometheus.Labels{"tenant": tenant})
}
