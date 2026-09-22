package controller

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	observabilityv1alpha1 "github.com/antnsn/alerts-operator/api/v1alpha1"
	"github.com/antnsn/alerts-operator/internal/backend"
	"github.com/antnsn/alerts-operator/internal/backend/fake"
)

func TestTenantFinalizerCleansBackend(t *testing.T) {
	tn, srv := newFakeTenant(t, "fin-tenant", true, true)
	srv.SetRules("1", "other/x", []backend.RuleGroup{{Name: "keep", Rules: []backend.Rule{{Alert: "K", Expr: "up"}}}})
	keep := &observabilityv1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "fin-keep", Namespace: "default"},
		Spec: observabilityv1alpha1.ContactPointSpec{TenantRef: tn.Name, Webhook: []observabilityv1alpha1.WebhookConfig{{URL: "http://keep"}}}}
	pol := &observabilityv1alpha1.NotificationPolicy{ObjectMeta: metav1.ObjectMeta{Name: "fin-pol", Namespace: "default"},
		Spec: observabilityv1alpha1.NotificationPolicySpec{TenantRef: tn.Name, Route: observabilityv1alpha1.Route{Receiver: "fin-keep"}}}
	g := &observabilityv1alpha1.AlertRuleGroup{ObjectMeta: metav1.ObjectMeta{Name: "fin-g", Namespace: "default"},
		Spec: observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: tn.Name, Backend: "loki", Groups: ruleGroups(`{a="b"}`)}}
	for _, o := range []observabilityv1alpha1.Conditioned{keep, pol, g} {
		createAndCleanup(t, o)
	}
	waitCondition(t, tn, observabilityv1alpha1.ConditionReady, metav1.ConditionTrue, "")
	if srv.Alertmanager("1") == nil || len(srv.LokiRules("1")["alerts-operator/default/fin-g"]) != 1 {
		t.Fatal("precondition: backend populated")
	}

	// Backend down: deletion is blocked, finalizer stays, Ready=False/Deleting.
	srv.Fail(503)
	if err := testClient.Delete(testCtx, tn); err != nil {
		t.Fatal(err)
	}
	waitCondition(t, tn, observabilityv1alpha1.ConditionReady, metav1.ConditionFalse, observabilityv1alpha1.ReasonDeleting)
	if !controllerutil.ContainsFinalizer(tn, tenantFinalizer) {
		t.Fatal("finalizer removed while backend down")
	}

	// Backend back: AM config and owned namespaces deleted, foreign namespace kept, object gone.
	srv.Fail(0)
	waitFor(t, func() bool {
		err := testClient.Get(testCtx, clientKey(tn), &observabilityv1alpha1.Tenant{})
		return errors.IsNotFound(err)
	})
	time.Sleep(200 * time.Millisecond)
	if srv.Alertmanager("1") != nil {
		t.Fatal("alertmanager config not deleted")
	}
	if _, ok := srv.LokiRules("1")["alerts-operator/default/fin-g"]; ok {
		t.Fatal("owned loki namespace not deleted")
	}
	if _, ok := srv.Rules("1")["other/x"]; !ok {
		t.Fatal("foreign mimir namespace deleted")
	}
}

// TestTenantFinalizerSkipsSharedAlertmanagerAcrossPrefixes covers a Codex P1 finding on this task:
// conflictingTenant's ownership key includes the effective rules namespace prefix, which is correct
// for rule namespaces but wrong for the Alertmanager document -- POST/GET/DELETE /api/v1/alerts is
// scoped only by X-Scope-OrgID (tenantId) and backend address, never by prefix. Two Tenants sharing
// tenantId+address but using different prefixes therefore share one live Alertmanager config even
// though conflictingTenant(mimir) reports no conflict between them (their rule namespaces genuinely
// don't collide). Deleting one must not wipe the other's shared, live config.
func TestTenantFinalizerSkipsSharedAlertmanagerAcrossPrefixes(t *testing.T) {
	srv := fake.New()
	defer srv.Close()
	srv.SetAlertmanager("1", &backend.AlertmanagerConfig{Config: "route:\n  receiver: x\nreceivers:\n- name: x\n"})

	// Deleted inline at the end of the test body, not via t.Cleanup: t.Cleanup callbacks run after
	// the test function returns, which is *after* the defer above has already closed srv (Go runs a
	// function's own defers before its registered Cleanups) -- survivor's own finalizer would then
	// try to reach an already-closed backend and hang. This is the same ordering hazard fixed in
	// tenant_api_test.go's TestTenantIDImmutable for the identical reason.
	survivor := &observabilityv1alpha1.Tenant{ObjectMeta: metav1.ObjectMeta{Name: "fin-am-survivor"},
		Spec: observabilityv1alpha1.TenantSpec{TenantID: "1", Mimir: &observabilityv1alpha1.BackendSpec{Address: srv.URL}, RulesNamespacePrefix: "prefix-b"}}
	if err := testClient.Create(testCtx, survivor); err != nil {
		t.Fatal(err)
	}

	dying := &observabilityv1alpha1.Tenant{ObjectMeta: metav1.ObjectMeta{Name: "fin-am-dying"},
		Spec: observabilityv1alpha1.TenantSpec{TenantID: "1", Mimir: &observabilityv1alpha1.BackendSpec{Address: srv.URL}, RulesNamespacePrefix: "prefix-a"}}
	if err := testClient.Create(testCtx, dying); err != nil {
		t.Fatal(err)
	}
	// Wait for the finalizer to actually land before deleting, so Delete can't race the very first
	// reconcile (AddFinalizer) and remove the object before finalize ever runs.
	waitFor(t, func() bool {
		_ = testClient.Get(testCtx, clientKey(dying), dying)
		return controllerutil.ContainsFinalizer(dying, tenantFinalizer)
	})

	if err := testClient.Delete(testCtx, dying); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		return errors.IsNotFound(testClient.Get(testCtx, clientKey(dying), &observabilityv1alpha1.Tenant{}))
	})
	if srv.Alertmanager("1") == nil {
		t.Fatal("shared alertmanager config wrongly deleted by the colliding Tenant's finalizer")
	}

	if err := testClient.Delete(testCtx, survivor); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		return errors.IsNotFound(testClient.Get(testCtx, clientKey(survivor), &observabilityv1alpha1.Tenant{}))
	})
}

// TestFinalizeOwnerDeterministicTiebreak covers the other Codex P1 finding: when two colliding
// Tenants are deleted concurrently, each finalizer must not simply defer to "the other conflicting
// Tenant" -- if that Tenant is also terminating, it will never survive to perform the cleanup either,
// and the backend state leaks forever. finalizeOwner is the pure decision function; this test drives
// it directly rather than through envtest, since reproducing the concurrent-deletion race reliably
// through the real reconcile loop would be flaky on any timing.
func TestFinalizeOwnerDeterministicTiebreak(t *testing.T) {
	now := metav1.Now()
	self := &observabilityv1alpha1.Tenant{ObjectMeta: metav1.ObjectMeta{Name: "b-self"}}

	if got := finalizeOwner(self, nil); got != "" {
		t.Fatalf("no claimants: got %q, want self to clean up", got)
	}

	live := observabilityv1alpha1.Tenant{ObjectMeta: metav1.ObjectMeta{Name: "z-live"}}
	if got := finalizeOwner(self, []observabilityv1alpha1.Tenant{live}); got != "z-live" {
		t.Fatalf("live claimant: got %q, want defer to z-live", got)
	}

	// Every claimant is also terminating, and self has the lowest name: self must clean up rather
	// than defer to a peer that will never survive to do it.
	termHigh := observabilityv1alpha1.Tenant{ObjectMeta: metav1.ObjectMeta{Name: "c-terminating", DeletionTimestamp: &now}}
	if got := finalizeOwner(self, []observabilityv1alpha1.Tenant{termHigh}); got != "" {
		t.Fatalf("self is lowest of an all-terminating set: got %q, want self to clean up", got)
	}

	// Every claimant is terminating and self is NOT the lowest: defer to whichever terminating
	// Tenant is -- exactly one member of the set must end up proceeding.
	termLow := observabilityv1alpha1.Tenant{ObjectMeta: metav1.ObjectMeta{Name: "a-terminating", DeletionTimestamp: &now}}
	if got := finalizeOwner(self, []observabilityv1alpha1.Tenant{termHigh, termLow}); got != "a-terminating" {
		t.Fatalf("self is not lowest of an all-terminating set: got %q, want defer to a-terminating", got)
	}
}

// TestTenantReconcilerAPIReaderFallsBackToClient covers apiReader's default for a TenantReconciler
// built directly (as most unit tests do), without going through SetupWithManager.
func TestTenantReconcilerAPIReaderFallsBackToClient(t *testing.T) {
	r := &TenantReconciler{Client: testClient}
	if got := r.apiReader(); got != testClient {
		t.Fatalf("expected fallback to Client when APIReader is unset, got %v", got)
	}
}

// TestTenantReconcilerSetupWithManagerSetsUncachedAPIReader covers a Codex P1 finding on this task:
// claimants (tenant_finalizer.go) must read through an uncached API reader, not the manager's cache,
// because its result feeds an irreversible RemoveFinalizer decision that never gets retried -- a
// stale cache read could still show a colliding Tenant that already finished deleting. This proves
// SetupWithManager actually wires APIReader to something other than the cache-backed Client; a
// missing or wrong wiring here would otherwise fail silently (apiReader's fallback means nothing else
// would notice).
func TestTenantReconcilerSetupWithManagerSetsUncachedAPIReader(t *testing.T) {
	// SkipNameValidation: the shared suite manager (suite_test.go) already registered a controller
	// named "tenant" for the life of this test binary (controller-runtime validates names against a
	// process-global registry, not a per-manager one), so a second SetupWithManager call on any
	// manager instance would otherwise fail here with "controller with name tenant already exists".
	skip := true
	mgr, err := ctrl.NewManager(testCfg, ctrl.Options{
		Scheme: scheme.Scheme, Metrics: metricsserver.Options{BindAddress: "0"},
		Controller: config.Controller{SkipNameValidation: &skip},
	})
	if err != nil {
		t.Fatal(err)
	}
	r := &TenantReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}
	if err := r.SetupWithManager(mgr); err != nil {
		t.Fatal(err)
	}
	if r.APIReader == nil {
		t.Fatal("SetupWithManager must set APIReader to an uncached reader")
	}
}
