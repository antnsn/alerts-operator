package controller

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
	// A Loki namespace under a *lookalike* prefix: same leading characters, but the next character
	// is not the separator. deleteOwnedNamespaces matches on prefix+separator, so this is a
	// different owner's state and must survive. Reverting that to a bare HasPrefix on the unslashed
	// prefix deletes it.
	srv.SetLokiRules("1", "alerts-operator-other_default_x", []backend.RuleGroup{{Name: "keep", Rules: []backend.Rule{{Alert: "KO", Expr: `{a="b"}`}}}})
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
	if srv.Alertmanager("1") == nil || len(srv.LokiRules("1")["alerts-operator_default_fin-g"]) != 1 {
		t.Fatalf("precondition: backend populated, got %+v", srv.LokiRules("1"))
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
	if _, ok := srv.LokiRules("1")["alerts-operator_default_fin-g"]; ok {
		t.Fatal("owned loki namespace not deleted")
	}
	if _, ok := srv.Rules("1")["other/x"]; !ok {
		t.Fatal("foreign mimir namespace deleted")
	}
	if _, ok := srv.LokiRules("1")["alerts-operator-other_default_x"]; !ok {
		t.Fatal("a loki namespace under a lookalike prefix was deleted: ownership must match on a segment boundary")
	}
}

// writingContactPointAndPolicy returns a ContactPoint+NotificationPolicy pair that makes tenantRef's
// Tenant actually compile and push an Alertmanager document (i.e. get a non-empty
// status.alertmanagerConfigHash), for tests that need a real *writing* Tenant rather than merely a
// Tenant with spec.mimir set. Names are suffixed so multiple pairs can coexist in one test.
func writingContactPointAndPolicy(tenantRef, suffix string) (*observabilityv1alpha1.ContactPoint, *observabilityv1alpha1.NotificationPolicy) {
	cp := &observabilityv1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "fin-cp-" + suffix, Namespace: "default"},
		Spec: observabilityv1alpha1.ContactPointSpec{TenantRef: tenantRef, Webhook: []observabilityv1alpha1.WebhookConfig{{URL: "http://" + suffix}}}}
	pol := &observabilityv1alpha1.NotificationPolicy{ObjectMeta: metav1.ObjectMeta{Name: "fin-pol-" + suffix, Namespace: "default"},
		Spec: observabilityv1alpha1.NotificationPolicySpec{TenantRef: tenantRef, Route: observabilityv1alpha1.Route{Receiver: "fin-cp-" + suffix}}}
	return cp, pol
}

// TestTenantFinalizerSkipsSharedAlertmanagerAcrossPrefixes covers a Codex P1 finding on this task:
// conflictingTenant's ownership key includes the effective rules namespace prefix, which is correct
// for rule namespaces but wrong for the Alertmanager document -- POST/GET/DELETE /api/v1/alerts is
// scoped only by X-Scope-OrgID (tenantId) and backend address, never by prefix. Two Tenants sharing
// tenantId+address but using different prefixes therefore share one live Alertmanager config even
// though conflictingTenant(mimir, withPrefix=true) reports no conflict between them (their rule
// namespaces genuinely don't collide). Deleting one must not wipe the other's shared, live config.
//
// Both Tenants must carry a confirmed status.alertmanagerConfigHash at this address or the test
// passes for the wrong reason: dying's own P2-1 gate (hash != "", task-20 review round 1) would
// skip the delete attempt before ever reaching the prefix-independent ownership check, and
// survivor would be filtered out of writingClaimants so there would be nobody to defer to.
//
// Since the steady-state ownership guard landed (tenant_alertmanager.go), a second Tenant sharing
// the key never writes the document and so never earns a hash of its own -- which leaves exactly
// one way for two hashes to coexist, and it is the one that matters most: a cluster upgraded from
// a release without the guard, where both Tenants had been overwriting each other every resync and
// both recorded a hash for it. dying's status is therefore seeded directly here rather than
// produced by a live write, because that is the state the upgrade actually leaves behind, and it
// is the state in which deleting one Tenant would destroy the other's live routing.
func TestTenantFinalizerSkipsSharedAlertmanagerAcrossPrefixes(t *testing.T) {
	srv := fake.New()
	defer srv.Close()

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
	survivorCP, survivorPol := writingContactPointAndPolicy(survivor.Name, "survivor")
	if err := testClient.Create(testCtx, survivorCP); err != nil {
		t.Fatal(err)
	}
	if err := testClient.Create(testCtx, survivorPol); err != nil {
		t.Fatal(err)
	}
	// Created and synced while it is still the sole claimant, so this hash is a genuine one.
	waitCondition(t, survivor, observabilityv1alpha1.ConditionAlertmanagerSynced, metav1.ConditionTrue, observabilityv1alpha1.ReasonSynced)

	dying := &observabilityv1alpha1.Tenant{ObjectMeta: metav1.ObjectMeta{Name: "fin-am-dying"},
		Spec: observabilityv1alpha1.TenantSpec{TenantID: "1", Mimir: &observabilityv1alpha1.BackendSpec{Address: srv.URL}, RulesNamespacePrefix: "prefix-a"}}
	if err := testClient.Create(testCtx, dying); err != nil {
		t.Fatal(err)
	}
	dyingCP, dyingPol := writingContactPointAndPolicy(dying.Name, "dying")
	if err := testClient.Create(testCtx, dyingCP); err != nil {
		t.Fatal(err)
	}
	if err := testClient.Create(testCtx, dyingPol); err != nil {
		t.Fatal(err)
	}
	// Both Tenants now see each other: the guard freezes the document rather than letting them
	// take turns overwriting it, which is the steady-state half of this same finding.
	waitCondition(t, dying, observabilityv1alpha1.ConditionAlertmanagerSynced, metav1.ConditionFalse, observabilityv1alpha1.ReasonConflict)
	seedAlertmanagerConfigHash(t, dying, srv.URL)

	if err := testClient.Delete(testCtx, dying); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		return errors.IsNotFound(testClient.Get(testCtx, clientKey(dying), &observabilityv1alpha1.Tenant{}))
	})
	if srv.Alertmanager("1") == nil {
		t.Fatal("shared alertmanager config wrongly deleted by the colliding Tenant's finalizer")
	}

	for _, o := range []client.Object{dyingCP, dyingPol, survivorCP, survivorPol} {
		_ = testClient.Delete(testCtx, o)
	}
	if err := testClient.Delete(testCtx, survivor); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		return errors.IsNotFound(testClient.Get(testCtx, clientKey(survivor), &observabilityv1alpha1.Tenant{}))
	})
}

// seedAlertmanagerConfigHash writes the two status fields the finalizer reads as "this Tenant wrote
// the live Alertmanager document at this address". Retried against the live Tenant reconciler's own
// status patches: the reconciler never clears either field (syncAlertmanager only ever sets them,
// and its status patch is a MergeFrom diff), so a write that lands survives -- but it can still lose
// an optimistic-lock race on the way in.
func seedAlertmanagerConfigHash(t *testing.T, tn *observabilityv1alpha1.Tenant, addr string) {
	t.Helper()
	waitFor(t, func() bool {
		var cur observabilityv1alpha1.Tenant
		if err := testClient.Get(testCtx, clientKey(tn), &cur); err != nil {
			return false
		}
		cur.Status.AlertmanagerConfigHash = "sha256:seeded-by-a-pre-guard-release"
		cur.Status.AlertmanagerConfigAddress = addr
		return testClient.Status().Update(testCtx, &cur) == nil
	})
}

// TestTenantFinalizerLeavesNeverWrittenAlertmanagerConfig covers Codex P2-1 (task-20 review round 1):
// finalize used to issue DELETE /api/v1/alerts whenever spec.mimir was set, with no check that this
// operator ever wrote that document -- so a rules-only Tenant (no accepted NotificationPolicy, which
// is precisely when syncAlertmanager never touches the backend at all, see tenant_alertmanager.go's
// ch.Policy==nil early return) would destroy a hand-written or externally-managed Alertmanager
// config on delete, despite the steady-state sync having correctly left it alone the whole time it
// was reconciling. Guard: only delete when tenant.Status.AlertmanagerConfigHash != "".
func TestTenantFinalizerLeavesNeverWrittenAlertmanagerConfig(t *testing.T) {
	srv := fake.New()
	defer srv.Close()
	const handWritten = "route:\n  receiver: hand-written\nreceivers:\n- name: hand-written\n"
	srv.SetAlertmanager("1", &backend.AlertmanagerConfig{Config: handWritten})

	tn := &observabilityv1alpha1.Tenant{ObjectMeta: metav1.ObjectMeta{Name: "fin-am-neverwrote"},
		Spec: observabilityv1alpha1.TenantSpec{TenantID: "1", Mimir: &observabilityv1alpha1.BackendSpec{Address: srv.URL}, RulesNamespacePrefix: "fin-neverwrote"}}
	if err := testClient.Create(testCtx, tn); err != nil {
		t.Fatal(err)
	}
	// No ContactPoint/NotificationPolicy is ever created for this Tenant: it manages rules only.
	waitCondition(t, tn, observabilityv1alpha1.ConditionAlertmanagerSynced, metav1.ConditionFalse, observabilityv1alpha1.ReasonNoNotificationPolicy)
	if tn.Status.AlertmanagerConfigHash != "" {
		t.Fatal("precondition: tenant must never have written an alertmanager document")
	}
	if got := srv.Alertmanager("1"); got == nil || got.Config != handWritten {
		t.Fatalf("precondition: hand-written config not intact before delete: %+v", got)
	}

	if err := testClient.Delete(testCtx, tn); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		return errors.IsNotFound(testClient.Get(testCtx, clientKey(tn), &observabilityv1alpha1.Tenant{}))
	})
	if got := srv.Alertmanager("1"); got == nil || got.Config != handWritten {
		t.Fatalf("finalizer deleted an alertmanager config this operator never wrote: %+v", got)
	}
}

// TestTenantFinalizerDeletesOwnAlertmanagerDespiteNonWritingClaimant covers Codex P2-2 (task-20
// review round 1): finalizeOwner used to defer AM cleanup to *any* live claimant sharing
// tenantId+address, including one that never writes the Alertmanager document at all (no accepted
// NotificationPolicy). That strands the writer's config forever: the non-writing claimant will never
// overwrite or delete it, and the writer's own CR is gone. Guard: only a claimant with a non-empty
// status.alertmanagerConfigHash counts as a valid claimant to defer to; a writer with no valid
// claimant must delete its own document rather than leave it stranded.
func TestTenantFinalizerDeletesOwnAlertmanagerDespiteNonWritingClaimant(t *testing.T) {
	srv := fake.New()
	defer srv.Close()

	writer := &observabilityv1alpha1.Tenant{ObjectMeta: metav1.ObjectMeta{Name: "fin-am-writer"},
		Spec: observabilityv1alpha1.TenantSpec{TenantID: "1", Mimir: &observabilityv1alpha1.BackendSpec{Address: srv.URL}, RulesNamespacePrefix: "fin-writer-prefix"}}
	if err := testClient.Create(testCtx, writer); err != nil {
		t.Fatal(err)
	}
	cp, pol := writingContactPointAndPolicy(writer.Name, "writer")
	if err := testClient.Create(testCtx, cp); err != nil {
		t.Fatal(err)
	}
	if err := testClient.Create(testCtx, pol); err != nil {
		t.Fatal(err)
	}
	waitCondition(t, writer, observabilityv1alpha1.ConditionAlertmanagerSynced, metav1.ConditionTrue, observabilityv1alpha1.ReasonSynced)
	if writer.Status.AlertmanagerConfigHash == "" || srv.Alertmanager("1") == nil {
		t.Fatal("precondition: writer never wrote an alertmanager document")
	}

	nonwriter := &observabilityv1alpha1.Tenant{ObjectMeta: metav1.ObjectMeta{Name: "fin-am-nonwriter"},
		Spec: observabilityv1alpha1.TenantSpec{TenantID: "1", Mimir: &observabilityv1alpha1.BackendSpec{Address: srv.URL}, RulesNamespacePrefix: "fin-nonwriter-prefix"}}
	if err := testClient.Create(testCtx, nonwriter); err != nil {
		t.Fatal(err)
	}
	// No ContactPoint/NotificationPolicy for nonwriter: it never writes the Alertmanager document.
	waitCondition(t, nonwriter, observabilityv1alpha1.ConditionAlertmanagerSynced, metav1.ConditionFalse, observabilityv1alpha1.ReasonNoNotificationPolicy)
	if nonwriter.Status.AlertmanagerConfigHash != "" {
		t.Fatal("precondition: nonwriter must never have written an alertmanager document")
	}

	if err := testClient.Delete(testCtx, writer); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		return errors.IsNotFound(testClient.Get(testCtx, clientKey(writer), &observabilityv1alpha1.Tenant{}))
	})
	if srv.Alertmanager("1") != nil {
		t.Fatal("writer's alertmanager config stranded: deferred to a claimant that never wrote it")
	}

	for _, o := range []client.Object{cp, pol} {
		_ = testClient.Delete(testCtx, o)
	}
	if err := testClient.Delete(testCtx, nonwriter); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		return errors.IsNotFound(testClient.Get(testCtx, clientKey(nonwriter), &observabilityv1alpha1.Tenant{}))
	})
}

// TestTenantFinalizerSkipsMimirRuleNamespacesSharedWithLiveClaimant covers Codex P3-2 (task-20 review
// round 1): the rule-namespace ownership guard (finalizeOwner over claimants(..., withPrefix=true))
// had no regression test on either backend -- deleting the guard from both call sites left the whole
// suite green. Two Tenants sharing tenantId+address+prefix collide on rule namespaces, the exact
// scenario conflictingTenant exists to guard in the steady-state prune; deleting one must not remove
// the survivor's live Mimir rule namespace.
func TestTenantFinalizerSkipsMimirRuleNamespacesSharedWithLiveClaimant(t *testing.T) {
	srv := fake.New()
	defer srv.Close()
	const collidePrefix = "fin-mrule-collide"
	const survivorNS = collidePrefix + "/default/keep-m"

	survivor := &observabilityv1alpha1.Tenant{ObjectMeta: metav1.ObjectMeta{Name: "fin-mrule-survivor"},
		Spec: observabilityv1alpha1.TenantSpec{TenantID: "1", Mimir: &observabilityv1alpha1.BackendSpec{Address: srv.URL}, RulesNamespacePrefix: collidePrefix}}
	if err := testClient.Create(testCtx, survivor); err != nil {
		t.Fatal(err)
	}
	survivorGroup := &observabilityv1alpha1.AlertRuleGroup{ObjectMeta: metav1.ObjectMeta{Name: "keep-m", Namespace: "default"},
		Spec: observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: survivor.Name, Backend: "mimir", Groups: []observabilityv1alpha1.RuleGroup{
			{Name: "g1", Rules: []observabilityv1alpha1.Rule{{Alert: "A", Expr: "up == 0"}}},
		}}}
	if err := testClient.Create(testCtx, survivorGroup); err != nil {
		t.Fatal(err)
	}
	waitCondition(t, survivor, observabilityv1alpha1.ConditionMimirRulesSynced, metav1.ConditionTrue, observabilityv1alpha1.ReasonSynced)
	if len(srv.Rules("1")[survivorNS]) != 1 {
		t.Fatalf("precondition: survivor namespace not populated, got %+v", srv.Rules("1"))
	}

	dying := &observabilityv1alpha1.Tenant{ObjectMeta: metav1.ObjectMeta{Name: "fin-mrule-dying"},
		Spec: observabilityv1alpha1.TenantSpec{TenantID: "1", Mimir: &observabilityv1alpha1.BackendSpec{Address: srv.URL}, RulesNamespacePrefix: collidePrefix}}
	if err := testClient.Create(testCtx, dying); err != nil {
		t.Fatal(err)
	}
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
	if len(srv.Rules("1")[survivorNS]) != 1 {
		t.Fatal("survivor's mimir rule namespace wrongly deleted by the colliding Tenant's finalizer")
	}

	if err := testClient.Delete(testCtx, survivorGroup); err != nil {
		t.Fatal(err)
	}
	if err := testClient.Delete(testCtx, survivor); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		return errors.IsNotFound(testClient.Get(testCtx, clientKey(survivor), &observabilityv1alpha1.Tenant{}))
	})
}

// TestTenantFinalizerSkipsLokiRuleNamespacesSharedWithLiveClaimant is
// TestTenantFinalizerSkipsMimirRuleNamespacesSharedWithLiveClaimant's Loki counterpart -- the P3-2
// finding explicitly calls out both backends, and the ownership guard is applied independently per
// backend (deleteOwnedNamespaces is called separately for Mimir and Loki), so covering one backend
// says nothing about the other.
func TestTenantFinalizerSkipsLokiRuleNamespacesSharedWithLiveClaimant(t *testing.T) {
	srv := fake.New()
	defer srv.Close()
	const collidePrefix = "fin-lrule-collide"
	// "_"-joined: Loki's ruler rejects a namespace containing "/" (alerts-operator-b4o).
	const survivorNS = collidePrefix + "_default_keep-l"

	survivor := &observabilityv1alpha1.Tenant{ObjectMeta: metav1.ObjectMeta{Name: "fin-lrule-survivor"},
		Spec: observabilityv1alpha1.TenantSpec{TenantID: "1", Loki: &observabilityv1alpha1.BackendSpec{Address: srv.URL}, RulesNamespacePrefix: collidePrefix}}
	if err := testClient.Create(testCtx, survivor); err != nil {
		t.Fatal(err)
	}
	survivorGroup := &observabilityv1alpha1.AlertRuleGroup{ObjectMeta: metav1.ObjectMeta{Name: "keep-l", Namespace: "default"},
		Spec: observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: survivor.Name, Backend: "loki", Groups: ruleGroups(`{a="b"}`)}}
	if err := testClient.Create(testCtx, survivorGroup); err != nil {
		t.Fatal(err)
	}
	waitCondition(t, survivor, observabilityv1alpha1.ConditionLokiRulesSynced, metav1.ConditionTrue, observabilityv1alpha1.ReasonSynced)
	if len(srv.LokiRules("1")[survivorNS]) != 1 {
		t.Fatalf("precondition: survivor namespace not populated, got %+v", srv.LokiRules("1"))
	}

	dying := &observabilityv1alpha1.Tenant{ObjectMeta: metav1.ObjectMeta{Name: "fin-lrule-dying"},
		Spec: observabilityv1alpha1.TenantSpec{TenantID: "1", Loki: &observabilityv1alpha1.BackendSpec{Address: srv.URL}, RulesNamespacePrefix: collidePrefix}}
	if err := testClient.Create(testCtx, dying); err != nil {
		t.Fatal(err)
	}
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
	if len(srv.LokiRules("1")[survivorNS]) != 1 {
		t.Fatal("survivor's loki rule namespace wrongly deleted by the colliding Tenant's finalizer")
	}

	if err := testClient.Delete(testCtx, survivorGroup); err != nil {
		t.Fatal(err)
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

// TestTenantFinalizerDeletesAlertmanagerAfterNotificationPolicyRemoved answers a question the task-20
// review asked directly: once a Tenant has written an Alertmanager document and then has its
// NotificationPolicy removed, what does the P2-1 gate (tenant.Status.AlertmanagerConfigHash != "")
// see, and does cleanup still happen correctly?
//
// AlertmanagerConfigHash is set exactly once, on syncAlertmanager's success path
// (tenant_alertmanager.go:127), and is never cleared anywhere in the codebase -- grepped: the only
// other reference is the cache-recency check that reads it, never resets it. Once a NotificationPolicy
// is removed, ch.Policy == nil short-circuits syncAlertmanager before it touches the backend or the
// hash at all (the ReasonNoNotificationPolicy branch), so the hash is left exactly as it was: stale
// (it no longer reflects "what would be compiled today"), but still a true record that this Tenant,
// and no one else, authored what is currently live in Mimir (single-writer model: only this
// Tenant's own reconciler was ever a candidate to have overwritten it since). So the gate still reads
// non-empty and the finalizer still deletes it -- correctly, because the document sitting in Mimir is
// still this Tenant's document, not a hand-written or third-party one; only its policy went away, not
// its ownership of what it already pushed.
func TestTenantFinalizerDeletesAlertmanagerAfterNotificationPolicyRemoved(t *testing.T) {
	srv := fake.New()
	defer srv.Close()

	tn := &observabilityv1alpha1.Tenant{ObjectMeta: metav1.ObjectMeta{Name: "fin-am-policyremoved"},
		Spec: observabilityv1alpha1.TenantSpec{TenantID: "1", Mimir: &observabilityv1alpha1.BackendSpec{Address: srv.URL}, RulesNamespacePrefix: "fin-policyremoved"}}
	if err := testClient.Create(testCtx, tn); err != nil {
		t.Fatal(err)
	}
	cp, pol := writingContactPointAndPolicy(tn.Name, "policyremoved")
	if err := testClient.Create(testCtx, cp); err != nil {
		t.Fatal(err)
	}
	if err := testClient.Create(testCtx, pol); err != nil {
		t.Fatal(err)
	}
	waitCondition(t, tn, observabilityv1alpha1.ConditionAlertmanagerSynced, metav1.ConditionTrue, observabilityv1alpha1.ReasonSynced)
	writtenHash := tn.Status.AlertmanagerConfigHash
	if writtenHash == "" || srv.Alertmanager("1") == nil {
		t.Fatal("precondition: tenant never wrote an alertmanager document")
	}

	// Remove the NotificationPolicy: the next reconcile takes syncAlertmanager's ch.Policy==nil
	// early return and never touches the backend or the hash again.
	if err := testClient.Delete(testCtx, pol); err != nil {
		t.Fatal(err)
	}
	waitCondition(t, tn, observabilityv1alpha1.ConditionAlertmanagerSynced, metav1.ConditionFalse, observabilityv1alpha1.ReasonNoNotificationPolicy)
	if tn.Status.AlertmanagerConfigHash != writtenHash {
		t.Fatalf("hash changed after policy removal: got %q, want unchanged %q", tn.Status.AlertmanagerConfigHash, writtenHash)
	}
	if srv.Alertmanager("1") == nil {
		t.Fatal("alertmanager document unexpectedly disappeared from the backend after policy removal")
	}

	if err := testClient.Delete(testCtx, tn); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		return errors.IsNotFound(testClient.Get(testCtx, clientKey(tn), &observabilityv1alpha1.Tenant{}))
	})
	if srv.Alertmanager("1") != nil {
		t.Fatal("finalizer failed to delete the tenant's own (stale but genuinely authored) alertmanager document")
	}

	_ = testClient.Delete(testCtx, cp)
}

// TestTenantFinalizerDoesNotDeleteAlertmanagerAfterAddressRepointedWithoutResync covers a Codex P1
// finding on the fix-round-1 (P2-1/P2-2) change: spec.mimir.address is mutable (repointing at a moved
// gateway is legitimate operations, tenant_types.go), but status.AlertmanagerConfigHash alone carries
// no record of *which* address it was confirmed against. If a Tenant successfully writes to address A
// (hash set), is then repointed to address B, and syncAlertmanager has not yet (or cannot) confirm a
// write to B, the hash is still non-empty -- stale evidence from A, not B. Without binding the hash to
// an address, deleting this Tenant would attempt to delete whatever document happens to live at the
// *new* address B, which this Tenant never wrote.
//
// Driven directly against finalize() with a fake client + a real fake Mimir server for B, not through
// the full envtest reconcile loop: reproducing "syncAlertmanager attempted and failed to confirm
// against B" deterministically through real reconciliation would depend on timing the manager's watch
// against a live resync, which is inherently racy. Status is set directly to the exact state the race
// would produce (hash confirmed, but AlertmanagerConfigAddress still recording the old address A) and
// finalize is called once, matching the pattern used elsewhere in this package for isolated Reconcile
// behavior (see TestTenantReadyOnFirstReconcileFallsBackToPending in tenant_controller_test.go).
func TestTenantFinalizerDoesNotDeleteAlertmanagerAfterAddressRepointedWithoutResync(t *testing.T) {
	srvB := fake.New()
	defer srvB.Close()
	const foreign = "route:\n  receiver: foreign\nreceivers:\n- name: foreign\n"
	srvB.SetAlertmanager("1", &backend.AlertmanagerConfig{Config: foreign})

	tn := &observabilityv1alpha1.Tenant{
		ObjectMeta: metav1.ObjectMeta{Name: "fin-am-repointed"},
		Spec:       observabilityv1alpha1.TenantSpec{TenantID: "1", Mimir: &observabilityv1alpha1.BackendSpec{Address: srvB.URL}},
		Status: observabilityv1alpha1.TenantStatus{
			// Confirmed once against a since-abandoned address A -- never against srvB (current).
			AlertmanagerConfigHash:    "sha256:stale-from-address-a",
			AlertmanagerConfigAddress: "http://address-a.invalid",
		},
	}
	r := &TenantReconciler{Client: newChildrenFakeClient(t, tn), Recorder: record.NewFakeRecorder(20)}
	if err := r.finalize(context.Background(), tn); err != nil {
		t.Fatal(err)
	}
	if got := srvB.Alertmanager("1"); got == nil || got.Config != foreign {
		t.Fatalf("finalizer deleted a foreign alertmanager config at the new address using a hash confirmed only against the old one: %+v", got)
	}
}
