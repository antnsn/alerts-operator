package controller

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"

	observabilityv1alpha1 "github.com/antnsn/alerts-operator/api/v1alpha1"
	"github.com/antnsn/alerts-operator/internal/backend"
	fakebackend "github.com/antnsn/alerts-operator/internal/backend/fake"
	"github.com/antnsn/alerts-operator/internal/backend/mimir"
)

func countPrefix(reqs []string, prefix string) int {
	n := 0
	for _, r := range reqs {
		if strings.HasPrefix(r, prefix) {
			n++
		}
	}
	return n
}

// rawRoute encodes a Route as the raw JSON its recursive Routes field actually stores
// (apiextensionsv1.JSON), the same convention used throughout the NotificationPolicy tests.
func rawRoute(t *testing.T, r observabilityv1alpha1.Route) apiextensionsv1.JSON {
	t.Helper()
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	return apiextensionsv1.JSON{Raw: b}
}

func TestTenantSyncsRulesAndAlertmanager(t *testing.T) {
	tn, srv := newFakeTenant(t, "sync-tenant", true, true)
	// Foreign state that must survive: a namespace outside our prefix.
	srv.SetRules("1", "other/x", []backend.RuleGroup{{Name: "keep-me", Rules: []backend.Rule{{Alert: "K", Expr: "up"}}}})

	mimirG := &observabilityv1alpha1.AlertRuleGroup{ObjectMeta: metav1.ObjectMeta{Name: "sync-m", Namespace: "default"},
		Spec: observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: tn.Name, Backend: "mimir", Groups: []observabilityv1alpha1.RuleGroup{
			{Name: "g1", Rules: []observabilityv1alpha1.Rule{{Alert: "A", Expr: "up == 0"}}},
			{Name: "g2", Rules: []observabilityv1alpha1.Rule{{Record: "r", Expr: "avg(up)"}}},
		}}}
	lokiG := &observabilityv1alpha1.AlertRuleGroup{ObjectMeta: metav1.ObjectMeta{Name: "sync-l", Namespace: "default"},
		Spec: observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: tn.Name, Backend: "loki", Groups: ruleGroups(`sum(rate({job="x"} |= "e" [5m])) > 0`)}}
	sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "sync-po", Namespace: "default"}, StringData: map[string]string{"user": "U", "token": "T"}}
	keep := &observabilityv1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "keep", Namespace: "default"},
		Spec: observabilityv1alpha1.ContactPointSpec{TenantRef: tn.Name, Webhook: []observabilityv1alpha1.WebhookConfig{{URL: "http://keep"}}}}
	po := &observabilityv1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "pushover", Namespace: "default"},
		Spec: observabilityv1alpha1.ContactPointSpec{TenantRef: tn.Name, Pushover: []observabilityv1alpha1.PushoverConfig{{
			UserKeySecretRef: observabilityv1alpha1.SecretKeyRef{Name: "sync-po", Key: "user"},
			TokenSecretRef:   observabilityv1alpha1.SecretKeyRef{Name: "sync-po", Key: "token"}}}}}
	pol := &observabilityv1alpha1.NotificationPolicy{ObjectMeta: metav1.ObjectMeta{Name: "sync-pol", Namespace: "default"},
		Spec: observabilityv1alpha1.NotificationPolicySpec{TenantRef: tn.Name, Route: observabilityv1alpha1.Route{Receiver: "keep",
			Routes: []apiextensionsv1.JSON{rawRoute(t, observabilityv1alpha1.Route{Receiver: "pushover", Matchers: []string{`severity="critical"`}})}}}}
	createAndCleanup(t, sec)
	for _, o := range []observabilityv1alpha1.Conditioned{mimirG, lokiG, keep, po, pol} {
		createAndCleanup(t, o)
	}

	waitCondition(t, tn, observabilityv1alpha1.ConditionReady, metav1.ConditionTrue, "")
	rules, lokiRules := srv.Rules("1"), srv.LokiRules("1")
	// The Loki namespace is joined with "_", not "/": Loki's ruler 404s on any namespace containing
	// a slash, so this spelling is the whole point of the fix (alerts-operator-b4o). Mimir keeps "/".
	if len(rules["alerts-operator/default/sync-m"]) != 2 || len(lokiRules["alerts-operator_default_sync-l"]) != 1 || len(rules["other/x"]) != 1 {
		t.Fatalf("rules in backend: mimir=%+v loki=%+v", rules, lokiRules)
	}
	for ns := range lokiRules {
		if strings.Contains(ns, "/") {
			t.Fatalf("a Loki rule namespace written by the operator must never contain %q: %q", "/", ns)
		}
	}
	if _, leaked := rules["alerts-operator_default_sync-l"]; leaked {
		t.Fatal("loki group must not be written to mimir")
	}
	am := srv.Alertmanager("1")
	if am == nil || !strings.Contains(am.Config, "receiver: default/keep") || !strings.Contains(am.Config, "- name: default/pushover") || !strings.Contains(am.Config, "user_key: U") {
		t.Fatalf("alertmanager config: %+v", am)
	}
	if tn.Status.RuleGroups.Mimir != 2 || tn.Status.RuleGroups.Loki != 1 || !strings.HasPrefix(tn.Status.AlertmanagerConfigHash, "sha256:") {
		t.Fatalf("status: %+v", tn.Status)
	}
	for _, o := range []observabilityv1alpha1.Conditioned{mimirG, lokiG, keep, po, pol} {
		waitCondition(t, o, observabilityv1alpha1.ConditionSynced, metav1.ConditionTrue, observabilityv1alpha1.ReasonSynced)
	}

	// No-op reconcile: metadata-only change on a child must not POST anything.
	srv.ResetRequests()
	if err := testClient.Get(testCtx, clientKey(mimirG), mimirG); err != nil {
		t.Fatal(err)
	}
	mimirG.Labels = map[string]string{"touch": "1"}
	if err := testClient.Update(testCtx, mimirG); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return countPrefix(srv.Requests(), "GET ") >= 2 })
	if n := countPrefix(srv.Requests(), "POST "); n != 0 {
		t.Fatalf("expected no POST on no-op, got %d: %v", n, srv.Requests())
	}

	// Change one group → exactly one POST, to that namespace.
	srv.ResetRequests()
	if err := testClient.Get(testCtx, clientKey(mimirG), mimirG); err != nil {
		t.Fatal(err)
	}
	mimirG.Spec.Groups[0].Rules[0].Expr = "up == 1"
	if err := testClient.Update(testCtx, mimirG); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		return srv.Rules("1")["alerts-operator/default/sync-m"][0].Rules[0].Expr == "up == 1"
	})
	if n := countPrefix(srv.Requests(), "POST /prometheus/config/v1/rules/alerts-operator%2Fdefault%2Fsync-m"); n != 1 {
		t.Fatalf("expected exactly one rules POST, got %d: %v", n, srv.Requests())
	}
	if n := countPrefix(srv.Requests(), "POST /api/v1/alerts"); n != 0 {
		t.Fatalf("alertmanager must not be re-posted: %v", srv.Requests())
	}

	// Remove a rule group → its namespace is pruned; foreign namespace untouched.
	if err := testClient.Delete(testCtx, lokiG); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { _, ok := srv.LokiRules("1")["alerts-operator_default_sync-l"]; return !ok })
	if _, ok := srv.Rules("1")["other/x"]; !ok {
		t.Fatal("foreign namespace was pruned")
	}

	// Backend down → BackendUnavailable, Ready False; recovery → Ready True.
	srv.Fail(503)
	if err := testClient.Get(testCtx, clientKey(mimirG), mimirG); err != nil {
		t.Fatal(err)
	}
	mimirG.Spec.Groups[1].Rules[0].Expr = "max(up)"
	if err := testClient.Update(testCtx, mimirG); err != nil {
		t.Fatal(err)
	}
	waitCondition(t, tn, observabilityv1alpha1.ConditionMimirRulesSynced, metav1.ConditionFalse, observabilityv1alpha1.ReasonBackendUnavailable)
	waitCondition(t, tn, observabilityv1alpha1.ConditionReady, metav1.ConditionFalse, observabilityv1alpha1.ReasonBackendUnavailable)
	srv.Fail(0)
	waitCondition(t, tn, observabilityv1alpha1.ConditionReady, metav1.ConditionTrue, "")
	if srv.Rules("1")["alerts-operator/default/sync-m"][1].Rules[0].Expr != "max(up)" {
		t.Fatal("change lost after recovery")
	}
}

// TestTenantKeepsStaleGenerationNamespace covers the race the controller must not lose: a spec edit
// bumps an AlertRuleGroup's Generation and enqueues both its own reconciler and the Tenant
// reconciler. If the Tenant runs first, acceptedCurrent is false for that group — without
// KeepNamespaces, syncRules would read that as "no longer desired" and prune its backend namespace,
// producing a transient alerting gap until the child re-validates.
//
// This is built on the same isolated fixture as TestTenantListChildrenStaleGeneration
// (newChildrenFakeClient) rather than envtest's shared manager: listChildren's MatchingFields query
// needs a cache-backed client (a real API server rejects field selectors on custom CRD fields
// outright — confirmed by hand, not merely asserted), and racing the live AlertRuleGroupReconciler
// to observe a transient "stale" window through that cache is not just flaky but effectively
// unwinnable — its correction lands well inside one poll tick, so the window is never observed. The
// isolated fake client has no live reconciler at all: we seed the "child hasn't re-validated yet"
// state directly and drive the Tenant reconciler by hand, which is what the envtest version was
// trying (and failing) to approximate.
func TestTenantKeepsStaleGenerationNamespace(t *testing.T) {
	s := fakebackend.New()
	t.Cleanup(s.Close)
	const oldGen, newGen = int64(1), int64(2)
	s.SetRules("1", "alerts-operator/default/stale-m", []backend.RuleGroup{{Name: "g", Rules: []backend.Rule{{Alert: "A", Expr: "up == 0"}}}})

	tn := &observabilityv1alpha1.Tenant{
		ObjectMeta: metav1.ObjectMeta{Name: "stale-tenant", Finalizers: []string{tenantFinalizer}},
		Spec:       observabilityv1alpha1.TenantSpec{TenantID: "1", Mimir: &observabilityv1alpha1.BackendSpec{Address: s.URL}},
	}
	// Generation has already moved to newGen (the spec edit that changed the rule expression), but
	// Accepted is still the pre-edit observation at oldGen: exactly the window between a child's
	// spec edit and its own reconciler re-validating it.
	mimirG := &observabilityv1alpha1.AlertRuleGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "stale-m", Namespace: "default", Generation: newGen},
		Spec:       observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: tn.Name, Backend: observabilityv1alpha1.BackendMimir, Groups: ruleGroups("up == 1")},
		Status:     observabilityv1alpha1.AlertRuleGroupStatus{Conditions: acceptedAt(oldGen)},
	}

	r := &TenantReconciler{Client: newChildrenFakeClient(t, tn, mimirG), Recorder: record.NewFakeRecorder(20)}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: tn.Name}}); err != nil {
		t.Fatal(err)
	}

	if len(s.Rules("1")["alerts-operator/default/stale-m"]) != 1 {
		t.Fatalf("stale-generation group's namespace must not be deleted: %+v", s.Rules("1"))
	}
	if n := countPrefix(s.Requests(), "DELETE"); n != 0 {
		t.Fatalf("no DELETE expected for a stale-generation namespace: %v", s.Requests())
	}
	// MimirRulesSynced must not claim completeness this pass never established: the excluded
	// namespace's content might not match its just-edited spec yet, so True/Synced would be a lie.
	var afterTn observabilityv1alpha1.Tenant
	if err := r.Get(context.Background(), clientKey(tn), &afterTn); err != nil {
		t.Fatal(err)
	}
	if c := findCond(&afterTn, observabilityv1alpha1.ConditionMimirRulesSynced); c.Status != metav1.ConditionUnknown || c.Reason != observabilityv1alpha1.ReasonPending {
		t.Fatalf("MimirRulesSynced must report Unknown/Pending while a stale-generation namespace is excluded from this pass, got %+v", c)
	}

	// The child "reconciles" (Accepted catches up to newGen); the Tenant reconciler must then push
	// the content it was withholding.
	var got observabilityv1alpha1.AlertRuleGroup
	if err := r.Get(context.Background(), clientKey(mimirG), &got); err != nil {
		t.Fatal(err)
	}
	setCondition(&got.Status.Conditions, observabilityv1alpha1.ConditionAccepted, metav1.ConditionTrue, observabilityv1alpha1.ReasonAccepted, "", newGen)
	if err := r.Status().Update(context.Background(), &got); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: tn.Name}}); err != nil {
		t.Fatal(err)
	}
	rs := s.Rules("1")["alerts-operator/default/stale-m"]
	if len(rs) != 1 || rs[0].Rules[0].Expr != "up == 1" {
		t.Fatalf("child re-validating should let the Tenant reconciler push the new content: %+v", rs)
	}
}

// TestTenantKeepsBothBackendNamespacesWhileBackendSwitchIsStale covers the Codex P1 on the
// Loki-separator change (alerts-operator-b4o review round 1). Editing an AlertRuleGroup's
// spec.backend bumps its Generation, and spec.backend already reads as the NEW backend while
// Accepted still describes the old one. Keying the keep entry only by spec.backend would therefore
// protect the namespace the group is moving *to* and leave the one it currently occupies
// unprotected: the old backend's prune sees a namespace under its prefix that nothing desires and
// deletes live, firing rules before the edit has even been validated.
//
// Before the separator change this could not happen -- both backends computed the same namespace
// string, so one keep entry covered both. The fix restores that property explicitly by keeping the
// namespace on every backend during the stale window.
func TestTenantKeepsBothBackendNamespacesWhileBackendSwitchIsStale(t *testing.T) {
	s := fakebackend.New()
	t.Cleanup(s.Close)
	const oldGen, newGen = int64(1), int64(2)
	// What the group currently occupies, written while it was still backend: mimir.
	s.SetRules("1", "alerts-operator/default/switcher", []backend.RuleGroup{{Name: "g", Rules: []backend.Rule{{Alert: "A", Expr: "up == 0"}}}})

	tn := &observabilityv1alpha1.Tenant{
		ObjectMeta: metav1.ObjectMeta{Name: "switch-tenant", Finalizers: []string{tenantFinalizer}},
		Spec: observabilityv1alpha1.TenantSpec{TenantID: "1",
			Mimir: &observabilityv1alpha1.BackendSpec{Address: s.URL},
			Loki:  &observabilityv1alpha1.BackendSpec{Address: s.URL}},
	}
	// spec.backend already says loki; Accepted is still the pre-edit observation at oldGen.
	switcher := &observabilityv1alpha1.AlertRuleGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "switcher", Namespace: "default", Generation: newGen},
		Spec:       observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: tn.Name, Backend: observabilityv1alpha1.BackendLoki, Groups: ruleGroups(`{job="x"} |= "e"`)},
		Status:     observabilityv1alpha1.AlertRuleGroupStatus{Conditions: acceptedAt(oldGen)},
	}

	r := &TenantReconciler{Client: newChildrenFakeClient(t, tn, switcher), Recorder: record.NewFakeRecorder(20)}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: tn.Name}}); err != nil {
		t.Fatal(err)
	}

	if len(s.Rules("1")["alerts-operator/default/switcher"]) != 1 {
		t.Fatalf("the namespace the group still occupies must survive the unvalidated window: %+v mimir=%+v requests=%v",
			s.Rules("1"), s.Rules("1"), s.Requests())
	}
	if n := countPrefix(s.Requests(), "DELETE"); n != 0 {
		t.Fatalf("no DELETE expected while a backend switch is unvalidated: %v", s.Requests())
	}
}

func TestTenantWithoutPolicy(t *testing.T) {
	tn, srv := newFakeTenant(t, "nopol-tenant", true, false)
	waitCondition(t, tn, observabilityv1alpha1.ConditionAlertmanagerSynced, metav1.ConditionFalse, observabilityv1alpha1.ReasonNoNotificationPolicy)
	waitCondition(t, tn, observabilityv1alpha1.ConditionReady, metav1.ConditionFalse, observabilityv1alpha1.ReasonNoNotificationPolicy)
	if srv.Alertmanager("1") != nil || countPrefix(srv.Requests(), "POST /api/v1/alerts") != 0 || countPrefix(srv.Requests(), "DELETE /api/v1/alerts") != 0 {
		t.Fatalf("backend AM must be untouched: %v", srv.Requests())
	}
}

func TestTenantAlertmanagerRejected(t *testing.T) {
	tn, srv := newFakeTenant(t, "rej-tenant", true, false)
	keep := &observabilityv1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "rej-keep", Namespace: "default"},
		Spec: observabilityv1alpha1.ContactPointSpec{TenantRef: tn.Name, Webhook: []observabilityv1alpha1.WebhookConfig{{URL: "http://keep"}}}}
	pol := &observabilityv1alpha1.NotificationPolicy{ObjectMeta: metav1.ObjectMeta{Name: "rej-pol", Namespace: "default"},
		Spec: observabilityv1alpha1.NotificationPolicySpec{TenantRef: tn.Name, Route: observabilityv1alpha1.Route{Receiver: "rej-keep"}}}
	srv.RejectPost("tenant quota exceeded")
	createAndCleanup(t, keep)
	createAndCleanup(t, pol)
	waitCondition(t, tn, observabilityv1alpha1.ConditionAlertmanagerSynced, metav1.ConditionFalse, observabilityv1alpha1.ReasonRejected)
	if !strings.Contains(findCond(tn, observabilityv1alpha1.ConditionAlertmanagerSynced).Message, "tenant quota exceeded") {
		t.Fatalf("message should carry backend body: %+v", findCond(tn, observabilityv1alpha1.ConditionAlertmanagerSynced))
	}
	waitCondition(t, pol, observabilityv1alpha1.ConditionSynced, metav1.ConditionFalse, observabilityv1alpha1.ReasonRejected)
	srv.RejectPost("")
	// Trigger a re-sync by touching the policy.
	if err := testClient.Get(testCtx, clientKey(pol), pol); err != nil {
		t.Fatal(err)
	}
	pol.Spec.Route.GroupWait = "10s"
	if err := testClient.Update(testCtx, pol); err != nil {
		t.Fatal(err)
	}
	waitCondition(t, tn, observabilityv1alpha1.ConditionAlertmanagerSynced, metav1.ConditionTrue, observabilityv1alpha1.ReasonSynced)
}

// TestTenantAlertmanagerCompileErrorClassifiedAsInvalid covers a Codex review finding on this task:
// a compile.AttributedError (a validation failure, e.g. a malformed webhook URL that passes CRD
// validation but fails Alertmanager's own config loader) must set the offending child's Synced
// condition to False/Invalid directly, not go through setChildSynced/syncedFromErr -- that
// classifier is built for backend/transport errors, and IsUnavailable's fallback ("anything that
// isn't a *backend.StatusError counts as unavailable") would otherwise misreport a content problem
// as a transient backend outage.
func TestTenantAlertmanagerCompileErrorClassifiedAsInvalid(t *testing.T) {
	tn := &observabilityv1alpha1.Tenant{
		ObjectMeta: metav1.ObjectMeta{Name: "tn-compile-err", Finalizers: []string{tenantFinalizer}},
		Spec:       observabilityv1alpha1.TenantSpec{TenantID: "1", Mimir: &observabilityv1alpha1.BackendSpec{Address: "http://stub"}},
	}
	badCP := &observabilityv1alpha1.ContactPoint{
		ObjectMeta: metav1.ObjectMeta{Name: "bad-hook", Namespace: "default", Generation: 1},
		Spec:       observabilityv1alpha1.ContactPointSpec{TenantRef: tn.Name, Webhook: []observabilityv1alpha1.WebhookConfig{{URL: "not-a-url"}}},
		Status:     observabilityv1alpha1.ContactPointStatus{Conditions: acceptedAt(1)},
	}
	pol := &observabilityv1alpha1.NotificationPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "pol", Namespace: "default", Generation: 1},
		Spec:       observabilityv1alpha1.NotificationPolicySpec{TenantRef: tn.Name, Route: observabilityv1alpha1.Route{Receiver: "bad-hook"}},
		Status:     observabilityv1alpha1.NotificationPolicyStatus{Conditions: acceptedAt(1)},
	}

	r := &TenantReconciler{
		Client:   newChildrenFakeClient(t, tn, badCP, pol),
		Recorder: record.NewFakeRecorder(20),
		NewMimir: func(backend.Options) MimirClient { return stubMimirClient{} },
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: tn.Name}}); err != nil {
		t.Fatal(err)
	}

	var got observabilityv1alpha1.ContactPoint
	if err := r.Get(context.Background(), clientKey(badCP), &got); err != nil {
		t.Fatal(err)
	}
	if c := findCond(&got, observabilityv1alpha1.ConditionSynced); c.Status != metav1.ConditionFalse || c.Reason != observabilityv1alpha1.ReasonInvalid {
		t.Fatalf("compile/validation failure on a ContactPoint must be Synced=False/Invalid, got %+v", c)
	}
}

// TestTenantAlertmanagerRechecksBackendAfterGenerationChange covers a second Codex review finding:
// the hash+recency skip in syncAlertmanager must not treat a Tenant generation change (e.g.
// spec.mimir.address or spec.tenantId edited to point at a different backend/tenant) as a no-op just
// because the compiled Policy/ContactPoints document happens to hash the same as before -- that would
// report Synced without ever having verified the (possibly different) backend actually holds it.
func TestTenantAlertmanagerRechecksBackendAfterGenerationChange(t *testing.T) {
	s := fakebackend.New()
	t.Cleanup(s.Close)

	tn := &observabilityv1alpha1.Tenant{
		ObjectMeta: metav1.ObjectMeta{Name: "tn-am-regen", Generation: 1},
		Spec:       observabilityv1alpha1.TenantSpec{TenantID: "1", Mimir: &observabilityv1alpha1.BackendSpec{Address: s.URL}},
	}
	keepCP := &observabilityv1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "keep", Namespace: "default"},
		Spec: observabilityv1alpha1.ContactPointSpec{TenantRef: tn.Name, Webhook: []observabilityv1alpha1.WebhookConfig{{URL: "http://keep"}}}}
	pol := &observabilityv1alpha1.NotificationPolicy{ObjectMeta: metav1.ObjectMeta{Name: "pol", Namespace: "default"},
		Spec: observabilityv1alpha1.NotificationPolicySpec{TenantRef: tn.Name, Route: observabilityv1alpha1.Route{Receiver: "keep"}}}

	r := &TenantReconciler{Client: newChildrenFakeClient(t, keepCP, pol), Recorder: record.NewFakeRecorder(20)}
	var freshCP observabilityv1alpha1.ContactPoint
	if err := r.Get(context.Background(), clientKey(keepCP), &freshCP); err != nil {
		t.Fatal(err)
	}
	var freshPol observabilityv1alpha1.NotificationPolicy
	if err := r.Get(context.Background(), clientKey(pol), &freshPol); err != nil {
		t.Fatal(err)
	}
	ch := &children{Policy: &freshPol, ContactPoints: []observabilityv1alpha1.ContactPoint{freshCP}}
	mc := mimir.New(backend.Options{Address: s.URL, TenantID: "1"})

	if err := r.syncAlertmanager(context.Background(), tn, mc, ch); err != nil {
		t.Fatal(err)
	}
	if n := countPrefix(s.Requests(), "GET /api/v1/alerts"); n != 1 {
		t.Fatalf("expected one AM GET on the first sync, got %d: %v", n, s.Requests())
	}

	// Same Policy/ContactPoints (same compiled hash) but a new generation, as if spec.mimir.address
	// or spec.tenantId had just changed.
	tn.Generation = 2
	s.ResetRequests()
	if err := r.syncAlertmanager(context.Background(), tn, mc, ch); err != nil {
		t.Fatal(err)
	}
	if n := countPrefix(s.Requests(), "GET /api/v1/alerts"); n != 1 {
		t.Fatalf("a generation change must force a real backend check even when the compiled hash is unchanged, got %d GETs: %v", n, s.Requests())
	}
}

// TestTenantAlertmanagerRechecksBackendAfterAuthRotation covers a follow-on Codex review finding on
// the same cache: a Secret watch enqueues the Tenant on a backend-auth Secret change without ever
// touching Generation, so gen+hash alone still can't tell a credential rotation from "nothing
// changed" -- the previous fix (generation) doesn't cover it.
func TestTenantAlertmanagerRechecksBackendAfterAuthRotation(t *testing.T) {
	s := fakebackend.New()
	t.Cleanup(s.Close)

	sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "am-auth", Namespace: "default"},
		Data: map[string][]byte{"username": []byte("u1"), "password": []byte("p1")}}
	tn := &observabilityv1alpha1.Tenant{
		ObjectMeta: metav1.ObjectMeta{Name: "tn-am-auth", Generation: 1},
		Spec: observabilityv1alpha1.TenantSpec{TenantID: "1", Mimir: &observabilityv1alpha1.BackendSpec{
			Address: s.URL,
			Auth:    &observabilityv1alpha1.BackendAuth{BasicAuthSecretRef: &observabilityv1alpha1.NamespacedName{Namespace: "default", Name: "am-auth"}},
		}},
	}
	keepCP := &observabilityv1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "keep", Namespace: "default"},
		Spec: observabilityv1alpha1.ContactPointSpec{TenantRef: tn.Name, Webhook: []observabilityv1alpha1.WebhookConfig{{URL: "http://keep"}}}}
	pol := &observabilityv1alpha1.NotificationPolicy{ObjectMeta: metav1.ObjectMeta{Name: "pol", Namespace: "default"},
		Spec: observabilityv1alpha1.NotificationPolicySpec{TenantRef: tn.Name, Route: observabilityv1alpha1.Route{Receiver: "keep"}}}

	r := &TenantReconciler{Client: newChildrenFakeClient(t, sec, keepCP, pol), Recorder: record.NewFakeRecorder(20)}
	var freshCP observabilityv1alpha1.ContactPoint
	if err := r.Get(context.Background(), clientKey(keepCP), &freshCP); err != nil {
		t.Fatal(err)
	}
	var freshPol observabilityv1alpha1.NotificationPolicy
	if err := r.Get(context.Background(), clientKey(pol), &freshPol); err != nil {
		t.Fatal(err)
	}
	ch := &children{Policy: &freshPol, ContactPoints: []observabilityv1alpha1.ContactPoint{freshCP}}
	mc := mimir.New(backend.Options{Address: s.URL, TenantID: "1"})

	if err := r.syncAlertmanager(context.Background(), tn, mc, ch); err != nil {
		t.Fatal(err)
	}
	if n := countPrefix(s.Requests(), "GET /api/v1/alerts"); n != 1 {
		t.Fatalf("expected one AM GET on the first sync, got %d: %v", n, s.Requests())
	}

	// Rotate the secret's password without touching Tenant.Generation at all -- exactly how the
	// Secret watch enqueues the Tenant.
	var freshSec corev1.Secret
	if err := r.Get(context.Background(), clientKey(sec), &freshSec); err != nil {
		t.Fatal(err)
	}
	freshSec.Data["password"] = []byte("p2-rotated")
	if err := r.Update(context.Background(), &freshSec); err != nil {
		t.Fatal(err)
	}

	s.ResetRequests()
	if err := r.syncAlertmanager(context.Background(), tn, mc, ch); err != nil {
		t.Fatal(err)
	}
	if n := countPrefix(s.Requests(), "GET /api/v1/alerts"); n != 1 {
		t.Fatalf("a credential rotation must force a real backend check even when gen+hash are unchanged, got %d GETs: %v", n, s.Requests())
	}
}

// TestSetChildSyncedRefreshesObservedGenerationOnRepeatOutcome covers a third Codex review finding:
// setChildSynced's dedup short-circuit compared only status/reason/message, so a child re-validated
// at a new generation whose outcome happens to match the previous one (e.g. True/Synced/"" both
// times) would never get Synced.ObservedGeneration bumped, leaving it permanently stale even though
// the object really was re-checked at its current generation.
func TestSetChildSyncedRefreshesObservedGenerationOnRepeatOutcome(t *testing.T) {
	cp := &observabilityv1alpha1.ContactPoint{
		ObjectMeta: metav1.ObjectMeta{Name: "cp", Namespace: "default", Generation: 2},
		Spec:       observabilityv1alpha1.ContactPointSpec{TenantRef: "t", Webhook: []observabilityv1alpha1.WebhookConfig{{URL: "http://x"}}},
		// Same outcome setChildSynced(nil) below will compute (True/Synced/""), but observed at the
		// PRE-edit generation: the object's current generation (2) has moved past it.
		Status: observabilityv1alpha1.ContactPointStatus{Conditions: []metav1.Condition{
			{Type: observabilityv1alpha1.ConditionSynced, Status: metav1.ConditionTrue, Reason: observabilityv1alpha1.ReasonSynced, ObservedGeneration: 1},
		}},
	}
	r := &TenantReconciler{Client: newChildrenFakeClient(t, cp)}

	r.setChildSynced(context.Background(), cp, nil)

	var after observabilityv1alpha1.ContactPoint
	if err := r.Get(context.Background(), clientKey(cp), &after); err != nil {
		t.Fatal(err)
	}
	if c := findCond(&after, observabilityv1alpha1.ConditionSynced); c.ObservedGeneration != 2 {
		t.Fatalf("Synced.ObservedGeneration must track the object's current generation even when the outcome is unchanged, got %+v", c)
	}
}

// TestSyncRulesPreservesDesiredCountOnListFailure covers a fourth Codex review finding: on a
// store.List failure, syncRules used to return 0 regardless of how many groups the accepted
// AlertRuleGroups call for -- inconsistent with the success path, which reports that same desired
// count even when some per-namespace writes fail. Zeroing it out on a transient list error would
// misreport "no rule groups" in Tenant status while the groups most likely remain installed exactly
// as before.
func TestSyncRulesPreservesDesiredCountOnListFailure(t *testing.T) {
	s := fakebackend.New()
	t.Cleanup(s.Close)
	s.Fail(503)

	tn := &observabilityv1alpha1.Tenant{ObjectMeta: metav1.ObjectMeta{Name: "tn-count"}, Spec: observabilityv1alpha1.TenantSpec{TenantID: "1"}}
	mimirG := &observabilityv1alpha1.AlertRuleGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "default", Generation: 1},
		Spec: observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: tn.Name, Backend: observabilityv1alpha1.BackendMimir, Groups: []observabilityv1alpha1.RuleGroup{
			{Name: "g1", Rules: []observabilityv1alpha1.Rule{{Alert: "A", Expr: "up == 0"}}},
			{Name: "g2", Rules: []observabilityv1alpha1.Rule{{Alert: "B", Expr: "up == 1"}}},
		}},
		Status: observabilityv1alpha1.AlertRuleGroupStatus{Conditions: acceptedAt(1)},
	}

	r := &TenantReconciler{Client: newChildrenFakeClient(t, mimirG), Recorder: record.NewFakeRecorder(20)}
	mc := mimir.New(backend.Options{Address: s.URL, TenantID: "1"})
	n, err := r.syncRules(context.Background(), tn, mc, observabilityv1alpha1.BackendMimir, []observabilityv1alpha1.AlertRuleGroup{*mimirG}, map[string]bool{})
	if !backend.IsUnavailable(err) {
		t.Fatalf("expected an unavailable error, got %v", err)
	}
	if n != 2 {
		t.Fatalf("desired count must be preserved on a List failure, got %d want 2", n)
	}
}

// mimirTenantAt builds a Tenant CR pointing at the fake backend, with the given effective rules
// namespace prefix spelled out or left unset (""), for the ownership-collision tests below.
func mimirTenantAt(name, addr, prefix string) *observabilityv1alpha1.Tenant {
	return &observabilityv1alpha1.Tenant{
		ObjectMeta: metav1.ObjectMeta{Name: name, Finalizers: []string{tenantFinalizer}},
		Spec: observabilityv1alpha1.TenantSpec{
			TenantID:             "1",
			Mimir:                &observabilityv1alpha1.BackendSpec{Address: addr},
			RulesNamespacePrefix: prefix,
		},
	}
}

func mimirGroupFor(tenant, name string) *observabilityv1alpha1.AlertRuleGroup {
	return &observabilityv1alpha1.AlertRuleGroup{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Generation: 1},
		Spec:       observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: tenant, Backend: observabilityv1alpha1.BackendMimir, Groups: ruleGroups("up == 0")},
		Status:     observabilityv1alpha1.AlertRuleGroupStatus{Conditions: acceptedAt(1)},
	}
}

func seedGroup(alert string) []backend.RuleGroup {
	return []backend.RuleGroup{{Name: "g", Rules: []backend.Rule{{Alert: alert, Expr: "up == 0"}}}}
}

// TestTenantDoesNotPruneWhenAnotherTenantSharesOwnership covers P1-1 of the Task 19 review. A
// backend rule namespace is <prefix><sep><k8s-namespace><sep><name> and carries no Tenant identity, and
// store.List is scoped only by X-Scope-OrgID, so two Tenant CRs sharing tenantId + backend address
// + effective prefix each see the other's namespaces as "under my prefix but not desired" and
// delete them — alternating forever, with Ready=True on both. The review reproduced exactly this:
// one Reconcile of team-a emitted
// DELETE /prometheus/config/v1/rules/alerts-operator%2Fdefault%2Fb-rules.
//
// team-b spells the prefix out while team-a leaves it unset: ownership must be compared on the
// *effective* prefix (Tenant.Prefix()), not the raw field, or the collision is missed for any
// Tenant stored before +kubebuilder:default materialised the field.
func TestTenantDoesNotPruneWhenAnotherTenantSharesOwnership(t *testing.T) {
	s := fakebackend.New()
	t.Cleanup(s.Close)
	s.SetRules("1", "alerts-operator/default/b-rules", seedGroup("B"))

	teamA := mimirTenantAt("team-a", s.URL, "")
	teamB := mimirTenantAt("team-b", s.URL, observabilityv1alpha1.DefaultRulesNamespacePrefix)
	r := &TenantReconciler{
		Client:   newChildrenFakeClient(t, teamA, teamB, mimirGroupFor("team-a", "a-rules"), mimirGroupFor("team-b", "b-rules")),
		Recorder: record.NewFakeRecorder(20),
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: teamA.Name}}); err != nil {
		t.Fatal(err)
	}

	if _, ok := s.Rules("1")["alerts-operator/default/b-rules"]; !ok {
		t.Fatalf("another Tenant's live rule namespace must never be pruned: %+v", s.Rules("1"))
	}
	if n := countPrefix(s.Requests(), "DELETE"); n != 0 {
		t.Fatalf("ambiguous ownership must not delete anything: %v", s.Requests())
	}
	// Writes are convergent, deletes are not: only pruning is suppressed.
	if len(s.Rules("1")["alerts-operator/default/a-rules"]) != 1 {
		t.Fatalf("own groups must still be written while pruning is skipped: %+v", s.Rules("1"))
	}

	var got observabilityv1alpha1.Tenant
	if err := r.Get(context.Background(), clientKey(teamA), &got); err != nil {
		t.Fatal(err)
	}
	c := findCond(&got, observabilityv1alpha1.ConditionMimirRulesSynced)
	if c.Status != metav1.ConditionFalse || c.Reason != observabilityv1alpha1.ReasonConflict {
		t.Fatalf("a shared-ownership Tenant must report MimirRulesSynced=False/Conflict, got %+v", c)
	}
	if !strings.Contains(c.Message, "team-b") {
		t.Fatalf("the condition must name the conflicting Tenant: %+v", c)
	}
}

// TestTenantPrunesWithoutOwnershipConflict is the control for the test above: with no other Tenant
// claiming the same (tenantId, address, prefix), an unwanted namespace under our prefix is still
// pruned exactly as before.
func TestTenantPrunesWithoutOwnershipConflict(t *testing.T) {
	s := fakebackend.New()
	t.Cleanup(s.Close)
	s.SetRules("1", "alerts-operator/default/gone", seedGroup("G"))

	only := mimirTenantAt("only-tenant", s.URL, "")
	r := &TenantReconciler{Client: newChildrenFakeClient(t, only), Recorder: record.NewFakeRecorder(20)}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: only.Name}}); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Rules("1")["alerts-operator/default/gone"]; ok {
		t.Fatalf("an unowned-but-ours namespace must still be pruned when nothing is ambiguous: %+v", s.Rules("1"))
	}
	var got observabilityv1alpha1.Tenant
	if err := r.Get(context.Background(), clientKey(only), &got); err != nil {
		t.Fatal(err)
	}
	if c := findCond(&got, observabilityv1alpha1.ConditionMimirRulesSynced); c.Status != metav1.ConditionTrue {
		t.Fatalf("no conflict, no error: MimirRulesSynced should be True, got %+v", c)
	}
}

// TestTenantPruneMatchesOwnershipOnSegmentBoundary is the guard that has to survive the Loki
// separator change: the prune claims a namespace only when it starts with prefix + *that backend's*
// separator. Deleting the guard -- reverting to a bare strings.HasPrefix(ns, tenant.Prefix()) --
// makes this test delete "alerts-operator-other/default/x" and "alerts-operator-other_default_x",
// which are somebody else's live rules.
//
// It covers both backends deliberately: before the fix the separator was "/" everywhere and only
// the Mimir spelling was ever exercised, so a Loki-side boundary bug would have gone unnoticed. The
// cross-shaped cases matter too: a Mimir-shaped "alerts-operator/..." must not be claimed by the
// *Loki* prune (nothing this operator writes to Loki looks like that), and vice versa.
func TestTenantPruneMatchesOwnershipOnSegmentBoundary(t *testing.T) {
	s := fakebackend.New()
	t.Cleanup(s.Close)

	// Genuinely ours on each backend, and no longer desired -> must be pruned.
	s.SetRules("1", "alerts-operator/default/gone-m", seedGroup("M"))
	s.SetLokiRules("1", "alerts-operator_default_gone-l", seedGroup("L"))
	// Lookalike prefixes: the same leading characters, but the next character is not the separator.
	s.SetRules("1", "alerts-operator-other/default/x", seedGroup("MO"))
	s.SetLokiRules("1", "alerts-operator-other_default_x", seedGroup("LO"))
	// Wrong-backend shapes: each backend must only claim its own separator.
	s.SetRules("1", "alerts-operator_default_mimir-shaped-loki-name", seedGroup("MX"))
	s.SetLokiRules("1", "alerts-operator/default/loki-shaped-mimir-name", seedGroup("LX"))

	tn := mimirTenantAt("boundary-tenant", s.URL, "")
	tn.Spec.Loki = &observabilityv1alpha1.BackendSpec{Address: s.URL}
	r := &TenantReconciler{Client: newChildrenFakeClient(t, tn), Recorder: record.NewFakeRecorder(20)}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: tn.Name}}); err != nil {
		t.Fatal(err)
	}

	mimirNow, lokiNow := s.Rules("1"), s.LokiRules("1")
	for _, gone := range []struct {
		in   map[string][]backend.RuleGroup
		ns   string
		what string
	}{
		{mimirNow, "alerts-operator/default/gone-m", "mimir"},
		{lokiNow, "alerts-operator_default_gone-l", "loki"},
	} {
		if _, ok := gone.in[gone.ns]; ok {
			t.Fatalf("%s: an undesired namespace under our own prefix must still be pruned: %q survived", gone.what, gone.ns)
		}
	}
	for _, kept := range []struct {
		in  map[string][]backend.RuleGroup
		ns  string
		why string
	}{
		{mimirNow, "alerts-operator-other/default/x", "a longer prefix is a different owner"},
		{lokiNow, "alerts-operator-other_default_x", "a longer prefix is a different owner"},
		{mimirNow, "alerts-operator_default_mimir-shaped-loki-name", "mimir joins on \"/\", so an underscore name is not ours"},
		{lokiNow, "alerts-operator/default/loki-shaped-mimir-name", "loki joins on \"_\", so a slash name is not ours"},
	} {
		if _, ok := kept.in[kept.ns]; !ok {
			t.Fatalf("%q must never be pruned (%s); requests: %v", kept.ns, kept.why, s.Requests())
		}
	}
}

// TestTenantOwnershipConflictIsPerBackend: Mimir and Loki are separate targets. Two Tenants sharing
// a Mimir must not stop either of them pruning its own Loki, which nothing else claims.
func TestTenantOwnershipConflictIsPerBackend(t *testing.T) {
	s := fakebackend.New()
	t.Cleanup(s.Close)
	s.SetRules("1", "alerts-operator/default/stale-mimir", seedGroup("M"))
	s.SetLokiRules("1", "alerts-operator_default_stale-loki", []backend.RuleGroup{{Name: "g", Rules: []backend.Rule{{Alert: "L", Expr: `{job="x"} |= "e"`}}}})

	both := mimirTenantAt("both-backends", s.URL, "")
	both.Spec.Loki = &observabilityv1alpha1.BackendSpec{Address: s.URL}
	mimirOnly := mimirTenantAt("mimir-only-peer", s.URL, "") // shares Mimir, claims no Loki

	r := &TenantReconciler{Client: newChildrenFakeClient(t, both, mimirOnly), Recorder: record.NewFakeRecorder(20)}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: both.Name}}); err != nil {
		t.Fatal(err)
	}

	if _, ok := s.Rules("1")["alerts-operator/default/stale-mimir"]; !ok {
		t.Fatalf("the contested Mimir namespace must survive: %+v", s.Rules("1"))
	}
	if _, ok := s.LokiRules("1")["alerts-operator_default_stale-loki"]; ok {
		t.Fatalf("a Mimir collision must not block the uncontested Loki prune: %+v", s.LokiRules("1"))
	}
	var got observabilityv1alpha1.Tenant
	if err := r.Get(context.Background(), clientKey(both), &got); err != nil {
		t.Fatal(err)
	}
	if c := findCond(&got, observabilityv1alpha1.ConditionMimirRulesSynced); c.Status != metav1.ConditionFalse || c.Reason != observabilityv1alpha1.ReasonConflict {
		t.Fatalf("MimirRulesSynced should report the collision, got %+v", c)
	}
	if c := findCond(&got, observabilityv1alpha1.ConditionLokiRulesSynced); c.Status != metav1.ConditionTrue {
		t.Fatalf("LokiRulesSynced must be unaffected by a Mimir collision, got %+v", c)
	}
}
