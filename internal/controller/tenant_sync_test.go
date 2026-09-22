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
	if len(rules["alerts-operator/default/sync-m"]) != 2 || len(lokiRules["alerts-operator/default/sync-l"]) != 1 || len(rules["other/x"]) != 1 {
		t.Fatalf("rules in backend: mimir=%+v loki=%+v", rules, lokiRules)
	}
	if _, leaked := rules["alerts-operator/default/sync-l"]; leaked {
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
	waitFor(t, func() bool { _, ok := srv.LokiRules("1")["alerts-operator/default/sync-l"]; return !ok })
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
