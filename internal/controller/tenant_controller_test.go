package controller

import (
	"context"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	observabilityv1alpha1 "github.com/antnsn/alerts-operator/api/v1alpha1"
	"github.com/antnsn/alerts-operator/internal/backend"
	"github.com/antnsn/alerts-operator/internal/compile"
	"github.com/antnsn/alerts-operator/internal/index"
)

// stubMimirClient is a no-op MimirClient: List reports no rules, everything else is unreachable in
// the tests that use it (a stale-generation child defers the Alertmanager write before any backend
// call, and no AlertRuleGroup is seeded). It exists purely to let Reconcile run to completion
// without a real Mimir behind it, for tests about status aggregation rather than backend I/O.
type stubMimirClient struct{}

func (stubMimirClient) List(context.Context) (map[string][]backend.RuleGroup, error) {
	return map[string][]backend.RuleGroup{}, nil
}
func (stubMimirClient) SetGroup(context.Context, string, backend.RuleGroup) error { return nil }
func (stubMimirClient) DeleteGroup(context.Context, string, string) error         { return nil }
func (stubMimirClient) DeleteNamespace(context.Context, string) error             { return nil }
func (stubMimirClient) Get(context.Context) (*backend.AlertmanagerConfig, error)  { return nil, nil }
func (stubMimirClient) Set(context.Context, *backend.AlertmanagerConfig) error    { return nil }
func (stubMimirClient) Delete(context.Context) error                              { return nil }

// TestTenantReadyOnFirstReconcileFallsBackToPending covers the P3-1 finding carried from the Task 18
// review: Ready's aggregation preserves a pending target's PRIOR Ready value when some condition is
// Unknown/Pending (see the "pending" block in Reconcile) -- but a Tenant's very first reconcile has
// no prior Ready condition to fall back to. Left unhandled, Go's zero-value default for the local
// (True/Synced) would silently report readiness this pass never established. It must report
// Unknown/Pending instead -- the same "we don't know yet" the pending target itself is reporting.
//
// Built on the isolated newChildrenFakeClient fixture (see TestTenantKeepsStaleGenerationNamespace):
// this is about Reconcile's in-memory aggregation, not backend I/O, and a stale-generation child
// created via the live envtest suite would race its own reconciler re-validating it away almost
// immediately, the same problem that test ran into.
func TestTenantReadyOnFirstReconcileFallsBackToPending(t *testing.T) {
	tn := &observabilityv1alpha1.Tenant{
		ObjectMeta: metav1.ObjectMeta{Name: "tn-first-pending", Finalizers: []string{tenantFinalizer}},
		Spec:       observabilityv1alpha1.TenantSpec{TenantID: "1", Mimir: &observabilityv1alpha1.BackendSpec{Address: "http://stub"}},
	}
	// Stale-generation ContactPoint: listChildren sets PendingAM, so syncAlertmanager reports
	// AlertmanagerSynced=Unknown/Pending without ever touching a backend.
	staleCP := &observabilityv1alpha1.ContactPoint{
		ObjectMeta: metav1.ObjectMeta{Name: "stale-cp", Namespace: "default", Generation: 2},
		Spec:       observabilityv1alpha1.ContactPointSpec{TenantRef: tn.Name, Webhook: []observabilityv1alpha1.WebhookConfig{{URL: "http://x"}}},
		Status:     observabilityv1alpha1.ContactPointStatus{Conditions: acceptedAt(1)},
	}

	r := &TenantReconciler{
		Client:   newChildrenFakeClient(t, tn, staleCP),
		Recorder: record.NewFakeRecorder(20),
		NewMimir: func(backend.Options) MimirClient { return stubMimirClient{} },
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: tn.Name}}); err != nil {
		t.Fatal(err)
	}

	var got observabilityv1alpha1.Tenant
	if err := r.Get(context.Background(), types.NamespacedName{Name: tn.Name}, &got); err != nil {
		t.Fatal(err)
	}
	if ready := findCond(&got, observabilityv1alpha1.ConditionReady); ready.Status != metav1.ConditionUnknown || ready.Reason != observabilityv1alpha1.ReasonPending {
		t.Fatalf("first reconcile with a pending child must report Ready=Unknown/Pending (no prior value to fall back to), got %+v", ready)
	}
}

func TestTenantReadyWithoutChildren(t *testing.T) {
	// Loki-only: no Alertmanager, so Ready does not depend on a NotificationPolicy.
	tn, _ := newFakeTenant(t, "tn-loki-only", false, true)
	waitCondition(t, tn, observabilityv1alpha1.ConditionReady, metav1.ConditionTrue, "")
	if !controllerutil.ContainsFinalizer(tn, tenantFinalizer) {
		t.Fatalf("finalizer missing: %v", tn.Finalizers)
	}
	if tn.Status.ObservedGeneration != tn.Generation {
		t.Fatalf("observedGeneration %d != %d", tn.Status.ObservedGeneration, tn.Generation)
	}
	mimirCond := findCond(tn, observabilityv1alpha1.ConditionMimirRulesSynced)
	if mimirCond.Status != metav1.ConditionUnknown || mimirCond.Reason != observabilityv1alpha1.ReasonNotConfigured {
		t.Fatalf("unconfigured backend should be Unknown/NotConfigured: %+v", mimirCond)
	}
	amCond := findCond(tn, observabilityv1alpha1.ConditionAlertmanagerSynced)
	if amCond.Status != metav1.ConditionUnknown || amCond.Reason != observabilityv1alpha1.ReasonNotConfigured {
		t.Fatalf("unconfigured alertmanager should be Unknown/NotConfigured: %+v", amCond)
	}
}

func TestTenantListChildrenFiltersAccepted(t *testing.T) {
	tn, _ := newFakeTenant(t, "tn-children", true, true)
	good := &observabilityv1alpha1.AlertRuleGroup{ObjectMeta: metav1.ObjectMeta{Name: "tn-good", Namespace: "default"},
		Spec: observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: tn.Name, Backend: "mimir", Groups: ruleGroups("up == 0")}}
	bad := &observabilityv1alpha1.AlertRuleGroup{ObjectMeta: metav1.ObjectMeta{Name: "tn-bad", Namespace: "default"},
		Spec: observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: tn.Name, Backend: "mimir", Groups: ruleGroups("up{")}}
	lokiG := &observabilityv1alpha1.AlertRuleGroup{ObjectMeta: metav1.ObjectMeta{Name: "tn-loki", Namespace: "default"},
		Spec: observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: tn.Name, Backend: "loki", Groups: ruleGroups(`{a="b"}`)}}
	cp := &observabilityv1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "tn-cp", Namespace: "default"},
		Spec: observabilityv1alpha1.ContactPointSpec{TenantRef: tn.Name, Webhook: []observabilityv1alpha1.WebhookConfig{{URL: "http://x"}}}}
	pol := &observabilityv1alpha1.NotificationPolicy{ObjectMeta: metav1.ObjectMeta{Name: "tn-pol", Namespace: "default"},
		Spec: observabilityv1alpha1.NotificationPolicySpec{TenantRef: tn.Name, Route: observabilityv1alpha1.Route{Receiver: "tn-cp"}}}
	for _, o := range []observabilityv1alpha1.Conditioned{good, bad, lokiG, cp, pol} {
		createAndCleanup(t, o)
	}
	waitCondition(t, good, observabilityv1alpha1.ConditionAccepted, metav1.ConditionTrue, "")
	waitCondition(t, bad, observabilityv1alpha1.ConditionAccepted, metav1.ConditionFalse, "")
	waitCondition(t, lokiG, observabilityv1alpha1.ConditionAccepted, metav1.ConditionTrue, "")
	waitCondition(t, pol, observabilityv1alpha1.ConditionAccepted, metav1.ConditionTrue, "")

	// listChildren queries via client.MatchingFields (index.IndexTenantRef), which is a
	// controller-runtime cache feature: it must go through the cache-backed testCacheClient,
	// not the uncached testClient (see suite_test.go), and cache reads are eventually
	// consistent with the writes just observed via waitCondition above, hence waitFor.
	r := &TenantReconciler{Client: testCacheClient}
	var ch *children
	waitFor(t, func() bool {
		var err error
		ch, err = r.listChildren(testCtx, tn)
		return err == nil && len(ch.MimirGroups) == 1 && len(ch.LokiGroups) == 1 && len(ch.ContactPoints) == 1 && ch.Policy != nil
	})
	if ch.MimirGroups[0].Name != "tn-good" {
		t.Fatalf("mimir groups: %+v", ch.MimirGroups)
	}
	if ch.Policy.Name != "tn-pol" {
		t.Fatalf("children: %+v", ch)
	}
}

// acceptedAt builds a single Accepted=True condition observed at the given generation, for
// constructing fake-client fixtures directly (see TestTenantListChildrenStaleGeneration).
func acceptedAt(gen int64) []metav1.Condition {
	return []metav1.Condition{{Type: observabilityv1alpha1.ConditionAccepted, Status: metav1.ConditionTrue, Reason: observabilityv1alpha1.ReasonAccepted, ObservedGeneration: gen}}
}

// newChildrenFakeClient builds an isolated (non-envtest) fake client seeded with objs, indexed on
// index.IndexTenantRef exactly like the production manager's cache. Used instead of
// testClient/testCacheClient because the real envtest reconcilers are live and watching every
// ContactPoint/NotificationPolicy/AlertRuleGroup in the shared suite: a spec edit meant to produce
// a transient "stale generation" window would be reconciled away asynchronously and is not
// deterministically observable. Seeding a fake client with the desired generation/condition
// combination directly tests listChildren's classification without that race.
//
// WithStatusSubresource is required for every Conditioned kind: without it, the fake client's
// Status() sub-writer fails every Get/Patch/Update with a bare "<kind> not found", even though the
// object is right there — the fake client keeps status subresource state in a separate store that
// only exists for types registered this way (confirmed by hand: TestTenantKeepsStaleGenerationNamespace
// needs this to drive r.Reconcile, which patches Tenant/AlertRuleGroup status, against this fixture).
func newChildrenFakeClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithStatusSubresource(&observabilityv1alpha1.Tenant{}, &observabilityv1alpha1.ContactPoint{},
			&observabilityv1alpha1.NotificationPolicy{}, &observabilityv1alpha1.AlertRuleGroup{}).
		WithIndex(&observabilityv1alpha1.ContactPoint{}, index.IndexTenantRef, func(o client.Object) []string {
			return []string{o.(*observabilityv1alpha1.ContactPoint).Spec.TenantRef}
		}).
		WithIndex(&observabilityv1alpha1.NotificationPolicy{}, index.IndexTenantRef, func(o client.Object) []string {
			return []string{o.(*observabilityv1alpha1.NotificationPolicy).Spec.TenantRef}
		}).
		WithIndex(&observabilityv1alpha1.AlertRuleGroup{}, index.IndexTenantRef, func(o client.Object) []string {
			return []string{o.(*observabilityv1alpha1.AlertRuleGroup).Spec.TenantRef}
		}).
		WithObjects(objs...).
		Build()
}

// TestTenantListChildrenStaleGeneration covers listChildren's stale-generation branch: a child
// whose Accepted condition is missing, or lags its current generation, must NOT be treated as
// desired (excluded from the returned slices, same as a validated rejection) but must also NOT be
// treated as no-longer-desired (an AlertRuleGroup's backend namespace lands in KeepNamespaces
// instead of being pruned; a ContactPoint/NotificationPolicy sets PendingAM instead of letting the
// Alertmanager write proceed on stale data).
//
// Each stale source (AlertRuleGroup / ContactPoint / NotificationPolicy) gets its own subtest with
// only that kind's stale object present: a single scenario with both a stale ContactPoint and a
// stale NotificationPolicy would still show PendingAM=true even if listChildren stopped setting it
// for one of the two kinds, since the other would mask the regression.
func TestTenantListChildrenStaleGeneration(t *testing.T) {
	tn := &observabilityv1alpha1.Tenant{ObjectMeta: metav1.ObjectMeta{Name: "tn-stale"}, Spec: observabilityv1alpha1.TenantSpec{TenantID: "1"}}

	t.Run("alert rule groups", func(t *testing.T) {
		// Accepted-current: Generation == the Accepted condition's ObservedGeneration. Included normally.
		currentARG := &observabilityv1alpha1.AlertRuleGroup{
			ObjectMeta: metav1.ObjectMeta{Name: "current-arg", Namespace: "default", Generation: 1},
			Spec:       observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: tn.Name, Backend: observabilityv1alpha1.BackendMimir, Groups: ruleGroups("up == 0")},
			Status:     observabilityv1alpha1.AlertRuleGroupStatus{Conditions: acceptedAt(1)},
		}
		// Stale: a spec edit bumped Generation to 2 but this AlertRuleGroup's own reconciler
		// hasn't re-validated it yet (ObservedGeneration still 1). Must be kept, not pruned.
		staleARG := &observabilityv1alpha1.AlertRuleGroup{
			ObjectMeta: metav1.ObjectMeta{Name: "stale-arg", Namespace: "default", Generation: 2},
			Spec:       observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: tn.Name, Backend: observabilityv1alpha1.BackendMimir, Groups: ruleGroups("up == 0")},
			Status:     observabilityv1alpha1.AlertRuleGroupStatus{Conditions: acceptedAt(1)},
		}
		// Stale via the other disjunct: no Accepted condition at all (never reconciled).
		missingARG := &observabilityv1alpha1.AlertRuleGroup{
			ObjectMeta: metav1.ObjectMeta{Name: "missing-arg", Namespace: "default", Generation: 1},
			Spec:       observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: tn.Name, Backend: observabilityv1alpha1.BackendMimir, Groups: ruleGroups("up == 0")},
		}

		r := &TenantReconciler{Client: newChildrenFakeClient(t, currentARG, staleARG, missingARG)}
		ch, err := r.listChildren(context.Background(), tn)
		if err != nil {
			t.Fatal(err)
		}
		if len(ch.MimirGroups) != 1 || ch.MimirGroups[0].Name != "current-arg" {
			t.Fatalf("accepted-current AlertRuleGroup should be the only one included: %+v", ch.MimirGroups)
		}
		wantKeep := map[string]bool{
			compile.BackendNamespace(tn.Prefix(), "default", "stale-arg"):   true,
			compile.BackendNamespace(tn.Prefix(), "default", "missing-arg"): true,
		}
		if !reflect.DeepEqual(ch.KeepNamespaces, wantKeep) {
			t.Fatalf("KeepNamespaces = %+v, want %+v (stale/missing AlertRuleGroups must be kept, not pruned)", ch.KeepNamespaces, wantKeep)
		}
		if ch.PendingAM {
			t.Fatalf("a stale AlertRuleGroup must not set PendingAM (that's only for ContactPoint/NotificationPolicy)")
		}
	})

	t.Run("stale contact point sets PendingAM", func(t *testing.T) {
		currentCP := &observabilityv1alpha1.ContactPoint{
			ObjectMeta: metav1.ObjectMeta{Name: "current-cp", Namespace: "default", Generation: 1},
			Spec:       observabilityv1alpha1.ContactPointSpec{TenantRef: tn.Name, Webhook: []observabilityv1alpha1.WebhookConfig{{URL: "http://x"}}},
			Status:     observabilityv1alpha1.ContactPointStatus{Conditions: acceptedAt(1)},
		}
		staleCP := &observabilityv1alpha1.ContactPoint{
			ObjectMeta: metav1.ObjectMeta{Name: "stale-cp", Namespace: "default", Generation: 2},
			Spec:       observabilityv1alpha1.ContactPointSpec{TenantRef: tn.Name, Webhook: []observabilityv1alpha1.WebhookConfig{{URL: "http://x"}}},
			Status:     observabilityv1alpha1.ContactPointStatus{Conditions: acceptedAt(1)},
		}

		r := &TenantReconciler{Client: newChildrenFakeClient(t, currentCP, staleCP)}
		ch, err := r.listChildren(context.Background(), tn)
		if err != nil {
			t.Fatal(err)
		}
		if len(ch.ContactPoints) != 1 || ch.ContactPoints[0].Name != "current-cp" {
			t.Fatalf("accepted-current ContactPoint should be the only one included: %+v", ch.ContactPoints)
		}
		if !ch.PendingAM {
			t.Fatalf("a stale ContactPoint alone must set PendingAM so the Alertmanager write is deferred")
		}
	})

	t.Run("stale notification policy sets PendingAM", func(t *testing.T) {
		currentNP := &observabilityv1alpha1.NotificationPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "current-pol", Namespace: "default", Generation: 1},
			Spec:       observabilityv1alpha1.NotificationPolicySpec{TenantRef: tn.Name, Route: observabilityv1alpha1.Route{Receiver: "x"}},
			Status:     observabilityv1alpha1.NotificationPolicyStatus{Conditions: acceptedAt(1)},
		}
		staleNP := &observabilityv1alpha1.NotificationPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "stale-pol", Namespace: "default", Generation: 2},
			Spec:       observabilityv1alpha1.NotificationPolicySpec{TenantRef: tn.Name, Route: observabilityv1alpha1.Route{Receiver: "x"}},
			Status:     observabilityv1alpha1.NotificationPolicyStatus{Conditions: acceptedAt(1)},
		}

		r := &TenantReconciler{Client: newChildrenFakeClient(t, currentNP, staleNP)}
		ch, err := r.listChildren(context.Background(), tn)
		if err != nil {
			t.Fatal(err)
		}
		if ch.Policy == nil || ch.Policy.Name != "current-pol" {
			t.Fatalf("accepted-current NotificationPolicy should win: %+v", ch.Policy)
		}
		if !ch.PendingAM {
			t.Fatalf("a stale NotificationPolicy alone must set PendingAM so the Alertmanager write is deferred")
		}
	})
}

// TestTenantBackendOptionsSecretResolution covers backendOptions's BasicAuthSecretRef resolution:
// success (values land in the returned Options), a missing Secret (clear error, not a silent
// unauthenticated fallback), a Secret missing a required key, and that the Secret is actually read
// from ref.Namespace (not, say, the Tenant's own namespace or some other implicit namespace).
func TestTenantBackendOptionsSecretResolution(t *testing.T) {
	r := &TenantReconciler{Client: testClient}
	tn := &observabilityv1alpha1.Tenant{Spec: observabilityv1alpha1.TenantSpec{TenantID: "42"}}

	t.Run("success", func(t *testing.T) {
		sec := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "bo-good", Namespace: "default"},
			Data:       map[string][]byte{"username": []byte("alice"), "password": []byte("s3cr3t")},
		}
		createAndCleanup(t, sec)
		spec := &observabilityv1alpha1.BackendSpec{Address: "http://backend", Auth: &observabilityv1alpha1.BackendAuth{
			BasicAuthSecretRef: &observabilityv1alpha1.NamespacedName{Namespace: "default", Name: "bo-good"},
		}}
		o, err := r.backendOptions(testCtx, tn, spec)
		if err != nil {
			t.Fatal(err)
		}
		if o.Address != "http://backend" || o.TenantID != "42" {
			t.Fatalf("options base fields not carried through: %+v", o)
		}
		if o.BasicAuth == nil || o.BasicAuth.Username != "alice" || o.BasicAuth.Password != "s3cr3t" {
			t.Fatalf("basic auth not resolved from the secret: %+v", o.BasicAuth)
		}
	})

	t.Run("secret not found", func(t *testing.T) {
		spec := &observabilityv1alpha1.BackendSpec{Address: "http://backend", Auth: &observabilityv1alpha1.BackendAuth{
			BasicAuthSecretRef: &observabilityv1alpha1.NamespacedName{Namespace: "default", Name: "bo-missing"},
		}}
		_, err := r.backendOptions(testCtx, tn, spec)
		if err == nil || !strings.Contains(err.Error(), "not found") {
			t.Fatalf("want a clear not-found error, got %v", err)
		}
	})

	t.Run("secret missing password key", func(t *testing.T) {
		sec := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "bo-partial", Namespace: "default"},
			Data:       map[string][]byte{"username": []byte("alice")},
		}
		createAndCleanup(t, sec)
		spec := &observabilityv1alpha1.BackendSpec{Address: "http://backend", Auth: &observabilityv1alpha1.BackendAuth{
			BasicAuthSecretRef: &observabilityv1alpha1.NamespacedName{Namespace: "default", Name: "bo-partial"},
		}}
		_, err := r.backendOptions(testCtx, tn, spec)
		if err == nil || !strings.Contains(err.Error(), "username and password") {
			t.Fatalf("want a clear missing-key error, got %v", err)
		}
	})

	t.Run("resolves from the referenced namespace, not another one with the same name", func(t *testing.T) {
		// Two Secrets with the SAME name but DIFFERENT data, in two different (pre-existing, so
		// nothing needs creating/cleaning up a Namespace object -- see the envtest note below)
		// namespaces. Referencing "kube-system" must come back with kube-system's data: if the
		// code ignored ref.Namespace and always read "default" (or the Secret's own eventual
		// namespace by some other means), this would silently return "default"'s credentials
		// instead and the test would catch it.
		//
		// "kube-system" is bootstrapped by the API server itself, unlike a namespace we'd create
		// via testClient.Create: envtest runs no namespace controller, so a namespace we deleted
		// ourselves would sit in Terminating forever and break a second run within the same
		// envtest instance (e.g. -count=2).
		defaultSec := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "bo-shared-name", Namespace: "default"},
			Data:       map[string][]byte{"username": []byte("default-user"), "password": []byte("default-pass")},
		}
		createAndCleanup(t, defaultSec)
		kubeSystemSec := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "bo-shared-name", Namespace: "kube-system"},
			Data:       map[string][]byte{"username": []byte("kube-system-user"), "password": []byte("kube-system-pass")},
		}
		createAndCleanup(t, kubeSystemSec)

		spec := &observabilityv1alpha1.BackendSpec{Address: "http://backend", Auth: &observabilityv1alpha1.BackendAuth{
			BasicAuthSecretRef: &observabilityv1alpha1.NamespacedName{Namespace: "kube-system", Name: "bo-shared-name"},
		}}
		o, err := r.backendOptions(testCtx, tn, spec)
		if err != nil {
			t.Fatal(err)
		}
		if o.BasicAuth == nil || o.BasicAuth.Username != "kube-system-user" || o.BasicAuth.Password != "kube-system-pass" {
			t.Fatalf("must resolve from the referenced namespace (kube-system), not default: %+v", o.BasicAuth)
		}
	})
}
