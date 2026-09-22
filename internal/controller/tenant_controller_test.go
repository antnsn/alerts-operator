package controller

import (
	"context"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	observabilityv1alpha1 "github.com/antnsn/alerts-operator/api/v1alpha1"
	"github.com/antnsn/alerts-operator/internal/compile"
	"github.com/antnsn/alerts-operator/internal/index"
)

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

// TestTenantListChildrenStaleGeneration covers listChildren's stale-generation branch: a child
// whose Accepted condition is missing, or lags its current generation, must NOT be treated as
// desired (excluded from the returned slices, same as a validated rejection) but must also NOT be
// treated as no-longer-desired (an AlertRuleGroup's backend namespace lands in KeepNamespaces
// instead of being pruned; a ContactPoint/NotificationPolicy sets PendingAM instead of letting the
// Alertmanager write proceed on stale data).
//
// This uses an isolated fake client (not testClient/testCacheClient) seeded with objects whose
// generation/status are set directly, rather than racing the real envtest reconcilers: those
// reconcilers are live and watching every ContactPoint/NotificationPolicy/AlertRuleGroup in the
// shared suite, so a spec edit meant to produce a transient "stale" window would be reconciled
// away asynchronously — not deterministically observable. Seeding a fake client with the desired
// generation/condition combination directly tests listChildren's classification without that race.
func TestTenantListChildrenStaleGeneration(t *testing.T) {
	tn := &observabilityv1alpha1.Tenant{ObjectMeta: metav1.ObjectMeta{Name: "tn-stale"}, Spec: observabilityv1alpha1.TenantSpec{TenantID: "1"}}

	acceptedAt := func(gen int64) []metav1.Condition {
		return []metav1.Condition{{Type: observabilityv1alpha1.ConditionAccepted, Status: metav1.ConditionTrue, Reason: observabilityv1alpha1.ReasonAccepted, ObservedGeneration: gen}}
	}

	// Accepted-current: Generation == the Accepted condition's ObservedGeneration. Included normally.
	currentARG := &observabilityv1alpha1.AlertRuleGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "current-arg", Namespace: "default", Generation: 1},
		Spec:       observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: tn.Name, Backend: observabilityv1alpha1.BackendMimir, Groups: ruleGroups("up == 0")},
		Status:     observabilityv1alpha1.AlertRuleGroupStatus{Conditions: acceptedAt(1)},
	}
	// Stale: a spec edit bumped Generation to 2 but this AlertRuleGroup's own reconciler hasn't
	// re-validated it yet (ObservedGeneration still 1). Must be kept, not pruned.
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
	currentNP := &observabilityv1alpha1.NotificationPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "current-pol", Namespace: "default", Generation: 1},
		Spec:       observabilityv1alpha1.NotificationPolicySpec{TenantRef: tn.Name, Route: observabilityv1alpha1.Route{Receiver: "current-cp"}},
		Status:     observabilityv1alpha1.NotificationPolicyStatus{Conditions: acceptedAt(1)},
	}
	staleNP := &observabilityv1alpha1.NotificationPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "stale-pol", Namespace: "default", Generation: 2},
		Spec:       observabilityv1alpha1.NotificationPolicySpec{TenantRef: tn.Name, Route: observabilityv1alpha1.Route{Receiver: "stale-cp"}},
		Status:     observabilityv1alpha1.NotificationPolicyStatus{Conditions: acceptedAt(1)},
	}

	fc := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithIndex(&observabilityv1alpha1.ContactPoint{}, index.IndexTenantRef, func(o client.Object) []string {
			return []string{o.(*observabilityv1alpha1.ContactPoint).Spec.TenantRef}
		}).
		WithIndex(&observabilityv1alpha1.NotificationPolicy{}, index.IndexTenantRef, func(o client.Object) []string {
			return []string{o.(*observabilityv1alpha1.NotificationPolicy).Spec.TenantRef}
		}).
		WithIndex(&observabilityv1alpha1.AlertRuleGroup{}, index.IndexTenantRef, func(o client.Object) []string {
			return []string{o.(*observabilityv1alpha1.AlertRuleGroup).Spec.TenantRef}
		}).
		WithObjects(currentARG, staleARG, missingARG, currentCP, staleCP, currentNP, staleNP).
		Build()

	r := &TenantReconciler{Client: fc}
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
	if len(ch.ContactPoints) != 1 || ch.ContactPoints[0].Name != "current-cp" {
		t.Fatalf("accepted-current ContactPoint should be the only one included: %+v", ch.ContactPoints)
	}
	if ch.Policy == nil || ch.Policy.Name != "current-pol" {
		t.Fatalf("accepted-current NotificationPolicy should win: %+v", ch.Policy)
	}
	if !ch.PendingAM {
		t.Fatalf("a stale ContactPoint or NotificationPolicy must set PendingAM so the Alertmanager write is deferred")
	}
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

	t.Run("wrong namespace is not resolved", func(t *testing.T) {
		// "kube-system" is bootstrapped by the API server itself (unlike a namespace we'd create
		// and delete ourselves, which envtest never finishes terminating, since no namespace
		// controller runs there): a Secret placed in it, referenced with a different Namespace in
		// BasicAuthSecretRef, proves ref.Namespace is what's actually read from, not the Secret's
		// real namespace or some hardcoded default.
		sec := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "bo-shared-name", Namespace: "kube-system"},
			Data:       map[string][]byte{"username": []byte("alice"), "password": []byte("s3cr3t")},
		}
		createAndCleanup(t, sec)
		spec := &observabilityv1alpha1.BackendSpec{Address: "http://backend", Auth: &observabilityv1alpha1.BackendAuth{
			BasicAuthSecretRef: &observabilityv1alpha1.NamespacedName{Namespace: "default", Name: "bo-shared-name"},
		}}
		_, err := r.backendOptions(testCtx, tn, spec)
		if err == nil || !strings.Contains(err.Error(), "not found") {
			t.Fatalf("secret in the wrong namespace must not resolve: %v", err)
		}
	})
}
