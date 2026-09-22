package controller

import (
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	observabilityv1alpha1 "github.com/antnsn/alerts-operator/api/v1alpha1"
	"github.com/antnsn/alerts-operator/internal/backend/fake"
)

func TestTenantCEL(t *testing.T) {
	srv := fake.New()
	defer srv.Close()
	cases := []struct {
		name    string
		spec    observabilityv1alpha1.TenantSpec
		wantErr bool
	}{
		{"mimir only", observabilityv1alpha1.TenantSpec{TenantID: "1", Mimir: &observabilityv1alpha1.BackendSpec{Address: srv.URL}}, false},
		{"loki only", observabilityv1alpha1.TenantSpec{TenantID: "1", Loki: &observabilityv1alpha1.BackendSpec{Address: srv.URL}}, false},
		{"no backend", observabilityv1alpha1.TenantSpec{TenantID: "1"}, true},
		{"alertmanager without mimir", observabilityv1alpha1.TenantSpec{TenantID: "1", Loki: &observabilityv1alpha1.BackendSpec{Address: srv.URL}, Alertmanager: &observabilityv1alpha1.AlertmanagerSpec{}}, true},
	}
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			obj := &observabilityv1alpha1.Tenant{ObjectMeta: metav1.ObjectMeta{Name: "cel-" + string(rune('a'+i))}, Spec: c.spec}
			err := testClient.Create(testCtx, obj)
			if (err != nil) != c.wantErr {
				t.Fatalf("wantErr=%v got %v", c.wantErr, err)
			}
			if err == nil {
				_ = testClient.Delete(testCtx, obj)
				waitFor(t, func() bool {
					return errors.IsNotFound(testClient.Get(testCtx, client.ObjectKeyFromObject(obj), &observabilityv1alpha1.Tenant{}))
				})
			}
		})
	}
}

// createTenant creates a Tenant and removes it (waiting for the delete to land) at test end.
func createTenant(t *testing.T, name string, spec observabilityv1alpha1.TenantSpec) *observabilityv1alpha1.Tenant {
	t.Helper()
	tn := &observabilityv1alpha1.Tenant{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: spec}
	if err := testClient.Create(testCtx, tn); err != nil {
		t.Fatalf("create must succeed: %v", err)
	}
	t.Cleanup(func() {
		_ = testClient.Delete(testCtx, tn)
		waitFor(t, func() bool {
			return errors.IsNotFound(testClient.Get(testCtx, client.ObjectKeyFromObject(tn), &observabilityv1alpha1.Tenant{}))
		})
	})
	return tn
}

// updateAwaitingVerdict re-reads the Tenant, applies mutate and Updates, retrying while the API
// server answers with a 409 conflict, and returns the first non-conflict verdict.
//
// Without this retry an immutability test does not test immutability (P2-2 of the Task 19 review):
// the live TenantReconciler in the shared envtest suite adds the finalizer and patches status
// immediately after Create, so the Update frequently loses the optimistic lock and returns "the
// object has been modified" -- an error, which satisfies a bare `err == nil { t.Fatal }` assertion
// whether or not the CEL rule exists. Measured: with the x-kubernetes-validations block deleted
// from the CRD the old test still passed 2 of 5 runs, and its unset-then-set subtest never reached
// CEL at all -- it saw the 409 every single time, rule present or absent.
func updateAwaitingVerdict(t *testing.T, tn *observabilityv1alpha1.Tenant, mutate func(*observabilityv1alpha1.Tenant)) error {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var cur observabilityv1alpha1.Tenant
		if err := testClient.Get(testCtx, client.ObjectKeyFromObject(tn), &cur); err != nil {
			t.Fatalf("get before update: %v", err)
		}
		mutate(&cur)
		err := testClient.Update(testCtx, &cur)
		if !errors.IsConflict(err) {
			return err
		}
		if time.Now().After(deadline) {
			t.Fatalf("update never got past an optimistic-lock conflict: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// assertCELRejected asserts the update was rejected by CEL specifically -- an Invalid status error
// carrying the rule's own message -- rather than by anything that merely happens to be an error.
func assertCELRejected(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("the update must be rejected with %q, got no error", want)
	}
	if !errors.IsInvalid(err) || !strings.Contains(err.Error(), want) {
		t.Fatalf("expected an Invalid rejection containing %q, got %T %v", want, err, err)
	}
}

// TestTenantRulesNamespacePrefixImmutable guards the CEL fix for a Codex review finding: syncRules
// only ever prunes backend rule namespaces under the Tenant's *current* prefix (see the ownership
// comment in tenant_rules.go), so changing spec.rulesNamespacePrefix after creation would silently
// orphan every namespace already written under the old one. Rather than tracking the previously
// applied prefix, the field is made immutable via CEL: creating with any valid value (including
// leaving it unset) is still fine, only changing it afterward is rejected.
func TestTenantRulesNamespacePrefixImmutable(t *testing.T) {
	srv := fake.New()
	defer srv.Close()
	const immutableMsg = "rulesNamespacePrefix is immutable"

	t.Run("explicit value cannot change", func(t *testing.T) {
		tn := createTenant(t, "cel-prefix-explicit", observabilityv1alpha1.TenantSpec{
			TenantID: "1", Mimir: &observabilityv1alpha1.BackendSpec{Address: srv.URL}, RulesNamespacePrefix: "custom",
		})
		err := updateAwaitingVerdict(t, tn, func(cur *observabilityv1alpha1.Tenant) {
			cur.Spec.RulesNamespacePrefix = "changed"
		})
		assertCELRejected(t, err, immutableMsg)
	})

	// The gap this covers: a "self == oldSelf" transition rule alone is not evaluated against a
	// field absent on the old object, so without +kubebuilder:default a Tenant created with the
	// field unset (Prefix() falls back to "alerts-operator", and rules land under that prefix) could
	// later have it set for the first time -- silently orphaning every alerts-operator/* namespace,
	// exactly the defect this immutability marker exists to prevent. +kubebuilder:default makes the
	// field always materialise as "alerts-operator" in the stored object, so oldSelf always exists.
	t.Run("unset-then-set is rejected, not just set-then-changed", func(t *testing.T) {
		tn := createTenant(t, "cel-prefix-unset", observabilityv1alpha1.TenantSpec{
			TenantID: "1", Mimir: &observabilityv1alpha1.BackendSpec{Address: srv.URL}, // RulesNamespacePrefix left unset
		})
		if err := testClient.Get(testCtx, client.ObjectKeyFromObject(tn), tn); err != nil {
			t.Fatal(err)
		}
		if tn.Spec.RulesNamespacePrefix != "alerts-operator" {
			t.Fatalf("expected the default to be materialised on create, got %q", tn.Spec.RulesNamespacePrefix)
		}
		err := updateAwaitingVerdict(t, tn, func(cur *observabilityv1alpha1.Tenant) {
			cur.Spec.RulesNamespacePrefix = "foo"
		})
		assertCELRejected(t, err, immutableMsg)
	})
}

// TestTenantIDImmutable covers P2-1 of the Task 19 review: spec.tenantId has the identical
// orphaning property as rulesNamespacePrefix, and was still mutable. It is sent as X-Scope-OrgID on
// every backend call, so editing it from "1" to "2" makes the next reconcile list org 2 (empty),
// write everything there, and never look at org 1 again -- the org-1 rule namespaces and
// Alertmanager config stay live and keep firing, unreachable by the prune loop and by Task 20's
// finalizer, so even deleting the Tenant does not recover them.
//
// Unlike rulesNamespacePrefix this needs no +kubebuilder:default to close the unset-then-set hole:
// tenantId is required with MinLength=1 (confirmed in the generated CRD's `required` list), so it
// is always present in the stored object and oldSelf always exists.
func TestTenantIDImmutable(t *testing.T) {
	srv := fake.New()
	defer srv.Close()

	// Run createTenant's Delete-and-wait cleanup inside a subtest (mirroring
	// TestTenantRulesNamespacePrefixImmutable above): t.Run blocks until the subtest and its
	// Cleanup funcs finish, so the Tenant is gone -- and Task 20's finalizer has had a chance to
	// reach the backend -- before the outer defer closes srv. Registering createTenant's Cleanup
	// directly on the outer t would instead run it after that defer, against an already-closed
	// server, and the finalizer would never complete, hanging Delete's own wait.
	t.Run("tenantId cannot change", func(t *testing.T) {
		tn := createTenant(t, "cel-tenantid", observabilityv1alpha1.TenantSpec{
			TenantID: "1", Mimir: &observabilityv1alpha1.BackendSpec{Address: srv.URL}, RulesNamespacePrefix: "tenantid-test",
		})
		err := updateAwaitingVerdict(t, tn, func(cur *observabilityv1alpha1.Tenant) { cur.Spec.TenantID = "2" })
		assertCELRejected(t, err, "tenantId is immutable")
	})
}

func TestTenantDefaults(t *testing.T) {
	tn := &observabilityv1alpha1.Tenant{}
	if tn.Prefix() != "alerts-operator" {
		t.Fatalf("prefix %q", tn.Prefix())
	}
	if tn.Resync().Minutes() != 5 {
		t.Fatalf("resync %v", tn.Resync())
	}
}
