package controller

import (
	"testing"

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

// TestTenantRulesNamespacePrefixImmutable guards the CEL fix for a Codex review finding: syncRules
// only ever prunes backend rule namespaces under the Tenant's *current* prefix (see the ownership
// comment in tenant_rules.go), so changing spec.rulesNamespacePrefix after creation would silently
// orphan every namespace already written under the old one. Rather than tracking the previously
// applied prefix, the field is made immutable via CEL: creating with any valid value (including
// leaving it unset) is still fine, only changing it afterward is rejected.
func TestTenantRulesNamespacePrefixImmutable(t *testing.T) {
	srv := fake.New()
	defer srv.Close()

	create := func(t *testing.T, name string, spec observabilityv1alpha1.TenantSpec) *observabilityv1alpha1.Tenant {
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

	t.Run("explicit value cannot change", func(t *testing.T) {
		tn := create(t, "cel-prefix-explicit", observabilityv1alpha1.TenantSpec{
			TenantID: "1", Mimir: &observabilityv1alpha1.BackendSpec{Address: srv.URL}, RulesNamespacePrefix: "custom",
		})
		tn.Spec.RulesNamespacePrefix = "changed"
		if err := testClient.Update(testCtx, tn); err == nil {
			t.Fatal("changing an explicit rulesNamespacePrefix after creation must be rejected")
		}
	})

	// The gap this covers: a "self == oldSelf" transition rule alone is not evaluated against a
	// field absent on the old object, so without +kubebuilder:default a Tenant created with the
	// field unset (Prefix() falls back to "alerts-operator", and rules land under that prefix) could
	// later have it set for the first time -- silently orphaning every alerts-operator/* namespace,
	// exactly the defect this immutability marker exists to prevent. +kubebuilder:default makes the
	// field always materialise as "alerts-operator" in the stored object, so oldSelf always exists.
	t.Run("unset-then-set is rejected, not just set-then-changed", func(t *testing.T) {
		tn := create(t, "cel-prefix-unset", observabilityv1alpha1.TenantSpec{
			TenantID: "1", Mimir: &observabilityv1alpha1.BackendSpec{Address: srv.URL}, // RulesNamespacePrefix left unset
		})
		if err := testClient.Get(testCtx, client.ObjectKeyFromObject(tn), tn); err != nil {
			t.Fatal(err)
		}
		if tn.Spec.RulesNamespacePrefix != "alerts-operator" {
			t.Fatalf("expected the default to be materialised on create, got %q", tn.Spec.RulesNamespacePrefix)
		}
		tn.Spec.RulesNamespacePrefix = "foo"
		if err := testClient.Update(testCtx, tn); err == nil {
			t.Fatal("setting a previously-unset rulesNamespacePrefix must be rejected")
		}
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
