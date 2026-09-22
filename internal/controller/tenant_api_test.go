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

func TestTenantDefaults(t *testing.T) {
	tn := &observabilityv1alpha1.Tenant{}
	if tn.Prefix() != "alerts-operator" {
		t.Fatalf("prefix %q", tn.Prefix())
	}
	if tn.Resync().Minutes() != 5 {
		t.Fatalf("resync %v", tn.Resync())
	}
}
