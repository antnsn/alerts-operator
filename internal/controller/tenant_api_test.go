package controller

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	observabilityv1alpha1 "github.com/antnsn/alerts-operator/api/v1alpha1"
)

func TestTenantCEL(t *testing.T) {
	cases := []struct {
		name    string
		spec    observabilityv1alpha1.TenantSpec
		wantErr bool
	}{
		{"mimir only", observabilityv1alpha1.TenantSpec{TenantID: "1", Mimir: &observabilityv1alpha1.BackendSpec{Address: "http://m"}}, false},
		{"loki only", observabilityv1alpha1.TenantSpec{TenantID: "1", Loki: &observabilityv1alpha1.BackendSpec{Address: "http://l"}}, false},
		{"no backend", observabilityv1alpha1.TenantSpec{TenantID: "1"}, true},
		{"alertmanager without mimir", observabilityv1alpha1.TenantSpec{TenantID: "1", Loki: &observabilityv1alpha1.BackendSpec{Address: "http://l"}, Alertmanager: &observabilityv1alpha1.AlertmanagerSpec{}}, true},
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
