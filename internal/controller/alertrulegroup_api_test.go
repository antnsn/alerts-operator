package controller

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	observabilityv1alpha1 "github.com/antnsn/alerts-operator/api/v1alpha1"
)

func TestAlertRuleGroupCEL(t *testing.T) {
	ok := observabilityv1alpha1.RuleGroup{Name: "g", Rules: []observabilityv1alpha1.Rule{{Alert: "A", Expr: "up == 0"}}}
	cases := []struct {
		name    string
		spec    observabilityv1alpha1.AlertRuleGroupSpec
		wantErr bool
	}{
		{"valid", observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: "t", Backend: "mimir", Groups: []observabilityv1alpha1.RuleGroup{ok}}, false},
		{"bad backend", observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: "t", Backend: "tempo", Groups: []observabilityv1alpha1.RuleGroup{ok}}, true},
		{"dup group", observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: "t", Backend: "loki", Groups: []observabilityv1alpha1.RuleGroup{ok, ok}}, true},
		{"rule without alert or record", observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: "t", Backend: "mimir", Groups: []observabilityv1alpha1.RuleGroup{{Name: "g", Rules: []observabilityv1alpha1.Rule{{Expr: "up"}}}}}, true},
		{"rule with both", observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: "t", Backend: "mimir", Groups: []observabilityv1alpha1.RuleGroup{{Name: "g", Rules: []observabilityv1alpha1.Rule{{Alert: "A", Record: "r", Expr: "up"}}}}}, true},
	}
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			obj := &observabilityv1alpha1.AlertRuleGroup{ObjectMeta: metav1.ObjectMeta{Name: "cel-" + string(rune('a'+i)), Namespace: "default"}, Spec: c.spec}
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
