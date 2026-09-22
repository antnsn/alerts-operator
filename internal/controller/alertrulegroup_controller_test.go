package controller

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	observabilityv1alpha1 "github.com/antnsn/alerts-operator/api/v1alpha1"
)

func ruleGroups(expr string) []observabilityv1alpha1.RuleGroup {
	return []observabilityv1alpha1.RuleGroup{{Name: "g", Rules: []observabilityv1alpha1.Rule{{Alert: "A", Expr: expr}}}}
}

func TestAlertRuleGroupAccepted(t *testing.T) {
	newFakeTenant(t, "arg-tenant", true, false) // mimir only

	missing := &observabilityv1alpha1.AlertRuleGroup{ObjectMeta: metav1.ObjectMeta{Name: "arg-missing", Namespace: "default"},
		Spec: observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: "arg-nope", Backend: "mimir", Groups: ruleGroups("up == 0")}}
	createAndCleanup(t, missing)
	waitCondition(t, missing, observabilityv1alpha1.ConditionAccepted, metav1.ConditionFalse, observabilityv1alpha1.ReasonTenantNotFound)

	loki := &observabilityv1alpha1.AlertRuleGroup{ObjectMeta: metav1.ObjectMeta{Name: "arg-loki", Namespace: "default"},
		Spec: observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: "arg-tenant", Backend: "loki", Groups: ruleGroups(`{job="x"} |= "e"`)}}
	createAndCleanup(t, loki)
	waitCondition(t, loki, observabilityv1alpha1.ConditionAccepted, metav1.ConditionFalse, observabilityv1alpha1.ReasonBackendNotConfigured)

	bad := &observabilityv1alpha1.AlertRuleGroup{ObjectMeta: metav1.ObjectMeta{Name: "arg-bad", Namespace: "default"},
		Spec: observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: "arg-tenant", Backend: "mimir", Groups: ruleGroups(`up{job=`)}}
	createAndCleanup(t, bad)
	waitCondition(t, bad, observabilityv1alpha1.ConditionAccepted, metav1.ConditionFalse, observabilityv1alpha1.ReasonInvalidRule)
	if c := findCond(bad, observabilityv1alpha1.ConditionAccepted); !strings.Contains(c.Message, "group g rule 0") {
		t.Fatalf("message should name the rule: %q", c.Message)
	}

	good := &observabilityv1alpha1.AlertRuleGroup{ObjectMeta: metav1.ObjectMeta{Name: "arg-good", Namespace: "default"},
		Spec: observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: "arg-tenant", Backend: "mimir", Groups: ruleGroups("up == 0")}}
	createAndCleanup(t, good)
	waitCondition(t, good, observabilityv1alpha1.ConditionAccepted, metav1.ConditionTrue, observabilityv1alpha1.ReasonAccepted)
	if good.Status.BackendNamespace != "alerts-operator/default/arg-good" || good.Status.ObservedGeneration != good.Generation {
		t.Fatalf("status %+v", good.Status)
	}

	// Tenant appearing later flips TenantNotFound → Accepted.
	newFakeTenant(t, "arg-nope", true, false)
	waitCondition(t, missing, observabilityv1alpha1.ConditionAccepted, metav1.ConditionTrue, observabilityv1alpha1.ReasonAccepted)
}

func findCond(obj observabilityv1alpha1.Conditioned, typ string) metav1.Condition {
	for _, c := range obj.GetConditions() {
		if c.Type == typ {
			return c
		}
	}
	return metav1.Condition{}
}
