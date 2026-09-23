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

// TestAlertRuleGroupBackendNamespaceIsPerBackend: status.backendNamespace is what an operator reads
// to find the group in the ruler, so it must be spelled with the separator that backend actually
// uses -- "/" on Mimir, "_" on Loki. A Loki value containing "/" would name a namespace Loki's
// router cannot address at all (alerts-operator-b4o).
func TestAlertRuleGroupBackendNamespaceIsPerBackend(t *testing.T) {
	newFakeTenant(t, "arg-tenant-ns", true, true) // both backends configured

	mimirARG := &observabilityv1alpha1.AlertRuleGroup{ObjectMeta: metav1.ObjectMeta{Name: "arg-ns-m", Namespace: "default"},
		Spec: observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: "arg-tenant-ns", Backend: "mimir", Groups: ruleGroups("up == 0")}}
	lokiARG := &observabilityv1alpha1.AlertRuleGroup{ObjectMeta: metav1.ObjectMeta{Name: "arg-ns-l", Namespace: "default"},
		Spec: observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: "arg-tenant-ns", Backend: "loki", Groups: ruleGroups(`{job="x"} |= "e"`)}}
	createAndCleanup(t, mimirARG)
	createAndCleanup(t, lokiARG)
	waitCondition(t, mimirARG, observabilityv1alpha1.ConditionAccepted, metav1.ConditionTrue, observabilityv1alpha1.ReasonAccepted)
	waitCondition(t, lokiARG, observabilityv1alpha1.ConditionAccepted, metav1.ConditionTrue, observabilityv1alpha1.ReasonAccepted)

	if got, want := mimirARG.Status.BackendNamespace, "alerts-operator/default/arg-ns-m"; got != want {
		t.Fatalf("mimir backendNamespace = %q, want %q", got, want)
	}
	if got, want := lokiARG.Status.BackendNamespace, "alerts-operator_default_arg-ns-l"; got != want {
		t.Fatalf("loki backendNamespace = %q, want %q", got, want)
	}
	if strings.Contains(lokiARG.Status.BackendNamespace, "/") {
		t.Fatalf("a Loki backendNamespace must never contain %q: %q", "/", lokiARG.Status.BackendNamespace)
	}
}

// TestAlertRuleGroupOversizedMessageTruncated guards against metav1.Condition's
// Message MaxLength=32768 (enforced by the generated CRD schema): rule.expr has no MaxLength
// of its own, and promql's parser echoes the whole offending input back into its error, so an
// oversized invalid expr would otherwise produce a status patch the API server rejects --
// leaving Accepted never recorded and the object stuck retrying forever.
func TestAlertRuleGroupOversizedMessageTruncated(t *testing.T) {
	newFakeTenant(t, "arg-tenant-oversized", true, false)

	huge := &observabilityv1alpha1.AlertRuleGroup{ObjectMeta: metav1.ObjectMeta{Name: "arg-oversized", Namespace: "default"},
		Spec: observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: "arg-tenant-oversized", Backend: "mimir",
			Groups: ruleGroups("up{job=" + strings.Repeat("x", maxConditionMessage))}}
	createAndCleanup(t, huge)
	waitCondition(t, huge, observabilityv1alpha1.ConditionAccepted, metav1.ConditionFalse, observabilityv1alpha1.ReasonInvalidRule)
	if c := findCond(huge, observabilityv1alpha1.ConditionAccepted); len(c.Message) > maxConditionMessage {
		t.Fatalf("condition message length %d exceeds max %d", len(c.Message), maxConditionMessage)
	}
}

func findCond(obj observabilityv1alpha1.Conditioned, typ string) metav1.Condition {
	for _, c := range obj.GetConditions() {
		if c.Type == typ {
			return c
		}
	}
	return metav1.Condition{}
}
