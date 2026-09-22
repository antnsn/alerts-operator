package controller

import (
	"strings"
	"testing"

	observabilityv1alpha1 "github.com/antnsn/alerts-operator/api/v1alpha1"
)

func TestValidateRuleGroups(t *testing.T) {
	ok := []observabilityv1alpha1.RuleGroup{{Name: "g", Interval: "1m", Rules: []observabilityv1alpha1.Rule{{Alert: "A", Expr: `up{job="x"} == 0`, For: "5m", KeepFiringFor: "1h"}}}}
	if err := validateRuleGroups(observabilityv1alpha1.BackendMimir, ok); err != nil {
		t.Fatal(err)
	}
	bad := []observabilityv1alpha1.RuleGroup{{Name: "g", Rules: []observabilityv1alpha1.Rule{{Alert: "A", Expr: "up == 0"}, {Alert: "B", Expr: `up{job=`}}}}
	err := validateRuleGroups(observabilityv1alpha1.BackendMimir, bad)
	if err == nil || !strings.HasPrefix(err.Error(), "group g rule 1:") {
		t.Fatalf("expected rule index in message, got %v", err)
	}
	// LogQL is not parsed locally: a Loki group with the same expr passes.
	if err := validateRuleGroups(observabilityv1alpha1.BackendLoki, bad); err != nil {
		t.Fatalf("loki should skip expr parsing: %v", err)
	}
	badFor := []observabilityv1alpha1.RuleGroup{{Name: "g", Rules: []observabilityv1alpha1.Rule{{Alert: "A", Expr: "up", For: "soon"}}}}
	if err := validateRuleGroups(observabilityv1alpha1.BackendLoki, badFor); err == nil || !strings.Contains(err.Error(), "for") {
		t.Fatalf("bad duration: %v", err)
	}
	badInterval := []observabilityv1alpha1.RuleGroup{{Name: "g", Interval: "x", Rules: []observabilityv1alpha1.Rule{{Alert: "A", Expr: "up"}}}}
	if err := validateRuleGroups(observabilityv1alpha1.BackendMimir, badInterval); err == nil || !strings.HasPrefix(err.Error(), "group g: interval") {
		t.Fatalf("bad interval: %v", err)
	}
}
