package compile

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	"github.com/antnsn/alerts-operator/api/v1alpha1"
	"github.com/antnsn/alerts-operator/internal/backend"
)

var update = flag.Bool("update", false, "rewrite golden files")

func golden(t *testing.T, name string, got []byte) {
	t.Helper()
	p := filepath.Join("testdata", name)
	if *update {
		if err := os.WriteFile(p, got, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read golden (run with -update first): %v", err)
	}
	if string(want) != string(got) {
		t.Fatalf("golden mismatch %s\n--- want\n%s\n--- got\n%s", name, want, got)
	}
}

func TestRulesGolden(t *testing.T) {
	in := []v1alpha1.AlertRuleGroup{
		{ObjectMeta: metav1.ObjectMeta{Name: "homelab", Namespace: "monitoring"}, Spec: v1alpha1.AlertRuleGroupSpec{TenantRef: "t", Backend: "mimir", Groups: []v1alpha1.RuleGroup{
			{Name: "node.health", Interval: "1m", Rules: []v1alpha1.Rule{
				{Alert: "NodeDown", Expr: `up{job="node"} == 0`, For: "5m", Labels: map[string]string{"severity": "critical"}, Annotations: map[string]string{"summary": "{{ $labels.instance }} down"}},
				{Record: "job:up:ratio", Expr: "avg by (job) (up)"},
			}},
		}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "udm", Namespace: "loki"}, Spec: v1alpha1.AlertRuleGroupSpec{TenantRef: "t", Backend: "loki", Groups: []v1alpha1.RuleGroup{
			{Name: "udm", Rules: []v1alpha1.Rule{{Alert: "UDMErrors", Expr: `sum(rate({host="udm"} |= "error" [5m])) > 1`, KeepFiringFor: "10m"}}},
		}}},
	}
	got := Rules("alerts-operator", in)
	if len(got) != 2 {
		t.Fatalf("namespaces: %v", got)
	}
	if _, ok := got["alerts-operator/monitoring/homelab"]; !ok {
		t.Fatalf("missing namespace: %v", got)
	}
	b, _ := yaml.Marshal(got)
	golden(t, "rules_basic.golden.yaml", b)
}

func TestRulesEqualIgnoresOrder(t *testing.T) {
	a := []backend.RuleGroup{{Name: "a", Rules: []backend.Rule{{Alert: "x", Expr: "1"}}}, {Name: "b", Rules: []backend.Rule{{Alert: "y", Expr: "2"}}}}
	b := []backend.RuleGroup{{Name: "b", Rules: []backend.Rule{{Alert: "y", Expr: "2"}}}, {Name: "a", Rules: []backend.Rule{{Alert: "x", Expr: "1"}}}}
	if !RulesEqual(a, b) {
		t.Fatal("order should not matter")
	}
	b[0].Rules[0].Expr = "3"
	if RulesEqual(a, b) {
		t.Fatal("content change must be detected")
	}
	// Comparing must not mutate the inputs (normalize deep-copies).
	c := []backend.RuleGroup{{Name: "c", Rules: []backend.Rule{{Alert: "z", Expr: "1", Labels: map[string]string{}}}}}
	_ = RulesEqual(c, c)
	if c[0].Rules[0].Labels == nil {
		t.Fatal("RulesEqual mutated its input")
	}
}
