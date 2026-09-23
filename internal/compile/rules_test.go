package compile

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
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
	// Rules is per-backend: the caller filters by backend first, and the namespace scheme differs
	// between them (Mimir joins on "/", Loki on "_" -- see BackendNamespace). The golden holds both
	// results merged so the two schemes are visible side by side; the keys can never collide,
	// because a Kubernetes name contains neither separator.
	got := map[string][]backend.RuleGroup{}
	for ns, gs := range Rules(v1alpha1.BackendMimir, "alerts-operator", in[:1]) {
		got[ns] = gs
	}
	for ns, gs := range Rules(v1alpha1.BackendLoki, "alerts-operator", in[1:]) {
		got[ns] = gs
	}
	if len(got) != 2 {
		t.Fatalf("namespaces: %v", got)
	}
	if _, ok := got["alerts-operator/monitoring/homelab"]; !ok {
		t.Fatalf("missing mimir namespace: %v", got)
	}
	if _, ok := got["alerts-operator_loki_udm"]; !ok {
		t.Fatalf("missing loki namespace: %v", got)
	}
	b, _ := yaml.Marshal(got)
	golden(t, "rules_basic.golden.yaml", b)
}

// TestLokiBackendNamespaceNeverContainsSlash is the regression test for alerts-operator-b4o, the
// defect that made the Loki backend inert on a real cluster: Loki's ruler HTTP router treats every
// literal "/" as a path-segment boundary, so it 404s on every per-namespace route
// (POST/DELETE/per-group GET) whose namespace contains an embedded slash -- even URL-escaped as
// %2F. Verified against live Loki 3.6.7: POST /loki/api/v1/rules/flatns -> 202,
// POST /loki/api/v1/rules/e2e%2Fe2e%2Fudm -> 404.
//
// Every namespace this operator builds is <prefix><sep><k8s-namespace><sep><name>, so with sep="/"
// no AlertRuleGroup could ever sync to Loki. Nothing in the unit suite noticed, because the fake
// backend accepted slashes; the property itself is what has to be asserted.
func TestLokiBackendNamespaceNeverContainsSlash(t *testing.T) {
	for _, c := range []struct{ prefix, ns, name string }{
		{"alerts-operator", "monitoring", "homelab"},
		{v1alpha1.DefaultRulesNamespacePrefix, "e2e", "udm"},
		{"a.b-c", "kube-system", "node.rules"},
	} {
		got := BackendNamespace(v1alpha1.BackendLoki, c.prefix, c.ns, c.name)
		if strings.Contains(got, "/") {
			t.Fatalf("a Loki rule namespace must never contain %q: got %q", "/", got)
		}
		if want := c.prefix + "_" + c.ns + "_" + c.name; got != want {
			t.Fatalf("BackendNamespace(loki) = %q, want %q", got, want)
		}
	}
	// The same property must hold for every key Rules produces, not only for the helper: a call
	// site that built a namespace some other way would be invisible to the check above.
	args := []v1alpha1.AlertRuleGroup{
		{ObjectMeta: metav1.ObjectMeta{Name: "udm", Namespace: "loki"}, Spec: v1alpha1.AlertRuleGroupSpec{Backend: v1alpha1.BackendLoki,
			Groups: []v1alpha1.RuleGroup{{Name: "udm", Rules: []v1alpha1.Rule{{Alert: "A", Expr: `{host="udm"} |= "error"`}}}}}},
	}
	for ns := range Rules(v1alpha1.BackendLoki, "alerts-operator", args) {
		if strings.Contains(ns, "/") {
			t.Fatalf("Rules(loki) produced a namespace containing %q: %q", "/", ns)
		}
	}
	// Mimir is deliberately unchanged: its ruler handles embedded slashes and the scheme is
	// verified working on a live cluster, so the narrow fix must not touch it.
	if got, want := BackendNamespace(v1alpha1.BackendMimir, "alerts-operator", "monitoring", "homelab"), "alerts-operator/monitoring/homelab"; got != want {
		t.Fatalf("BackendNamespace(mimir) = %q, want %q", got, want)
	}
}

// TestOwnsNamespaceMatchesOnSegmentBoundary guards the prune and the finalizer: ownership is a
// prefix match on a *segment boundary* with the separator of the backend in question. A bare
// strings.HasPrefix(ns, "alerts-operator") would also claim "alerts-operator-other_..." and delete
// somebody else's live rules.
func TestOwnsNamespaceMatchesOnSegmentBoundary(t *testing.T) {
	for _, c := range []struct {
		be   v1alpha1.Backend
		ns   string
		want bool
	}{
		{v1alpha1.BackendMimir, "alerts-operator/default/x", true},
		{v1alpha1.BackendMimir, "alerts-operator-other/default/x", false},
		{v1alpha1.BackendMimir, "alerts-operator", false},
		{v1alpha1.BackendMimir, "alerts-operator_default_x", false}, // a Loki-shaped name is not a Mimir one
		{v1alpha1.BackendLoki, "alerts-operator_default_x", true},
		{v1alpha1.BackendLoki, "alerts-operator-other_default_x", false},
		{v1alpha1.BackendLoki, "alerts-operator", false},
		{v1alpha1.BackendLoki, "alerts-operator/default/x", false}, // a Mimir-shaped name is not a Loki one
	} {
		if got := OwnsNamespace(c.be, "alerts-operator", c.ns); got != c.want {
			t.Fatalf("OwnsNamespace(%s, %q) = %v, want %v", c.be, c.ns, got, c.want)
		}
	}
	if got, want := OwnedNamespacePrefix(v1alpha1.BackendLoki, "alerts-operator"), "alerts-operator_"; got != want {
		t.Fatalf("OwnedNamespacePrefix(loki) = %q, want %q", got, want)
	}
	if got, want := OwnedNamespacePrefix(v1alpha1.BackendMimir, "alerts-operator"), "alerts-operator/"; got != want {
		t.Fatalf("OwnedNamespacePrefix(mimir) = %q, want %q", got, want)
	}
}

// TestLokiOwnershipRequiresUnderscoreFreePrefix makes an otherwise invisible coupling explicit:
// OwnsNamespace's segment-boundary match is only unambiguous because Tenant.spec.rulesNamespacePrefix
// cannot contain "_". If it could, a Tenant with prefix "team" would own every namespace of a Tenant
// with prefix "team_a" -- and would prune them, since conflictingTenant compares prefixes for
// equality and would not see the two as colliding.
//
// The assertion below is deliberately the *hazard*, not the safe outcome: it is true, and the only
// thing standing between it and deleted live rules is the CRD pattern. Loosening that pattern
// without reading this will make the intent obvious rather than silently reintroduce the hazard.
// The pattern itself is enforced by TestTenantRulesNamespacePrefixExcludesUnderscore.
func TestLokiOwnershipRequiresUnderscoreFreePrefix(t *testing.T) {
	if !OwnsNamespace(v1alpha1.BackendLoki, "team", "team_a_default_x") {
		t.Fatal("precondition changed: prefix \"team\" no longer swallows a \"team_a\" namespace; " +
			"if OwnsNamespace now parses segments, revisit whether rulesNamespacePrefix still needs to forbid \"_\"")
	}
	// No two prefixes that the CRD pattern actually admits can collide this way, because neither can
	// contain the other's separator.
	for _, p := range []string{"team", "team-a", "team.a", "alerts-operator"} {
		for _, q := range []string{"team", "team-a", "team.a", "alerts-operator"} {
			if p == q {
				continue
			}
			if strings.HasPrefix(OwnedNamespacePrefix(v1alpha1.BackendLoki, q), OwnedNamespacePrefix(v1alpha1.BackendLoki, p)) {
				t.Fatalf("legal prefixes %q and %q collide on the Loki ownership boundary", p, q)
			}
			if strings.HasPrefix(OwnedNamespacePrefix(v1alpha1.BackendMimir, q), OwnedNamespacePrefix(v1alpha1.BackendMimir, p)) {
				t.Fatalf("legal prefixes %q and %q collide on the Mimir ownership boundary", p, q)
			}
		}
	}
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
