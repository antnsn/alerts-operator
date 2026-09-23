package loki

import (
	"context"
	"strings"
	"testing"

	"github.com/antnsn/alerts-operator/internal/backend"
	"github.com/antnsn/alerts-operator/internal/backend/fake"
)

// TestClientReadsOnlyInBulk: the reconciler diffs from one bulk GET, so the client must never
// issue a per-namespace or per-group GET. That is a sufficiency choice (one read covers the whole
// diff), not a workaround for a broken route -- per-group GET is verified working on Loki 3.6.7.
//
// The namespace here uses the "_" scheme the operator actually writes; the loop also asserts no
// request path carries an escaped slash, which is what Loki's router rejects (alerts-operator-b4o).
func TestClientReadsOnlyInBulk(t *testing.T) {
	s := fake.New()
	defer s.Close()
	c := New(backend.Options{Address: s.URL, TenantID: "1"})
	ctx := context.Background()
	const ns = "alerts-operator_ns_l"
	g := backend.RuleGroup{Name: "g", Rules: []backend.Rule{{Alert: "E", Expr: `sum(rate({job="x"} |= "error" [5m])) > 0`}}}
	if err := c.SetGroup(ctx, ns, g); err != nil {
		t.Fatal(err)
	}
	got, err := c.List(ctx)
	if err != nil || got[ns][0].Name != "g" {
		t.Fatalf("list: %+v %v", got, err)
	}
	if err := c.DeleteGroup(ctx, ns, "g"); err != nil {
		t.Fatal(err)
	}
	if err := c.DeleteNamespace(ctx, ns); err != nil {
		t.Fatal(err)
	}
	for _, r := range s.Requests() {
		if strings.HasPrefix(r, "GET /loki/api/v1/rules/") {
			t.Fatalf("per-group/namespace GET is forbidden: %s", r)
		}
		if !strings.Contains(r, "/loki/api/v1/rules") {
			t.Fatalf("wrong base path: %s", r)
		}
		if strings.Contains(strings.ToUpper(r), "%2F") {
			t.Fatalf("a Loki request path must never carry an escaped slash: %s", r)
		}
	}
	got, err = c.List(ctx)
	if err != nil || len(got) != 0 {
		t.Fatalf("empty after delete: %+v %v", got, err)
	}
}
