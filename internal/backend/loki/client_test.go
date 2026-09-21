package loki

import (
	"context"
	"strings"
	"testing"

	"github.com/antnsn/alerts-operator/internal/backend"
	"github.com/antnsn/alerts-operator/internal/backend/fake"
)

func TestClientNeverUsesPerGroupGet(t *testing.T) {
	s := fake.New()
	defer s.Close()
	c := New(backend.Options{Address: s.URL, TenantID: "1"})
	ctx := context.Background()
	g := backend.RuleGroup{Name: "g", Rules: []backend.Rule{{Alert: "E", Expr: `sum(rate({job="x"} |= "error" [5m])) > 0`}}}
	if err := c.SetGroup(ctx, "alerts-operator/ns/l", g); err != nil {
		t.Fatal(err)
	}
	got, err := c.List(ctx)
	if err != nil || got["alerts-operator/ns/l"][0].Name != "g" {
		t.Fatalf("list: %+v %v", got, err)
	}
	if err := c.DeleteGroup(ctx, "alerts-operator/ns/l", "g"); err != nil {
		t.Fatal(err)
	}
	if err := c.DeleteNamespace(ctx, "alerts-operator/ns/l"); err != nil {
		t.Fatal(err)
	}
	for _, r := range s.Requests() {
		if strings.HasPrefix(r, "GET /loki/api/v1/rules/") {
			t.Fatalf("per-group/namespace GET is forbidden: %s", r)
		}
		if !strings.Contains(r, "/loki/api/v1/rules") {
			t.Fatalf("wrong base path: %s", r)
		}
	}
	got, err = c.List(ctx)
	if err != nil || len(got) != 0 {
		t.Fatalf("empty after delete: %+v %v", got, err)
	}
}
