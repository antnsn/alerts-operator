package mimir

import (
	"context"
	"testing"

	"github.com/antnsn/alerts-operator/internal/backend"
	"github.com/antnsn/alerts-operator/internal/backend/fake"
)

func TestClientRulesAndAlertmanager(t *testing.T) {
	s := fake.New()
	defer s.Close()
	c := New(backend.Options{Address: s.URL, TenantID: "1"})
	ctx := context.Background()

	got, err := c.List(ctx)
	if err != nil || len(got) != 0 {
		t.Fatalf("empty list: %v %v", got, err)
	}
	g := backend.RuleGroup{Name: "g", Interval: "1m", Rules: []backend.Rule{{Alert: "A", Expr: "up == 0", For: "5m", Labels: map[string]string{"severity": "critical"}}}}
	if err := c.SetGroup(ctx, "alerts-operator/ns/x", g); err != nil {
		t.Fatal(err)
	}
	got, err = c.List(ctx)
	if err != nil || len(got["alerts-operator/ns/x"]) != 1 || got["alerts-operator/ns/x"][0].Rules[0].Labels["severity"] != "critical" {
		t.Fatalf("list after set: %+v %v", got, err)
	}
	if err := c.DeleteGroup(ctx, "alerts-operator/ns/x", "g"); err != nil {
		t.Fatal(err)
	}
	if err := c.DeleteNamespace(ctx, "alerts-operator/ns/x"); err != nil {
		t.Fatal(err)
	}
	if reqs := s.Requests(); reqs[1] != "POST /prometheus/config/v1/rules/alerts-operator%2Fns%2Fx" {
		t.Fatalf("path escaping: %v", reqs)
	}

	am, err := c.Get(ctx)
	if err != nil || am != nil {
		t.Fatalf("no config: %v %v", am, err)
	}
	cfg := &backend.AlertmanagerConfig{Config: "route:\n  receiver: x\nreceivers:\n- name: x\n", TemplateFiles: map[string]string{"a.tmpl": "{{ define \"a\" }}x{{ end }}"}}
	if err := c.Set(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	am, err = c.Get(ctx)
	if err != nil || am == nil || am.Config != cfg.Config || am.TemplateFiles["a.tmpl"] != cfg.TemplateFiles["a.tmpl"] {
		t.Fatalf("round trip: %+v %v", am, err)
	}
	if err := c.Delete(ctx); err != nil {
		t.Fatal(err)
	}
	s.Fail(502)
	if _, err := c.List(ctx); !backend.IsUnavailable(err) {
		t.Fatalf("expected unavailable, got %v", err)
	}
}
