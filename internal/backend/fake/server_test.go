package fake

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/antnsn/alerts-operator/internal/backend"
)

func do(t *testing.T, method, url, body string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	req.Header.Set("X-Scope-OrgID", "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestFakeMimirRulesRoundTrip(t *testing.T) {
	s := New()
	defer s.Close()
	code, _ := do(t, "POST", s.URL+"/prometheus/config/v1/rules/p%2Fns%2Fa", "name: g1\nrules:\n- alert: A\n  expr: up == 0\n")
	if code != 202 {
		t.Fatalf("post %d", code)
	}
	code, body := do(t, "GET", s.URL+"/prometheus/config/v1/rules", "")
	if code != 200 || !strings.Contains(body, "p/ns/a:") || !strings.Contains(body, "name: g1") {
		t.Fatalf("get %d %q", code, body)
	}
	if code, _ = do(t, "DELETE", s.URL+"/prometheus/config/v1/rules/p%2Fns%2Fa/g1", ""); code != 202 {
		t.Fatalf("delete group %d", code)
	}
	if code, _ = do(t, "GET", s.URL+"/prometheus/config/v1/rules", ""); code != 404 {
		t.Fatalf("empty list should be 404, got %d", code)
	}
	if len(s.Rules("1")) != 0 {
		t.Fatalf("state not empty")
	}
}

func TestFakeLokiPerGroupGetIsBroken(t *testing.T) {
	s := New()
	defer s.Close()
	s.SetLokiRules("1", "p/ns/a", []backend.RuleGroup{{Name: "g", Rules: []backend.Rule{{Alert: "A", Expr: `{job="x"} |= "err"`}}}})
	if len(s.Rules("1")) != 0 {
		t.Fatal("loki seed must not appear in the mimir map")
	}
	code, body := do(t, "GET", s.URL+"/loki/api/v1/rules/p%2Fns%2Fa/g", "")
	if code != 404 || body == "" {
		t.Fatalf("expected malformed 404, got %d %q", code, body)
	}
	if code, _ = do(t, "GET", s.URL+"/loki/api/v1/rules", ""); code != 200 {
		t.Fatalf("bulk get %d", code)
	}
}

func TestFakeAlertmanagerAndFaults(t *testing.T) {
	s := New()
	defer s.Close()
	if code, _ := do(t, "GET", s.URL+"/api/v1/alerts", ""); code != 404 {
		t.Fatalf("no config should be 404, got %d", code)
	}
	if code, _ := do(t, "POST", s.URL+"/api/v1/alerts", "alertmanager_config: |\n  route:\n    receiver: x\n  receivers:\n  - name: x\n"); code != 201 {
		t.Fatalf("post %d", code)
	}
	if s.Alertmanager("1") == nil || !strings.Contains(s.Alertmanager("1").Config, "receiver: x") {
		t.Fatalf("not stored")
	}
	s.Fail(503)
	if code, _ := do(t, "GET", s.URL+"/api/v1/alerts", ""); code != 503 {
		t.Fatalf("fail %d", code)
	}
	s.Fail(0)
	s.RejectPost("invalid config")
	if code, body := do(t, "POST", s.URL+"/api/v1/alerts", "x: y"); code != 400 || !strings.Contains(body, "invalid config") {
		t.Fatalf("reject %d %q", code, body)
	}
	s.RejectPost("")
	if code, _ := do(t, "DELETE", s.URL+"/api/v1/alerts", ""); code != 200 || s.Alertmanager("1") != nil {
		t.Fatalf("delete")
	}
	if got := s.Requests(); len(got) == 0 || !strings.HasPrefix(got[0], "GET /api/v1/alerts") {
		t.Fatalf("requests %v", got)
	}
}
