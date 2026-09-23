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

// TestFakeLokiRejectsEmbeddedSlashInNamespace models the real defect behind alerts-operator-b4o.
// Loki's ruler HTTP router matches on the DECODED path, so %2F becomes a path-segment boundary and
// a namespace with an embedded slash produces more segments than any registered pattern has: the
// route simply does not exist and Loki answers 404. Verified against live Loki 3.6.7:
// POST /loki/api/v1/rules/flatns -> 202, POST /loki/api/v1/rules/e2e%2Fe2e%2Fudm -> 404.
//
// The fake reproduces it so this class of bug fails in unit tests instead of only on a real
// cluster -- the original defect was invisible to the whole unit suite precisely because the fake
// happily accepted slashes.
func TestFakeLokiRejectsEmbeddedSlashInNamespace(t *testing.T) {
	s := New()
	defer s.Close()
	body := "name: g\nrules:\n- alert: A\n  expr: '{job=\"x\"} |= \"err\"'\n"

	// Two embedded slashes: 3 decoded segments, no route at all -> 404 on every verb.
	for _, m := range []string{"POST", "DELETE", "GET"} {
		if code, _ := do(t, m, s.URL+"/loki/api/v1/rules/p%2Fns%2Fa", body); code != 404 {
			t.Fatalf("%s on a slash-containing loki namespace must 404, got %d", m, code)
		}
	}
	// One embedded slash: 2 decoded segments, which collides with the {namespace}/{group} route --
	// POST is not registered there, so Loki answers 405, not 202.
	if code, _ := do(t, "POST", s.URL+"/loki/api/v1/rules/p%2Fns", body); code != 405 {
		t.Fatalf("a one-slash loki namespace must hit the {ns}/{group} route (405), got %d", code)
	}
	if len(s.LokiRules("1")) != 0 {
		t.Fatalf("nothing may be stored for a rejected namespace: %+v", s.LokiRules("1"))
	}

	// A slash-free namespace -- the scheme the operator now uses -- works end to end, including the
	// per-group GET that the project previously (and wrongly) believed was broken on 3.6.7.
	if code, _ := do(t, "POST", s.URL+"/loki/api/v1/rules/p_ns_a", body); code != 202 {
		t.Fatalf("slash-free POST %d", code)
	}
	if code, b := do(t, "GET", s.URL+"/loki/api/v1/rules/p_ns_a/g", ""); code != 200 || !strings.Contains(b, "name: g") {
		t.Fatalf("per-group GET on a slash-free namespace must work: %d %q", code, b)
	}
	if code, _ := do(t, "DELETE", s.URL+"/loki/api/v1/rules/p_ns_a", ""); code != 202 {
		t.Fatalf("slash-free DELETE %d", code)
	}
}

// TestFakeLokiBulkGetSeesSlashNamespaces: the *store* can still hold a namespace with a slash (one
// written outside this API, or by an older/other tool). Only the per-namespace routes are
// unreachable, so bulk GET must still list it -- the operator's prune has to be able to see, and
// deliberately not claim, such a namespace.
func TestFakeLokiBulkGetSeesSlashNamespaces(t *testing.T) {
	s := New()
	defer s.Close()
	s.SetLokiRules("1", "p/ns/a", []backend.RuleGroup{{Name: "g", Rules: []backend.Rule{{Alert: "A", Expr: `{job="x"} |= "err"`}}}})
	if len(s.Rules("1")) != 0 {
		t.Fatal("loki seed must not appear in the mimir map")
	}
	code, body := do(t, "GET", s.URL+"/loki/api/v1/rules", "")
	if code != 200 || !strings.Contains(body, "p/ns/a:") {
		t.Fatalf("bulk get %d %q", code, body)
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
