// Package fake is an in-memory Mimir + Loki ruler/Alertmanager API for tests.
package fake

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"

	"sigs.k8s.io/yaml"

	"github.com/antnsn/alerts-operator/internal/backend"
)

const (
	mimirRulesBase = "/prometheus/config/v1/rules"
	lokiRulesBase  = "/loki/api/v1/rules"
)

type tenantState struct {
	mimir map[string][]backend.RuleGroup // Mimir ruler namespaces
	loki  map[string][]backend.RuleGroup // Loki ruler namespaces
	am    *backend.AlertmanagerConfig
}

// Server is an in-memory httptest server that speaks the Mimir ruler,
// Loki ruler, and Mimir Alertmanager APIs used by internal/backend.
type Server struct {
	*httptest.Server
	mu         sync.Mutex
	tenants    map[string]*tenantState
	failStatus int
	rejectMsg  string
	requests   []string
}

// New starts a fake Mimir/Loki server. Call Close when done.
func New() *Server {
	s := &Server{tenants: map[string]*tenantState{}}
	s.Server = httptest.NewServer(http.HandlerFunc(s.handle))
	return s
}

func (s *Server) tenant(id string) *tenantState {
	t, ok := s.tenants[id]
	if !ok {
		t = &tenantState{mimir: map[string][]backend.RuleGroup{}, loki: map[string][]backend.RuleGroup{}}
		s.tenants[id] = t
	}
	return t
}

// Rules returns a copy of the tenant's Mimir ruler state (namespace → groups).
func (s *Server) Rules(tenant string) map[string][]backend.RuleGroup {
	s.mu.Lock()
	defer s.mu.Unlock()
	return copyRules(s.tenant(tenant).mimir)
}

// LokiRules returns a copy of the tenant's Loki ruler state (namespace → groups).
func (s *Server) LokiRules(tenant string) map[string][]backend.RuleGroup {
	s.mu.Lock()
	defer s.mu.Unlock()
	return copyRules(s.tenant(tenant).loki)
}

// copyRules deep-copies namespace -> groups so callers can't mutate server
// state (including nested rule labels/annotations) through the returned map.
func copyRules(in map[string][]backend.RuleGroup) map[string][]backend.RuleGroup {
	out := map[string][]backend.RuleGroup{}
	for ns, gs := range in {
		cp := make([]backend.RuleGroup, len(gs))
		for i, g := range gs {
			cp[i] = copyRuleGroup(g)
		}
		out[ns] = cp
	}
	return out
}

func copyRuleGroup(g backend.RuleGroup) backend.RuleGroup {
	out := g
	out.Rules = make([]backend.Rule, len(g.Rules))
	for i, rule := range g.Rules {
		out.Rules[i] = copyRule(rule)
	}
	return out
}

func copyRule(r backend.Rule) backend.Rule {
	out := r
	out.Labels = copyStringMap(r.Labels)
	out.Annotations = copyStringMap(r.Annotations)
	return out
}

func copyStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// Alertmanager returns a copy of the tenant's Alertmanager config, or nil if none is stored.
func (s *Server) Alertmanager(tenant string) *backend.AlertmanagerConfig {
	s.mu.Lock()
	defer s.mu.Unlock()
	am := s.tenant(tenant).am
	if am == nil {
		return nil
	}
	c := *am
	c.TemplateFiles = copyStringMap(am.TemplateFiles)
	return &c
}

// SetRules seeds the Mimir ruler state for tenant/namespace.
func (s *Server) SetRules(tenant, ns string, groups []backend.RuleGroup) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tenant(tenant).mimir[ns] = append([]backend.RuleGroup(nil), groups...)
}

// SetLokiRules seeds the Loki ruler state for tenant/namespace.
func (s *Server) SetLokiRules(tenant, ns string, groups []backend.RuleGroup) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tenant(tenant).loki[ns] = append([]backend.RuleGroup(nil), groups...)
}

// SetAlertmanager seeds the Alertmanager config for tenant.
func (s *Server) SetAlertmanager(tenant string, cfg *backend.AlertmanagerConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tenant(tenant).am = cfg
}

// Fail makes every request return status until Fail(0) is called.
func (s *Server) Fail(status int) { s.mu.Lock(); s.failStatus = status; s.mu.Unlock() }

// RejectPost makes every POST return 400 msg until RejectPost("") is called.
func (s *Server) RejectPost(msg string) { s.mu.Lock(); s.rejectMsg = msg; s.mu.Unlock() }

// ResetRequests clears the request log.
func (s *Server) ResetRequests() { s.mu.Lock(); s.requests = nil; s.mu.Unlock() }

// Requests returns the "METHOD path" log of requests handled so far.
func (s *Server) Requests() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.requests...)
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = append(s.requests, r.Method+" "+r.URL.EscapedPath())
	if s.failStatus != 0 {
		http.Error(w, "injected failure", s.failStatus)
		return
	}
	if r.Method == http.MethodPost && s.rejectMsg != "" {
		http.Error(w, s.rejectMsg, http.StatusBadRequest)
		return
	}
	tenant := r.Header.Get("X-Scope-OrgID")
	if tenant == "" {
		http.Error(w, "no org id", http.StatusUnauthorized)
		return
	}
	st := s.tenant(tenant)
	path := r.URL.EscapedPath()

	switch {
	case path == "/api/v1/alerts":
		s.handleAM(w, r, st)
	case matchesBase(path, mimirRulesBase):
		s.handleRules(w, r, st.mimir, strings.TrimPrefix(path, mimirRulesBase), false)
	case matchesBase(path, lokiRulesBase):
		s.handleRules(w, r, st.loki, strings.TrimPrefix(path, lokiRulesBase), true)
	default:
		http.NotFound(w, r)
	}
}

// matchesBase reports whether path is exactly base or base followed by a
// "/"-delimited suffix, so "/rules" doesn't also match "/rulesfoo".
func matchesBase(path, base string) bool {
	return path == base || strings.HasPrefix(path, base+"/")
}

func (s *Server) handleAM(w http.ResponseWriter, r *http.Request, st *tenantState) {
	switch r.Method {
	case http.MethodGet:
		if st.am == nil {
			http.Error(w, "alertmanager storage object not found", http.StatusNotFound)
			return
		}
		b, _ := yaml.Marshal(st.am)
		w.Header().Set("Content-Type", "application/yaml")
		_, _ = w.Write(b)
	case http.MethodPost:
		body, _ := io.ReadAll(r.Body)
		var cfg backend.AlertmanagerConfig
		if err := yaml.Unmarshal(body, &cfg); err != nil || cfg.Config == "" {
			http.Error(w, "error validating Alertmanager config: "+fmt.Sprint(err), http.StatusBadRequest)
			return
		}
		st.am = &cfg
		w.WriteHeader(http.StatusCreated)
	case http.MethodDelete:
		st.am = nil
		w.WriteHeader(http.StatusOK)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// segments splits "/ns/group" (escaped) into decoded parts, the way Mimir's ruler routes: on the
// raw, still-escaped path, so an escaped "/" inside one segment stays inside it.
func segments(rest string) []string {
	rest = strings.Trim(rest, "/")
	if rest == "" {
		return nil
	}
	parts := strings.Split(rest, "/")
	for i, p := range parts {
		if u, err := unescape(p); err == nil {
			parts[i] = u
		}
	}
	return parts
}

// decodedSegments splits on the DECODED path, the way Loki's ruler routes: %2F is turned back into
// a literal "/" before pattern matching, so it counts as a path-segment boundary. This is what
// makes a namespace with an embedded slash unaddressable on Loki (alerts-operator-b4o).
func decodedSegments(rest string) []string {
	if u, err := unescape(rest); err == nil {
		rest = u
	}
	rest = strings.Trim(rest, "/")
	if rest == "" {
		return nil
	}
	return strings.Split(rest, "/")
}

func unescape(s string) (string, error) {
	return url.PathUnescape(s)
}

func (s *Server) handleRules(w http.ResponseWriter, r *http.Request, rules map[string][]backend.RuleGroup, rest string, loki bool) {
	seg := segments(rest)
	if loki {
		// Loki routes on the decoded path (see decodedSegments): a namespace containing "/" yields
		// more segments than any registered pattern has, so no route matches and the ruler answers a
		// bare 404 -- for POST, DELETE and GET alike. Verified against live Loki 3.6.7:
		// POST /loki/api/v1/rules/flatns -> 202, POST /loki/api/v1/rules/e2e%2Fe2e%2Fudm -> 404.
		// A namespace with exactly one embedded slash decodes to two segments and so collides with
		// the {namespace}/{group} route instead, which is why POST there answers 405: that fallthrough
		// is reproduced by simply routing on the decoded segments below.
		seg = decodedSegments(rest)
		if len(seg) > 2 {
			http.NotFound(w, r)
			return
		}
	}
	switch {
	case r.Method == http.MethodGet && len(seg) == 0:
		if len(rules) == 0 {
			http.Error(w, "no rule groups found", http.StatusNotFound)
			return
		}
		b, _ := yaml.Marshal(rules)
		w.Header().Set("Content-Type", "application/yaml")
		_, _ = w.Write(b)
	case r.Method == http.MethodGet && len(seg) == 1:
		gs, ok := rules[seg[0]]
		if !ok {
			http.Error(w, "namespace not found", http.StatusNotFound)
			return
		}
		b, _ := yaml.Marshal(map[string][]backend.RuleGroup{seg[0]: gs})
		_, _ = w.Write(b)
	case r.Method == http.MethodGet && len(seg) == 2:
		// Loki's per-group GET is not special-cased: on a slash-free namespace it works exactly like
		// Mimir's (verified GET=200 on live Loki 3.6.7). The operator still only ever uses the bulk
		// GET -- see internal/backend/loki -- but for sufficiency, not because this route is broken.
		for _, g := range rules[seg[0]] {
			if g.Name == seg[1] {
				b, _ := yaml.Marshal(g)
				_, _ = w.Write(b)
				return
			}
		}
		http.Error(w, "group not found", http.StatusNotFound)
	case r.Method == http.MethodPost && len(seg) == 1:
		body, _ := io.ReadAll(r.Body)
		var g backend.RuleGroup
		if err := yaml.Unmarshal(body, &g); err != nil || g.Name == "" {
			http.Error(w, "invalid rule group", http.StatusBadRequest)
			return
		}
		ns := seg[0]
		replaced := false
		for i := range rules[ns] {
			if rules[ns][i].Name == g.Name {
				rules[ns][i] = g
				replaced = true
			}
		}
		if !replaced {
			rules[ns] = append(rules[ns], g)
		}
		w.WriteHeader(http.StatusAccepted)
	case r.Method == http.MethodDelete && len(seg) == 1:
		delete(rules, seg[0])
		w.WriteHeader(http.StatusAccepted)
	case r.Method == http.MethodDelete && len(seg) == 2:
		ns := seg[0]
		var kept []backend.RuleGroup
		for _, g := range rules[ns] {
			if g.Name != seg[1] {
				kept = append(kept, g)
			}
		}
		if len(kept) == 0 {
			delete(rules, ns)
		} else {
			rules[ns] = kept
		}
		w.WriteHeader(http.StatusAccepted)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}
