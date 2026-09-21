package backend

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHTTPTenantHeaderAndAuth(t *testing.T) {
	var gotTenant, gotAuth, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotTenant = r.Header.Get("X-Scope-OrgID")
		u, p, _ := r.BasicAuth()
		gotAuth = u + ":" + p
		gotPath = r.URL.EscapedPath()
		w.WriteHeader(202)
	}))
	defer srv.Close()
	h := NewHTTP(Options{Address: srv.URL, TenantID: "1", BasicAuth: &BasicAuth{Username: "u", Password: "p"}})
	if err := h.PostYAML(context.Background(), "/x/"+EscapePath("alerts-operator/ns/name"), map[string]string{"a": "b"}); err != nil {
		t.Fatal(err)
	}
	if gotTenant != "1" || gotAuth != "u:p" || gotPath != "/x/alerts-operator%2Fns%2Fname" {
		t.Fatalf("tenant=%q auth=%q path=%q", gotTenant, gotAuth, gotPath)
	}
}

func TestNewHTTPDoesNotMutateInjectedClient(t *testing.T) {
	// Issue: when caller supplies HTTPClient and Options.Timeout,
	// NewHTTP should not mutate the caller-owned client
	injected := &http.Client{}
	originalTimeout := injected.Timeout

	_ = NewHTTP(Options{
		Address:    "http://localhost:8080",
		TenantID:   "1",
		HTTPClient: injected,
		Timeout:    time.Second,
	})

	if injected.Timeout != originalTimeout {
		t.Fatalf("injected client was mutated: Timeout before=%v after=%v", originalTimeout, injected.Timeout)
	}
}

func TestHTTPErrorClassification(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/500":
			w.WriteHeader(500)
		case "/400":
			http.Error(w, "bad rule", 400)
		case "/404":
			w.WriteHeader(404)
		}
	}))
	defer srv.Close()
	h := NewHTTP(Options{Address: srv.URL, TenantID: "1"})
	ctx := context.Background()
	if _, _, err := h.Do(ctx, "GET", "/500", nil, ""); !IsUnavailable(err) {
		t.Fatalf("500 should be unavailable: %v", err)
	}
	if _, _, err := h.Do(ctx, "GET", "/400", nil, ""); !IsRejected(err) || err.Error() != "backend returned 400: bad rule" {
		t.Fatalf("400: %v", err)
	}
	var out map[string]any
	if found, err := h.GetYAML(ctx, "/404", &out); found || err != nil {
		t.Fatalf("404 → found=%v err=%v", found, err)
	}
	if err := h.Delete(ctx, "/404"); err != nil {
		t.Fatalf("delete 404 should be tolerated: %v", err)
	}
	srv.Close()
	if _, _, err := h.Do(ctx, "GET", "/500", nil, ""); !IsUnavailable(err) {
		t.Fatalf("connection refused should be unavailable: %v", err)
	}
}

func TestGetYAMLEmptyBodyDoesNotModifyOut(t *testing.T) {
	// Verify documented contract: empty 2xx body returns found=true without modifying out
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		// Empty body
	}))
	defer srv.Close()

	h := NewHTTP(Options{Address: srv.URL, TenantID: "1"})
	ctx := context.Background()

	// Initialize output with a value to verify it's not modified
	out := map[string]string{"original": "value"}
	found, err := h.GetYAML(ctx, "/", &out)

	if !found || err != nil {
		t.Fatalf("empty 2xx: found=%v err=%v", found, err)
	}

	// Verify the output parameter was NOT modified
	if _, ok := out["original"]; !ok {
		t.Fatalf("empty body modified out: %v", out)
	}
}

func TestHTTPOversizedRejectedBody(t *testing.T) {
	// Issue: if reading a >=400 response body fails/truncates (e.g., exceeds 8 MiB limit),
	// should still return StatusError so IsRejected is true, not IsUnavailable
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(400)
		// Write a body larger than 8 MiB to trigger truncation
		largebody := strings.Repeat("x", 9<<20)
		_, _ = w.Write([]byte(largebody))
	}))
	defer srv.Close()

	h := NewHTTP(Options{Address: srv.URL, TenantID: "1"})
	ctx := context.Background()
	_, _, err := h.Do(ctx, "GET", "/", nil, "")

	// Even though body was oversized, this is a 400 so should be rejected
	if !IsRejected(err) {
		t.Fatalf("oversized 400 body should be rejected: %v (IsUnavailable=%v)", err, IsUnavailable(err))
	}
	if IsUnavailable(err) {
		t.Fatalf("oversized 400 should not be unavailable: %v", err)
	}
}
