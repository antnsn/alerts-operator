package backend

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
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
