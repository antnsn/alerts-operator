package backend

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"sigs.k8s.io/yaml"
)

// StatusError is returned for HTTP responses with status >= 400.
type StatusError struct {
	Status int
	Body   string
}

func (e *StatusError) Error() string {
	b := strings.TrimSpace(e.Body)
	if len(b) > 512 {
		b = b[:512] + "…"
	}
	if b == "" {
		return fmt.Sprintf("backend returned %d", e.Status)
	}
	return fmt.Sprintf("backend returned %d: %s", e.Status, b)
}

// IsUnavailable reports transport errors and 5xx responses.
func IsUnavailable(err error) bool {
	if err == nil {
		return false
	}
	var se *StatusError
	if errors.As(err, &se) {
		return se.Status >= 500
	}
	return true
}

// IsRejected reports 4xx responses other than 404.
func IsRejected(err error) bool {
	var se *StatusError
	return errors.As(err, &se) && se.Status >= 400 && se.Status < 500 && se.Status != 404
}

// IsNotFound reports 404 responses.
func IsNotFound(err error) bool {
	var se *StatusError
	return errors.As(err, &se) && se.Status == 404
}

// EscapePath escapes each segment (including "/") and joins with "/".
func EscapePath(segments ...string) string {
	parts := make([]string, len(segments))
	for i, s := range segments {
		parts[i] = url.PathEscape(s)
	}
	return strings.Join(parts, "/")
}

// HTTP is the shared transport for Mimir and Loki clients.
type HTTP struct {
	base   string
	tenant string
	auth   *BasicAuth
	client *http.Client
}

// NewHTTP creates a new HTTP client with the given options.
func NewHTTP(o Options) *HTTP {
	c := o.HTTPClient
	if c == nil {
		timeout := o.Timeout
		if timeout <= 0 {
			timeout = 30 * time.Second
		}
		c = &http.Client{Timeout: timeout}
	}
	// Note: injected client is used as-is; caller owns it and controls timeout
	return &HTTP{base: strings.TrimRight(o.Address, "/"), tenant: o.TenantID, auth: o.BasicAuth, client: c}
}

// Do performs one request. Status >= 400 yields *StatusError.
func (h *HTTP) Do(ctx context.Context, method, path string, body []byte, contentType string) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, h.base+path, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("X-Scope-OrgID", h.tenant)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if h.auth != nil {
		req.SetBasicAuth(h.auth.Username, h.auth.Password)
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	const maxSize = 8 << 20 // 8MB
	lr := io.LimitedReader{R: resp.Body, N: maxSize + 1}
	respBody, err := io.ReadAll(&lr)

	// For >=400 status, always return StatusError with the available body,
	// even if the body read errored or truncated
	if resp.StatusCode >= 400 {
		return resp.StatusCode, respBody, &StatusError{Status: resp.StatusCode, Body: string(respBody)}
	}

	// For <400 status, surface any read errors
	if err != nil {
		return resp.StatusCode, nil, err
	}
	if lr.N < 1 {
		return resp.StatusCode, nil, fmt.Errorf("response body exceeds %d bytes", maxSize)
	}
	return resp.StatusCode, respBody, nil
}

// GetYAML decodes a YAML response into out. A 404 returns found=false, err=nil.
// An empty 2xx body returns found=true without modifying out.
func (h *HTTP) GetYAML(ctx context.Context, path string, out any) (bool, error) {
	_, body, err := h.Do(ctx, http.MethodGet, path, nil, "")
	if IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return true, nil
	}
	if err := yaml.Unmarshal(body, out); err != nil {
		return true, fmt.Errorf("decode %s: %w", path, err)
	}
	return true, nil
}

// PostYAML encodes in as YAML and POSTs it.
func (h *HTTP) PostYAML(ctx context.Context, path string, in any) error {
	body, err := yaml.Marshal(in)
	if err != nil {
		return err
	}
	_, _, err = h.Do(ctx, http.MethodPost, path, body, "application/yaml")
	return err
}

// Delete issues DELETE; 404 is treated as success.
func (h *HTTP) Delete(ctx context.Context, path string) error {
	_, _, err := h.Do(ctx, http.MethodDelete, path, nil, "")
	if IsNotFound(err) {
		return nil
	}
	return err
}
