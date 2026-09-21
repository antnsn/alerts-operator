// Package loki talks to the Grafana Loki ruler API.
//
// Only the bulk GET /loki/api/v1/rules is used for reads: on Loki 3.6.7 the
// per-group GET returns a malformed 404.
package loki

import (
	"context"

	"github.com/antnsn/alerts-operator/internal/backend"
)

const rulesPath = "/loki/api/v1/rules"

// Client is the Loki client for the ruler API.
type Client struct{ h *backend.HTTP }

var _ backend.RuleStore = (*Client)(nil)

// New creates a new Loki client with the given options.
func New(o backend.Options) *Client { return &Client{h: backend.NewHTTP(o)} }

// List retrieves all rule groups for the tenant.
func (c *Client) List(ctx context.Context) (map[string][]backend.RuleGroup, error) {
	out := map[string][]backend.RuleGroup{}
	if _, err := c.h.GetYAML(ctx, rulesPath, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// SetGroup sets or replaces a rule group in the given namespace.
func (c *Client) SetGroup(ctx context.Context, namespace string, g backend.RuleGroup) error {
	return c.h.PostYAML(ctx, rulesPath+"/"+backend.EscapePath(namespace), g)
}

// DeleteGroup deletes a specific rule group from the given namespace.
func (c *Client) DeleteGroup(ctx context.Context, namespace, group string) error {
	return c.h.Delete(ctx, rulesPath+"/"+backend.EscapePath(namespace, group))
}

// DeleteNamespace deletes all rule groups in the given namespace.
func (c *Client) DeleteNamespace(ctx context.Context, namespace string) error {
	return c.h.Delete(ctx, rulesPath+"/"+backend.EscapePath(namespace))
}
