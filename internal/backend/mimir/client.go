// Package mimir talks to the Grafana Mimir ruler and Alertmanager config APIs.
package mimir

import (
	"context"

	"github.com/antnsn/alerts-operator/internal/backend"
)

const (
	rulesPath = "/prometheus/config/v1/rules"
	amPath    = "/api/v1/alerts"
)

// Client is the Mimir client for ruler and Alertmanager config APIs.
type Client struct{ h *backend.HTTP }

var (
	_ backend.RuleStore         = (*Client)(nil)
	_ backend.AlertmanagerStore = (*Client)(nil)
)

// New creates a new Mimir client with the given options.
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

// Get retrieves the Alertmanager config. Returns nil, nil if no config is stored.
func (c *Client) Get(ctx context.Context) (*backend.AlertmanagerConfig, error) {
	var cfg backend.AlertmanagerConfig
	found, err := c.h.GetYAML(ctx, amPath, &cfg)
	if err != nil || !found {
		return nil, err
	}
	return &cfg, nil
}

// Set updates the Alertmanager config.
func (c *Client) Set(ctx context.Context, cfg *backend.AlertmanagerConfig) error {
	return c.h.PostJSON(ctx, amPath, cfg)
}

// Delete removes the Alertmanager config.
func (c *Client) Delete(ctx context.Context) error { return c.h.Delete(ctx, amPath) }
