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

type Client struct{ h *backend.HTTP }

var (
	_ backend.RuleStore         = (*Client)(nil)
	_ backend.AlertmanagerStore = (*Client)(nil)
)

func New(o backend.Options) *Client { return &Client{h: backend.NewHTTP(o)} }

func (c *Client) List(ctx context.Context) (map[string][]backend.RuleGroup, error) {
	out := map[string][]backend.RuleGroup{}
	if _, err := c.h.GetYAML(ctx, rulesPath, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Client) SetGroup(ctx context.Context, namespace string, g backend.RuleGroup) error {
	return c.h.PostYAML(ctx, rulesPath+"/"+backend.EscapePath(namespace), g)
}

func (c *Client) DeleteGroup(ctx context.Context, namespace, group string) error {
	return c.h.Delete(ctx, rulesPath+"/"+backend.EscapePath(namespace, group))
}

func (c *Client) DeleteNamespace(ctx context.Context, namespace string) error {
	return c.h.Delete(ctx, rulesPath+"/"+backend.EscapePath(namespace))
}

func (c *Client) Get(ctx context.Context) (*backend.AlertmanagerConfig, error) {
	var cfg backend.AlertmanagerConfig
	found, err := c.h.GetYAML(ctx, amPath, &cfg)
	if err != nil || !found {
		return nil, err
	}
	return &cfg, nil
}

func (c *Client) Set(ctx context.Context, cfg *backend.AlertmanagerConfig) error {
	return c.h.PostYAML(ctx, amPath, cfg)
}

func (c *Client) Delete(ctx context.Context) error { return c.h.Delete(ctx, amPath) }
