// Package backend defines the Mimir/Loki API shapes and client interfaces used by the operator.
package backend

import (
	"context"
	"net/http"
	"time"
)

// Rule is one ruler rule in backend YAML shape.
type Rule struct {
	Record        string            `json:"record,omitempty"`
	Alert         string            `json:"alert,omitempty"`
	Expr          string            `json:"expr"`
	For           string            `json:"for,omitempty"`
	KeepFiringFor string            `json:"keep_firing_for,omitempty"`
	Labels        map[string]string `json:"labels,omitempty"`
	Annotations   map[string]string `json:"annotations,omitempty"`
}

// RuleGroup is one ruler group in backend YAML shape.
type RuleGroup struct {
	Name     string `json:"name"`
	Interval string `json:"interval,omitempty"`
	Rules    []Rule `json:"rules"`
}

// AlertmanagerConfig is the Mimir per-tenant Alertmanager document (POST/GET /api/v1/alerts).
type AlertmanagerConfig struct {
	Config        string            `json:"alertmanager_config"`
	TemplateFiles map[string]string `json:"template_files,omitempty"`
}

// RuleStore manages ruler rule groups for one tenant.
type RuleStore interface {
	// List returns namespace → groups. Empty map when the tenant has no rules.
	List(ctx context.Context) (map[string][]RuleGroup, error)
	SetGroup(ctx context.Context, namespace string, g RuleGroup) error
	DeleteGroup(ctx context.Context, namespace, group string) error
	DeleteNamespace(ctx context.Context, namespace string) error
}

// AlertmanagerStore manages the Alertmanager config for one tenant.
type AlertmanagerStore interface {
	// Get returns nil, nil when no config is stored.
	Get(ctx context.Context) (*AlertmanagerConfig, error)
	Set(ctx context.Context, cfg *AlertmanagerConfig) error
	Delete(ctx context.Context) error
}

// BasicAuth holds username and password for HTTP Basic Authentication.
type BasicAuth struct {
	Username string
	Password string
}

// Options configure a backend client.
type Options struct {
	Address    string
	TenantID   string
	BasicAuth  *BasicAuth
	Timeout    time.Duration // default 30s
	HTTPClient *http.Client  // default http.DefaultClient with Timeout
}
