package compile

import (
	"fmt"
	"os"
	"path/filepath"

	amconfig "github.com/prometheus/alertmanager/config"
	amtemplate "github.com/prometheus/alertmanager/template"
	"github.com/prometheus/prometheus/promql/parser"
	"sigs.k8s.io/yaml"

	"github.com/antnsn/alerts-operator/internal/backend"
)

// ValidateAlertmanager runs Alertmanager's own config loader on the compiled document and
// parses the template files the same way Alertmanager does at startup (config.Load alone
// does not touch templates). Template names are rewritten to real temp files first, scoped
// strictly to the document's "templates" key: the document is unmarshaled into a generic map
// and only that key is replaced, so a matcher, group_by label, or receiver name that happens to
// equal a template name is never touched.
func ValidateAlertmanager(cfg *backend.AlertmanagerConfig) error {
	text := cfg.Config
	var paths []string
	if len(cfg.TemplateFiles) > 0 {
		dir, err := os.MkdirTemp("", "am-templates-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(dir) //nolint:errcheck // best-effort cleanup of a temp dir

		var doc map[string]any
		if err := yaml.Unmarshal([]byte(text), &doc); err != nil {
			return fmt.Errorf("alertmanager config: %w", err)
		}
		for name, body := range cfg.TemplateFiles {
			p := filepath.Join(dir, name)
			if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
				return err
			}
			paths = append(paths, p)
		}
		doc["templates"] = paths
		raw, err := yaml.Marshal(doc)
		if err != nil {
			return fmt.Errorf("alertmanager config: %w", err)
		}
		text = string(raw)
	}
	if _, err := amconfig.Load(text); err != nil {
		return fmt.Errorf("alertmanager config: %w", err)
	}
	if len(paths) > 0 {
		if _, err := amtemplate.FromGlobs(paths); err != nil {
			return fmt.Errorf("alertmanager templates: %w", err)
		}
	}
	return nil
}

// validateReceiver runs Alertmanager's config loader against a single receiver in isolation
// (wrapped in the smallest valid document: a root route that points at it and nothing else), so
// receiver-specific failures -- a malformed webhook/Slack URL, invalid email settings, and so on
// -- surface here and can be attributed to the ContactPoint that produced them, rather than
// being folded into the whole-document validation in ValidateAlertmanager.
func validateReceiver(rcv amReceiver) error {
	doc := amConfig{Route: &amRoute{Receiver: rcv.Name}, Receivers: []amReceiver{rcv}}
	raw, err := yaml.Marshal(doc)
	if err != nil {
		return err
	}
	if _, err := amconfig.Load(string(raw)); err != nil {
		return fmt.Errorf("receiver: %w", err)
	}
	return nil
}

// promqlParser is stateless (holds only Options) and safe for concurrent use; shared across calls.
var promqlParser = parser.NewParser(parser.Options{})

// ValidatePromQL parses a Mimir rule expression.
func ValidatePromQL(expr string) error {
	_, err := promqlParser.ParseExpr(expr)
	return err
}
