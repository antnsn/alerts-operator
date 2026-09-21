package compile

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	amconfig "github.com/prometheus/alertmanager/config"
	amtemplate "github.com/prometheus/alertmanager/template"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/antnsn/alerts-operator/internal/backend"
)

// ValidateAlertmanager runs Alertmanager's own config loader on the compiled document and
// parses the template files the same way Alertmanager does at startup (config.Load alone
// does not touch templates). Template names are rewritten to real temp files first.
func ValidateAlertmanager(cfg *backend.AlertmanagerConfig) error {
	text := cfg.Config
	var paths []string
	if len(cfg.TemplateFiles) > 0 {
		dir, err := os.MkdirTemp("", "am-templates-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(dir) //nolint:errcheck // best-effort cleanup of a temp dir
		for name, body := range cfg.TemplateFiles {
			p := filepath.Join(dir, name)
			if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
				return err
			}
			text = strings.ReplaceAll(text, "- "+name+"\n", "- "+p+"\n")
			paths = append(paths, p)
		}
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

// promqlParser is stateless (holds only Options) and safe for concurrent use; shared across calls.
var promqlParser = parser.NewParser(parser.Options{})

// ValidatePromQL parses a Mimir rule expression.
func ValidatePromQL(expr string) error {
	_, err := promqlParser.ParseExpr(expr)
	return err
}
