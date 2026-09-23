package compile

import (
	"fmt"
	"sort"
	"strings"

	amconfig "github.com/prometheus/alertmanager/config"
	amtemplate "github.com/prometheus/alertmanager/template"
	"github.com/prometheus/prometheus/promql/parser"
	"sigs.k8s.io/yaml"

	"github.com/antnsn/alerts-operator/internal/backend"
)

// ValidateAlertmanager runs Alertmanager's own config loader on the compiled document and parses
// the template bodies the same way Alertmanager does at startup (config.Load alone does not touch
// templates).
//
// Everything here is in memory, deliberately: the operator's own container runs on
// gcr.io/distroless/static with securityContext.readOnlyRootFilesystem: true and no volume mounted
// at /tmp (charts/alerts-operator/values.yaml, config/manager/manager.yaml), so os.TempDir()
// resolves to a path it cannot write. An earlier version wrote the template bodies to
// os.MkdirTemp and handed the paths to template.FromGlobs, which is the only API that package
// documents -- on the shipped artifact that mkdir fails, the failure is attributed to the
// NotificationPolicy as a compile error, and syncAlertmanager then abandons the whole
// Alertmanager document (receivers, routing tree, inhibit rules -- not just templates) before any
// backend I/O. Neither half of what FromGlobs does needs a file:
//
//   - amconfig.Load parses YAML only. It is config.LoadFile, not Load, that resolves the
//     "templates" entries against a base directory, so the compiled document's bare template names
//     pass through Load untouched and no path rewriting is needed at all.
//   - template.Template.Parse takes an io.Reader and is exactly what FromGlobs calls per file
//     after ParseGlob. template.New installs the same DefaultFuncs FromGlobs relies on. The two
//     embedded defaults FromGlobs parses first (default.tmpl, email.tmpl) are unreachable from
//     here -- the embed.FS is unexported -- and are not needed: text/template resolves
//     {{ template "name" }} references at execution time, not at parse time, so a user template
//     referring to a default one parses identically with or without them.
//
// Templates are parsed in sorted name order so a document with several broken templates always
// reports the same one.
func ValidateAlertmanager(cfg *backend.AlertmanagerConfig) error {
	if _, err := amconfig.Load(cfg.Config); err != nil {
		return fmt.Errorf("alertmanager config: %w", err)
	}
	if len(cfg.TemplateFiles) == 0 {
		return nil
	}
	tmpl, err := amtemplate.New()
	if err != nil {
		return fmt.Errorf("alertmanager templates: %w", err)
	}
	names := make([]string, 0, len(cfg.TemplateFiles))
	for name := range cfg.TemplateFiles {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := tmpl.Parse(strings.NewReader(cfg.TemplateFiles[name])); err != nil {
			return fmt.Errorf("alertmanager templates: %s: %w", name, err)
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
