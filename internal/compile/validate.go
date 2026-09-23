package compile

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	amconfig "github.com/prometheus/alertmanager/config"
	amtemplate "github.com/prometheus/alertmanager/template"
	"github.com/prometheus/prometheus/promql/parser"
	"sigs.k8s.io/yaml"

	"github.com/antnsn/alerts-operator/api/v1alpha1"
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
//   - template.Template.Parse takes an io.Reader and parses into the same text+html pair FromGlobs
//     builds, with template.New installing the same DefaultFuncs -- which is the part that matters
//     here, since a template using toUpper or reReplaceAll must not be rejected. It is *not* what
//     FromGlobs calls for a user file, despite the symmetry: FromGlobs calls Parse only for its two
//     embedded defaults (template/template.go:77-98) and routes every user path through FromGlob ->
//     text/html ParseGlob (:120-136), which associates a file's body with a template named after
//     the file's basename rather than with the root template. That difference does not affect a
//     parse-only validation -- neither form errors on it -- and the review of this fix ran 18
//     template corpora through both paths with identical accept/reject in all 18.
//   - The two embedded defaults are unreachable from here (the embed.FS is unexported) and are not
//     needed: text/template resolves {{ template "name" }} references at execution time, not at
//     parse time, so a user template referring to (or redefining) a default one parses identically
//     with or without them. Also confirmed empirically in that review, not assumed.
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

// ValidateContactPoint compiles one ContactPoint into its receiver and validates that receiver in
// isolation, exactly as Alertmanager() does for each ContactPoint of a tenant. The ContactPoint
// reconciler runs it at Accepted time so a malformed receiver is refused on the object that carries
// it, instead of surfacing only when the Tenant compiles the whole document and taking every other
// receiver and the routing tree down with it (alerts-operator-8ic) -- the same shape as
// AlertRuleGroup, which parses PromQL at Accepted time.
//
// It validates with the *real* Secret values, not placeholders: a webhook URL, Slack URL or Discord
// URL may come entirely from a Secret, and whether it is valid is a property of that value.
// Validating a placeholder would accept what the Tenant compile then rejects, which is the very
// asymmetry this exists to remove. The values pass through the same secretRecorder as the compile
// path, so the returned error is redacted and safe to record in a status condition. An unresolvable
// Secret is returned as an error naming the ref (not validated as empty); the reconciler checks
// Secret existence first and reports that case as SecretNotFound before calling this.
func ValidateContactPoint(cp *v1alpha1.ContactPoint, secrets SecretResolver) error {
	rec := newSecretRecorder(secrets)
	_, err := compileAndValidateReceiver(cp, rec)
	if err != nil {
		return errors.New(rec.redact(err.Error()))
	}
	return nil
}

// ValidateNotificationPolicy runs Alertmanager's config loader over one policy's route tree and
// inhibit rules in isolation, with a placeholder webhook receiver standing in for every receiver
// name the tree references. This catches what the whole-document ValidateAlertmanager would
// otherwise attribute to the policy only at Tenant compile time -- unparsable matchers, bad
// durations, malformed group_by, broken inhibit rules -- so the NotificationPolicy reconciler can
// refuse them at Accepted time, the same gate ContactPoint and AlertRuleGroup have
// (alerts-operator-8ic). Receiver existence is not this function's concern (the reconciler checks
// ContactPoint references itself), and templates belong to the Tenant, so neither is validated here.
func ValidateNotificationPolicy(pol *v1alpha1.NotificationPolicy) error {
	names, err := pol.Spec.Route.Receivers()
	if err != nil {
		return err
	}
	known := map[string]bool{}
	doc := amConfig{}
	for _, name := range names {
		full := ReceiverName(pol.Namespace, name)
		if known[full] {
			continue
		}
		known[full] = true
		doc.Receivers = append(doc.Receivers, amReceiver{Name: full, WebhookConfigs: []amWebhook{{URL: "http://placeholder.invalid/"}}})
	}
	route, err := compileRoute(&pol.Spec.Route, pol.Namespace, known)
	if err != nil {
		return err
	}
	doc.Route = route
	for _, ir := range pol.Spec.InhibitRules {
		doc.InhibitRules = append(doc.InhibitRules, amInhibitRule{SourceMatchers: ir.SourceMatchers, TargetMatchers: ir.TargetMatchers, Equal: ir.Equal})
	}
	raw, err := yaml.Marshal(doc)
	if err != nil {
		return err
	}
	if _, err := amconfig.Load(string(raw)); err != nil {
		return fmt.Errorf("alertmanager config: %w", err)
	}
	return nil
}

// compileAndValidateReceiver is the single receiver-level pipeline shared by ValidateContactPoint
// and Alertmanager(): resolve secrets, build the receiver, validate it in isolation.
func compileAndValidateReceiver(cp *v1alpha1.ContactPoint, rec *secretRecorder) (amReceiver, error) {
	rcv, err := compileReceiver(cp, rec.resolve)
	if err != nil {
		return rcv, err
	}
	if err := validateReceiver(rcv); err != nil {
		return rcv, err
	}
	return rcv, nil
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
