package compile

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	"github.com/antnsn/alerts-operator/api/v1alpha1"
)

func ptr[T any](v T) *T { return &v }

func secrets(m map[string]string) SecretResolver {
	return func(ns, name, key string) (string, error) {
		v, ok := m[ns+"/"+name+"/"+key]
		if !ok {
			return "", errors.New("missing " + ns + "/" + name + "/" + key)
		}
		return v, nil
	}
}

func fullInput() AlertmanagerInput {
	return AlertmanagerInput{
		Policy: &v1alpha1.NotificationPolicy{ObjectMeta: metav1.ObjectMeta{Name: "homelab", Namespace: "monitoring"}, Spec: v1alpha1.NotificationPolicySpec{
			TenantRef: "t",
			Route: v1alpha1.Route{Receiver: "keep", GroupBy: []string{"alertname", "namespace"}, GroupWait: "30s", GroupInterval: "5m", RepeatInterval: "4h",
				Routes: []apiextensionsv1.JSON{{Raw: []byte(`{"receiver":"pushover","matchers":["severity=\"critical\""],"continue":true}`)}}},
			InhibitRules: []v1alpha1.InhibitRule{{SourceMatchers: []string{`severity="critical"`}, TargetMatchers: []string{`severity="warning"`}, Equal: []string{"alertname"}}},
		}},
		ContactPoints: []v1alpha1.ContactPoint{
			{ObjectMeta: metav1.ObjectMeta{Name: "pushover", Namespace: "monitoring"}, Spec: v1alpha1.ContactPointSpec{TenantRef: "t", Pushover: []v1alpha1.PushoverConfig{{UserKeySecretRef: v1alpha1.SecretKeyRef{Name: "po", Key: "user"}, TokenSecretRef: v1alpha1.SecretKeyRef{Name: "po", Key: "token"}, Priority: "1", SendResolved: ptr(true)}}}},
			{ObjectMeta: metav1.ObjectMeta{Name: "keep", Namespace: "monitoring"}, Spec: v1alpha1.ContactPointSpec{TenantRef: "t", Webhook: []v1alpha1.WebhookConfig{{URL: "http://keep-backend.keep:8080/alerts/event/prometheus", HTTPConfig: &v1alpha1.HTTPConfig{BearerTokenSecretRef: &v1alpha1.SecretKeyRef{Name: "keep", Key: "api-key"}}, MaxAlerts: ptr(int32(0))}}}},
			{ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "monitoring"}, Spec: v1alpha1.ContactPointSpec{TenantRef: "t",
				Slack:    []v1alpha1.SlackConfig{{APIURLSecretRef: v1alpha1.SecretKeyRef{Name: "slack", Key: "url"}, Channel: "#alerts", Title: "t", Text: "x"}},
				Discord:  []v1alpha1.DiscordConfig{{WebhookURLSecretRef: v1alpha1.SecretKeyRef{Name: "discord", Key: "url"}}},
				Telegram: []v1alpha1.TelegramConfig{{BotTokenSecretRef: v1alpha1.SecretKeyRef{Name: "tg", Key: "token"}, ChatID: 42, ParseMode: "HTML"}},
				Email:    []v1alpha1.EmailConfig{{To: "a@b.c", From: "x@b.c", Smarthost: "smtp:587", AuthUsername: "x", AuthPasswordSecretRef: &v1alpha1.SecretKeyRef{Name: "smtp", Key: "pw"}, RequireTLS: ptr(true)}},
			}},
			{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "team-b"}, Spec: v1alpha1.ContactPointSpec{TenantRef: "t", Webhook: []v1alpha1.WebhookConfig{{URLSecretRef: &v1alpha1.SecretKeyRef{Name: "hook", Key: "url"}}}}},
		},
		Secrets: secrets(map[string]string{
			"monitoring/po/user": "U", "monitoring/po/token": "T", "monitoring/keep/api-key": "K",
			"monitoring/slack/url": "https://hooks.slack.com/x", "monitoring/discord/url": "https://discord.com/api/webhooks/x",
			"monitoring/tg/token": "TG", "monitoring/smtp/pw": "PW", "team-b/hook/url": "http://hook",
		}),
		Templates: map[string]string{"default.tmpl": `{{ define "pushover.default.title" }}[{{ .Status }}] {{ .CommonLabels.alertname }}{{ end }}`},
	}
}

func TestAlertmanagerGolden(t *testing.T) {
	cfg, err := Alertmanager(fullInput())
	if err != nil {
		t.Fatal(err)
	}
	b, _ := yaml.Marshal(cfg)
	golden(t, "am_full.golden.yaml", b)
	if !strings.Contains(cfg.Config, "receiver: monitoring/keep") || !strings.Contains(cfg.Config, "- name: team-b/other") {
		t.Fatalf("receiver naming:\n%s", cfg.Config)
	}
	if !strings.Contains(cfg.Config, "templates:\n- default.tmpl") {
		t.Fatalf("templates list missing:\n%s", cfg.Config)
	}
	if HashAlertmanager(cfg) != HashAlertmanager(cfg) || !strings.HasPrefix(HashAlertmanager(cfg), "sha256:") { //nolint:staticcheck // deliberately re-hashing to assert determinism
		t.Fatal("hash")
	}
}

func TestAlertmanagerAttributesErrors(t *testing.T) {
	in := fullInput()
	in.Secrets = secrets(map[string]string{})
	_, err := Alertmanager(in)
	var ae *AttributedError
	if !errors.As(err, &ae) || ae.Kind != "ContactPoint" {
		t.Fatalf("expected ContactPoint attribution, got %v", err)
	}

	in = fullInput()
	in.Policy.Spec.Route.Routes[0] = apiextensionsv1.JSON{Raw: []byte(`{"receiver":"nope"}`)}
	_, err = Alertmanager(in)
	if !errors.As(err, &ae) || ae.Kind != "NotificationPolicy" || !strings.Contains(err.Error(), "nope") {
		t.Fatalf("expected policy attribution, got %v", err)
	}

	in = fullInput()
	in.Policy.Spec.Route.GroupWait = "not-a-duration"
	_, err = Alertmanager(in)
	if !errors.As(err, &ae) || ae.Kind != "NotificationPolicy" {
		t.Fatalf("expected validation attributed to policy, got %v", err)
	}

	in = fullInput()
	in.Policy.Spec.Route.Routes[0] = apiextensionsv1.JSON{Raw: []byte("[]")}
	_, err = Alertmanager(in)
	if !errors.As(err, &ae) || ae.Kind != "NotificationPolicy" {
		t.Fatalf("expected decode-error attribution, got %v", err)
	}

	// Receiver-level failures (e.g. a malformed webhook URL) must be attributed to the
	// offending ContactPoint, not folded into whole-document validation attributed to the policy.
	in = fullInput()
	in.Secrets = secrets(map[string]string{
		"monitoring/po/user": "U", "monitoring/po/token": "T", "monitoring/keep/api-key": "K",
		"monitoring/slack/url": "https://hooks.slack.com/x", "monitoring/discord/url": "https://discord.com/api/webhooks/x",
		"monitoring/tg/token": "TG", "monitoring/smtp/pw": "PW", "team-b/hook/url": "not-a-url",
	})
	_, err = Alertmanager(in)
	if !errors.As(err, &ae) || ae.Kind != "ContactPoint" || ae.Namespace != "team-b" || ae.Name != "other" {
		t.Fatalf("expected ContactPoint attribution for invalid receiver, got %v", err)
	}
}

func TestAlertmanagerRedactsSecretsFromErrors(t *testing.T) {
	in := fullInput()
	// Every resolved value here is realistic-length and non-colliding, so the assertions below
	// test genuine redaction rather than an accidental short-value substring collision (fullInput's
	// default secrets are single characters like "U"/"T"/"K", which is fine for the golden test but
	// would make this test pass for the wrong reason: e.g. "T" happens to be a substring of
	// "SECRET-VALUE").
	in.Secrets = secrets(map[string]string{
		"monitoring/po/user": "pushover-user-abc123", "monitoring/po/token": "pushover-token-xyz789", "monitoring/keep/api-key": "keep-api-key-456",
		// A trailing control character makes Go's url.Parse itself fail with an error whose
		// Error() method formats the URL with %q (Go double-quote escaping): a secret ending in an
		// actual newline byte appears in the error text as a literal `\n` (backslash + n), not a
		// raw newline byte. That's the leak vector -- Alertmanager's config loader surfaces
		// url.Parse errors verbatim -- so both the raw value and its %q-escaped form must be
		// redacted.
		"monitoring/slack/url": "https://hooks.slack.com/services/SECRET-VALUE\n", "monitoring/discord/url": "https://discord.com/api/webhooks/discord-secret-abc",
		"monitoring/tg/token": "telegram-token-def456", "monitoring/smtp/pw": "smtp-password-ghi789", "team-b/hook/url": "http://hook",
	})
	_, err := Alertmanager(in)
	if err == nil {
		t.Fatal("expected error")
	}
	if strings.Contains(err.Error(), "SECRET-VALUE") {
		t.Fatalf("secret value leaked into error (raw or escaped): %v", err)
	}
	if !strings.Contains(err.Error(), "[REDACTED]") {
		t.Fatalf("expected [REDACTED] marker, got %v", err)
	}
	// Unrelated error text must survive intact: redaction must not corrupt words that happen to
	// share a short substring with some other resolved secret (a regression this test previously
	// masked, found while investigating the escaped-form leak).
	if !strings.Contains(err.Error(), "invalid control character") {
		t.Fatalf("unrelated error text corrupted by redaction: %v", err)
	}
}

// TestAlertmanagerTemplateNameCollidingWithAGroupByLabel keeps a historical regression pinned: two
// earlier versions of ValidateAlertmanager rewrote the compiled document's "templates" entries to
// temp-file paths -- first by a whole-document string replace, then by unmarshaling and replacing
// only that key -- before handing them to template.FromGlobs. The rewrite is gone entirely (the
// loader never reads files, see validate.go), so a template name that happens to equal a group_by
// label, a matcher or a receiver name can no longer corrupt anything by construction. This asserts
// that property from the outside so a future reintroduction of any document rewriting is caught.
func TestAlertmanagerTemplateNameCollidingWithAGroupByLabel(t *testing.T) {
	// "default.tmpl" cannot itself be used here: it contains a "." and so is never a valid
	// Alertmanager group_by label name (`^[a-zA-Z_][a-zA-Z0-9_]*$`), which would make this test
	// fail regardless of the bug under test. "clashname" is a valid label name that still
	// collides textually with the template's map key, which is what the original whole-document
	// string-replace bug actually depended on.
	in := fullInput()
	in.Policy.Spec.Route.GroupBy = []string{"alertname", "clashname"}
	in.Templates = map[string]string{"clashname": `{{ define "x" }}ok{{ end }}`}
	cfg, err := Alertmanager(in)
	if err != nil {
		t.Fatalf("expected valid config despite group_by/template name collision, got %v", err)
	}
	if !strings.Contains(cfg.Config, "- clashname\n") {
		t.Fatalf("group_by entry corrupted or missing:\n%s", cfg.Config)
	}
	if strings.Contains(cfg.Config, "am-templates-") {
		t.Fatalf("cfg.Config leaked a validation temp path:\n%s", cfg.Config)
	}
}

func TestAlertmanagerRejectsBrokenTemplate(t *testing.T) {
	in := fullInput()
	in.Templates = map[string]string{"bad.tmpl": `{{ define "x" }}{{ .Unclosed `}
	_, err := Alertmanager(in)
	if err == nil || !strings.Contains(err.Error(), "templates") {
		t.Fatalf("expected template parse error, got %v", err)
	}
}

func TestValidatePromQL(t *testing.T) {
	if err := ValidatePromQL(`up{job="x"} == 0`); err != nil {
		t.Fatal(err)
	}
	if err := ValidatePromQL(`up{job=`); err == nil {
		t.Fatal("expected parse error")
	}
}

// readOnlyFilesystem points every temp-directory environment variable Go's os.TempDir consults at a
// path that does not exist, so any os.MkdirTemp/os.CreateTemp call fails with ENOENT -- for every
// user, root included. That is how this test expresses the one property the unit suite otherwise
// cannot: the deployed operator runs on gcr.io/distroless/static with
// securityContext.readOnlyRootFilesystem: true and no volume mounted at /tmp
// (charts/alerts-operator/values.yaml, config/manager/manager.yaml), so os.TempDir() resolves to a
// path it cannot write. A validation path that writes to disk passes on a developer's writable
// machine and bricks every Tenant that sets spec.alertmanager.templatesRef in the shipped artifact.
func readOnlyFilesystem(t *testing.T) {
	t.Helper()
	nowhere := filepath.Join(t.TempDir(), "no-such-dir")
	for _, v := range []string{"TMPDIR", "TMP", "TEMP"} {
		t.Setenv(v, nowhere)
	}
	if _, err := os.MkdirTemp("", "probe-"); err == nil {
		t.Fatalf("test setup is not actually read-only: MkdirTemp succeeded under TMPDIR=%s", nowhere)
	}
}

// TestValidateAlertmanagerNeedsNoWritableFilesystem is the deployed-runtime test for the template
// validation path: templates must be parsed without ever touching the filesystem.
func TestValidateAlertmanagerNeedsNoWritableFilesystem(t *testing.T) {
	readOnlyFilesystem(t)

	in := fullInput()
	if len(in.Templates) == 0 {
		t.Fatal("fixture must carry templates or this test proves nothing")
	}
	cfg, err := Alertmanager(in)
	if err != nil {
		t.Fatalf("template validation must not require a writable filesystem: %v", err)
	}
	if !strings.Contains(cfg.Config, "templates:\n- default.tmpl") {
		t.Fatalf("templates list missing:\n%s", cfg.Config)
	}
}

// TestValidateAlertmanagerStillRejectsBrokenTemplatesOnReadOnlyFilesystem guards the other
// direction: making the read-only case pass must not mean skipping template validation there.
func TestValidateAlertmanagerStillRejectsBrokenTemplatesOnReadOnlyFilesystem(t *testing.T) {
	readOnlyFilesystem(t)

	in := fullInput()
	in.Templates = map[string]string{"bad.tmpl": `{{ define "x" }}{{ .Unclosed `}
	_, err := Alertmanager(in)
	if err == nil || !strings.Contains(err.Error(), "templates") {
		t.Fatalf("expected template parse error on a read-only filesystem, got %v", err)
	}
}
