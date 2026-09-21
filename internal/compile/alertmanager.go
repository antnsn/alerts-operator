package compile

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"sigs.k8s.io/yaml"

	"github.com/antnsn/alerts-operator/api/v1alpha1"
	"github.com/antnsn/alerts-operator/internal/backend"
)

// SecretResolver returns the value of one Secret key.
type SecretResolver func(namespace, name, key string) (string, error)

// AttributedError names the CR responsible for a compile/validation failure.
type AttributedError struct {
	Kind      string
	Namespace string
	Name      string
	Err       error
}

func (e *AttributedError) Error() string {
	return fmt.Sprintf("%s %s/%s: %v", e.Kind, e.Namespace, e.Name, e.Err)
}
func (e *AttributedError) Unwrap() error { return e.Err }

// secretRecorder wraps a SecretResolver, recording every non-empty value it resolves over the
// lifetime of one Alertmanager() call. Alertmanager's own config/template errors can embed raw
// field values verbatim (e.g. a url.Parse failure quotes the whole URL it was given), so every
// error this package returns is passed through redact() first to keep secret values out of
// status conditions and events.
type secretRecorder struct {
	inner  SecretResolver
	values map[string]struct{}
}

func newSecretRecorder(inner SecretResolver) *secretRecorder {
	return &secretRecorder{inner: inner, values: map[string]struct{}{}}
}

// resolve satisfies SecretResolver, recording the value before returning it.
func (r *secretRecorder) resolve(namespace, name, key string) (string, error) {
	v, err := r.inner(namespace, name, key)
	if v != "" {
		r.values[v] = struct{}{}
	}
	return v, err
}

// redact replaces every recorded secret value in msg with "[REDACTED]", in both its raw form and
// its Go %q/strconv.Quote-escaped form (net/url.Error, among others, formats the value it was
// given with %q, so a secret containing e.g. a literal newline shows up as a literal `\n` rather
// than a raw newline byte -- the raw-only replacement missed that entirely).
//
// All replacements happen in a single pass over the original message via strings.Replacer, which
// never rescans its own output: this matters because a short secret value can be a substring of
// the literal word "REDACTED" itself (this bit us during testing -- doing the replacements
// sequentially with strings.ReplaceAll turned an already-inserted "[REDACTED]" into
// "[REDAC[REDACTED]ED]" when another, shorter, secret happened to match inside it). Values are
// ordered longest-first so a value that's a substring of another isn't left partially redacted.
func (r *secretRecorder) redact(msg string) string {
	if len(r.values) == 0 {
		return msg
	}
	vals := make([]string, 0, len(r.values))
	for v := range r.values {
		vals = append(vals, v)
	}
	sort.Slice(vals, func(i, j int) bool { return len(vals[i]) > len(vals[j]) })

	pairs := make([]string, 0, len(vals)*4)
	for _, v := range vals {
		pairs = append(pairs, v, "[REDACTED]")
		if esc := quotedInner(v); esc != v {
			pairs = append(pairs, esc, "[REDACTED]")
		}
	}
	return strings.NewReplacer(pairs...).Replace(msg)
}

// quotedInner returns s as it would appear inside a Go %q/strconv.Quote string, without the
// surrounding quotes -- the escaping net/url.Error (and similar) applies to field values it embeds.
func quotedInner(s string) string {
	q := strconv.Quote(s)
	return q[1 : len(q)-1]
}

// attribute wraps a non-nil err as an *AttributedError with its message redacted of any secret
// value resolved so far. Returns nil when err is nil.
func (r *secretRecorder) attribute(kind, namespace, name string, err error) error {
	if err == nil {
		return nil
	}
	return &AttributedError{Kind: kind, Namespace: namespace, Name: name, Err: errors.New(r.redact(err.Error()))}
}

// AlertmanagerInput is everything needed to compile one tenant's Alertmanager document.
type AlertmanagerInput struct {
	Policy        *v1alpha1.NotificationPolicy
	ContactPoints []v1alpha1.ContactPoint
	Secrets       SecretResolver
	Templates     map[string]string
}

// ReceiverName is the Alertmanager receiver name for a ContactPoint.
func ReceiverName(namespace, name string) string { return namespace + "/" + name }

// --- Alertmanager-native YAML shapes (json tags = AM yaml keys) ---

type amConfig struct {
	Route        *amRoute        `json:"route"`
	Receivers    []amReceiver    `json:"receivers"`
	InhibitRules []amInhibitRule `json:"inhibit_rules,omitempty"`
	Templates    []string        `json:"templates,omitempty"`
}

type amRoute struct {
	Receiver       string    `json:"receiver"`
	GroupBy        []string  `json:"group_by,omitempty"`
	GroupWait      string    `json:"group_wait,omitempty"`
	GroupInterval  string    `json:"group_interval,omitempty"`
	RepeatInterval string    `json:"repeat_interval,omitempty"`
	Matchers       []string  `json:"matchers,omitempty"`
	Continue       bool      `json:"continue,omitempty"`
	Routes         []amRoute `json:"routes,omitempty"`
}

type amInhibitRule struct {
	SourceMatchers []string `json:"source_matchers,omitempty"`
	TargetMatchers []string `json:"target_matchers,omitempty"`
	Equal          []string `json:"equal,omitempty"`
}

type amReceiver struct {
	Name            string       `json:"name"`
	WebhookConfigs  []amWebhook  `json:"webhook_configs,omitempty"`
	PushoverConfigs []amPushover `json:"pushover_configs,omitempty"`
	SlackConfigs    []amSlack    `json:"slack_configs,omitempty"`
	DiscordConfigs  []amDiscord  `json:"discord_configs,omitempty"`
	TelegramConfigs []amTelegram `json:"telegram_configs,omitempty"`
	EmailConfigs    []amEmail    `json:"email_configs,omitempty"`
}

type amHTTPConfig struct {
	Authorization *amAuthorization `json:"authorization,omitempty"`
	BasicAuth     *amBasicAuth     `json:"basic_auth,omitempty"`
}
type amAuthorization struct {
	Credentials string `json:"credentials"`
}
type amBasicAuth struct {
	Username string `json:"username"`
	Password string `json:"password"`
}
type amWebhook struct {
	SendResolved *bool         `json:"send_resolved,omitempty"`
	URL          string        `json:"url"`
	HTTPConfig   *amHTTPConfig `json:"http_config,omitempty"`
	MaxAlerts    *int32        `json:"max_alerts,omitempty"`
}
type amPushover struct {
	SendResolved *bool  `json:"send_resolved,omitempty"`
	UserKey      string `json:"user_key"`
	Token        string `json:"token"`
	Title        string `json:"title,omitempty"`
	Message      string `json:"message,omitempty"`
	URL          string `json:"url,omitempty"`
	URLTitle     string `json:"url_title,omitempty"`
	Priority     string `json:"priority,omitempty"`
	Sound        string `json:"sound,omitempty"`
}
type amSlack struct {
	SendResolved *bool  `json:"send_resolved,omitempty"`
	APIURL       string `json:"api_url"`
	Channel      string `json:"channel,omitempty"`
	Username     string `json:"username,omitempty"`
	Title        string `json:"title,omitempty"`
	Text         string `json:"text,omitempty"`
	IconEmoji    string `json:"icon_emoji,omitempty"`
}
type amDiscord struct {
	SendResolved *bool  `json:"send_resolved,omitempty"`
	WebhookURL   string `json:"webhook_url"`
	Title        string `json:"title,omitempty"`
	Message      string `json:"message,omitempty"`
}
type amTelegram struct {
	SendResolved *bool  `json:"send_resolved,omitempty"`
	BotToken     string `json:"bot_token"`
	ChatID       int64  `json:"chat_id"`
	ParseMode    string `json:"parse_mode,omitempty"`
	Message      string `json:"message,omitempty"`
}
type amEmail struct {
	SendResolved *bool  `json:"send_resolved,omitempty"`
	To           string `json:"to"`
	From         string `json:"from,omitempty"`
	Smarthost    string `json:"smarthost,omitempty"`
	Hello        string `json:"hello,omitempty"`
	AuthUsername string `json:"auth_username,omitempty"`
	AuthPassword string `json:"auth_password,omitempty"`
	RequireTLS   *bool  `json:"require_tls,omitempty"`
}

// Alertmanager compiles and validates the per-tenant Alertmanager document.
func Alertmanager(in AlertmanagerInput) (*backend.AlertmanagerConfig, error) {
	if in.Policy == nil {
		return nil, fmt.Errorf("no NotificationPolicy")
	}
	rec := newSecretRecorder(in.Secrets)
	cfg := amConfig{}

	// Receivers from every ContactPoint, deterministic order. Each receiver is also validated in
	// isolation so a receiver-specific failure (malformed webhook/Slack URL, bad email settings,
	// ...) is attributed to the ContactPoint that produced it, not folded into the whole-document
	// validation below (which is attributed to the policy).
	byName := map[string]bool{}
	for i := range in.ContactPoints {
		cp := &in.ContactPoints[i]
		rcv, err := compileReceiver(cp, rec.resolve)
		if err != nil {
			return nil, rec.attribute("ContactPoint", cp.Namespace, cp.Name, err)
		}
		if err := validateReceiver(rcv); err != nil {
			return nil, rec.attribute("ContactPoint", cp.Namespace, cp.Name, err)
		}
		cfg.Receivers = append(cfg.Receivers, rcv)
		byName[rcv.Name] = true
	}
	sort.Slice(cfg.Receivers, func(i, j int) bool { return cfg.Receivers[i].Name < cfg.Receivers[j].Name })

	// Route tree, rewriting receiver names to <policy-ns>/<name>.
	pol := in.Policy
	route, err := compileRoute(&pol.Spec.Route, pol.Namespace, byName)
	if err != nil {
		return nil, rec.attribute("NotificationPolicy", pol.Namespace, pol.Name, err)
	}
	cfg.Route = route
	for _, ir := range pol.Spec.InhibitRules {
		cfg.InhibitRules = append(cfg.InhibitRules, amInhibitRule{SourceMatchers: ir.SourceMatchers, TargetMatchers: ir.TargetMatchers, Equal: ir.Equal})
	}

	// Templates: names must match template_files keys.
	for name := range in.Templates {
		cfg.Templates = append(cfg.Templates, name)
	}
	sort.Strings(cfg.Templates)

	raw, err := yaml.Marshal(cfg)
	if err != nil {
		return nil, errors.New(rec.redact(err.Error()))
	}
	out := &backend.AlertmanagerConfig{Config: string(raw), TemplateFiles: in.Templates}
	if err := ValidateAlertmanager(out); err != nil {
		// Whole-document validation covers route/matchers/durations/inhibit/templates: attribute
		// to the policy. Receiver-specific failures are caught earlier, per-receiver, above.
		return nil, rec.attribute("NotificationPolicy", pol.Namespace, pol.Name, err)
	}
	return out, nil
}

func compileRoute(r *v1alpha1.Route, ns string, known map[string]bool) (*amRoute, error) {
	full := ReceiverName(ns, r.Receiver)
	if !known[full] {
		return nil, fmt.Errorf("receiver %q not found as ContactPoint %s", r.Receiver, full)
	}
	out := &amRoute{Receiver: full, GroupBy: r.GroupBy, GroupWait: r.GroupWait, GroupInterval: r.GroupInterval,
		RepeatInterval: r.RepeatInterval, Matchers: r.Matchers, Continue: r.Continue}
	children, err := r.ChildRoutes()
	if err != nil {
		return nil, fmt.Errorf("route %q: %w", r.Receiver, err)
	}
	for i := range children {
		child, err := compileRoute(&children[i], ns, known)
		if err != nil {
			return nil, err
		}
		out.Routes = append(out.Routes, *child)
	}
	return out, nil
}

func compileReceiver(cp *v1alpha1.ContactPoint, secrets SecretResolver) (amReceiver, error) {
	get := func(ref *v1alpha1.SecretKeyRef) (string, error) {
		if ref == nil {
			return "", nil
		}
		v, err := secrets(cp.Namespace, ref.Name, ref.Key)
		if err != nil {
			return "", fmt.Errorf("secret %s/%s key %s: %w", cp.Namespace, ref.Name, ref.Key, err)
		}
		return v, nil
	}
	rcv := amReceiver{Name: ReceiverName(cp.Namespace, cp.Name)}
	for _, w := range cp.Spec.Webhook {
		url := w.URL
		if w.URLSecretRef != nil {
			v, err := get(w.URLSecretRef)
			if err != nil {
				return rcv, err
			}
			url = v
		}
		hook := amWebhook{URL: url, SendResolved: w.SendResolved, MaxAlerts: w.MaxAlerts}
		if w.HTTPConfig != nil {
			hc := &amHTTPConfig{}
			if w.HTTPConfig.BearerTokenSecretRef != nil {
				tok, err := get(w.HTTPConfig.BearerTokenSecretRef)
				if err != nil {
					return rcv, err
				}
				hc.Authorization = &amAuthorization{Credentials: tok}
			}
			if w.HTTPConfig.BasicAuth != nil {
				u, err := get(&w.HTTPConfig.BasicAuth.UsernameSecretRef)
				if err != nil {
					return rcv, err
				}
				p, err := get(&w.HTTPConfig.BasicAuth.PasswordSecretRef)
				if err != nil {
					return rcv, err
				}
				hc.BasicAuth = &amBasicAuth{Username: u, Password: p}
			}
			hook.HTTPConfig = hc
		}
		rcv.WebhookConfigs = append(rcv.WebhookConfigs, hook)
	}
	for _, p := range cp.Spec.Pushover {
		user, err := get(&p.UserKeySecretRef)
		if err != nil {
			return rcv, err
		}
		tok, err := get(&p.TokenSecretRef)
		if err != nil {
			return rcv, err
		}
		rcv.PushoverConfigs = append(rcv.PushoverConfigs, amPushover{SendResolved: p.SendResolved, UserKey: user, Token: tok,
			Title: p.Title, Message: p.Message, URL: p.URL, URLTitle: p.URLTitle, Priority: p.Priority, Sound: p.Sound})
	}
	for _, s := range cp.Spec.Slack {
		u, err := get(&s.APIURLSecretRef)
		if err != nil {
			return rcv, err
		}
		rcv.SlackConfigs = append(rcv.SlackConfigs, amSlack{SendResolved: s.SendResolved, APIURL: u, Channel: s.Channel, Username: s.Username, Title: s.Title, Text: s.Text, IconEmoji: s.IconEmoji})
	}
	for _, d := range cp.Spec.Discord {
		u, err := get(&d.WebhookURLSecretRef)
		if err != nil {
			return rcv, err
		}
		rcv.DiscordConfigs = append(rcv.DiscordConfigs, amDiscord{SendResolved: d.SendResolved, WebhookURL: u, Title: d.Title, Message: d.Message})
	}
	for _, tg := range cp.Spec.Telegram {
		tok, err := get(&tg.BotTokenSecretRef)
		if err != nil {
			return rcv, err
		}
		rcv.TelegramConfigs = append(rcv.TelegramConfigs, amTelegram{SendResolved: tg.SendResolved, BotToken: tok, ChatID: tg.ChatID, ParseMode: tg.ParseMode, Message: tg.Message})
	}
	for _, e := range cp.Spec.Email {
		pw, err := get(e.AuthPasswordSecretRef)
		if err != nil {
			return rcv, err
		}
		rcv.EmailConfigs = append(rcv.EmailConfigs, amEmail{SendResolved: e.SendResolved, To: e.To, From: e.From, Smarthost: e.Smarthost, Hello: e.Hello, AuthUsername: e.AuthUsername, AuthPassword: pw, RequireTLS: e.RequireTLS})
	}
	return rcv, nil
}

// HashAlertmanager returns "sha256:<hex>" over the canonical YAML of cfg.
func HashAlertmanager(cfg *backend.AlertmanagerConfig) string {
	b, _ := yaml.Marshal(cfg)
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}
