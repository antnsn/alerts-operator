package compile

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/antnsn/alerts-operator/api/v1alpha1"
)

func webhookCP(url string, ref *v1alpha1.SecretKeyRef) *v1alpha1.ContactPoint {
	return &v1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "hook", Namespace: "team-b"},
		Spec: v1alpha1.ContactPointSpec{TenantRef: "t", Webhook: []v1alpha1.WebhookConfig{{URL: url, URLSecretRef: ref}}}}
}

// TestValidateContactPointRejectsMalformedWebhook: the per-receiver check that compile.Alertmanager
// runs must be reachable on its own, so the ContactPoint reconciler can refuse a malformed receiver
// at Accepted time instead of letting it break the whole tenant's document (alerts-operator-8ic).
func TestValidateContactPointRejectsMalformedWebhook(t *testing.T) {
	err := ValidateContactPoint(webhookCP("not-a-url", nil), secrets(map[string]string{}))
	if err == nil {
		t.Fatal("expected a malformed webhook URL to be rejected")
	}
	if !strings.Contains(err.Error(), "receiver") {
		t.Fatalf("error should come from the receiver validation, got %v", err)
	}
}

func TestValidateContactPointAcceptsValidWebhook(t *testing.T) {
	if err := ValidateContactPoint(webhookCP("http://hook", nil), secrets(map[string]string{})); err != nil {
		t.Fatalf("valid receiver rejected: %v", err)
	}
}

// A URL read from a Secret is part of what makes the receiver valid or not, so it is validated with
// its real value -- and that value must never leak into the returned error (it ends up in a status
// condition).
func TestValidateContactPointRedactsSecretValues(t *testing.T) {
	const secretURL = "hunter2://bad url with space"
	cp := webhookCP("", &v1alpha1.SecretKeyRef{Name: "hook", Key: "url"})
	err := ValidateContactPoint(cp, secrets(map[string]string{"team-b/hook/url": secretURL}))
	if err == nil {
		t.Fatal("expected the secret-sourced URL to be rejected")
	}
	if strings.Contains(err.Error(), secretURL) || strings.Contains(err.Error(), "hunter2") {
		t.Fatalf("secret value leaked into error: %v", err)
	}
}

// A missing Secret is the reconciler's SecretNotFound case, not a receiver validation failure; the
// validator still reports it (attributed to the ref) rather than validating with an empty value.
func TestValidateContactPointReportsUnresolvableSecret(t *testing.T) {
	cp := webhookCP("", &v1alpha1.SecretKeyRef{Name: "hook", Key: "url"})
	err := ValidateContactPoint(cp, secrets(map[string]string{}))
	if err == nil || !strings.Contains(err.Error(), "secret team-b/hook key url") {
		t.Fatalf("expected the unresolvable secret ref to be named, got %v", err)
	}
}

func policyWith(mut func(*v1alpha1.NotificationPolicy)) *v1alpha1.NotificationPolicy {
	pol := fullInput().Policy
	mut(pol)
	return pol
}

// ValidateNotificationPolicy runs Alertmanager's loader over the policy's own route tree and inhibit
// rules with placeholder receivers, so matcher and duration errors are refused on the policy at
// Accepted time rather than only when the Tenant compiles the whole document (alerts-operator-8ic,
// the NotificationPolicy half).
func TestValidateNotificationPolicyAcceptsValidPolicy(t *testing.T) {
	if err := ValidateNotificationPolicy(fullInput().Policy); err != nil {
		t.Fatalf("valid policy rejected: %v", err)
	}
}

func TestValidateNotificationPolicyRejectsBadDuration(t *testing.T) {
	err := ValidateNotificationPolicy(policyWith(func(p *v1alpha1.NotificationPolicy) { p.Spec.Route.GroupWait = "not-a-duration" }))
	if err == nil || !strings.Contains(err.Error(), "not-a-duration") {
		t.Fatalf("expected the bad duration to be named, got %v", err)
	}
}

func TestValidateNotificationPolicyRejectsBadMatcher(t *testing.T) {
	err := ValidateNotificationPolicy(policyWith(func(p *v1alpha1.NotificationPolicy) { p.Spec.Route.Matchers = []string{`severity=~"["`} }))
	if err == nil {
		t.Fatal("expected the unparsable matcher to be rejected")
	}
}

func TestValidateNotificationPolicyRejectsBadInhibitRule(t *testing.T) {
	err := ValidateNotificationPolicy(policyWith(func(p *v1alpha1.NotificationPolicy) { p.Spec.InhibitRules[0].SourceMatchers = []string{"=="} }))
	if err == nil {
		t.Fatal("expected the unparsable inhibit matcher to be rejected")
	}
}
