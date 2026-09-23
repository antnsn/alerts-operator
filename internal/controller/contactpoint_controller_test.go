package controller

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	observabilityv1alpha1 "github.com/antnsn/alerts-operator/api/v1alpha1"
)

func TestContactPointAccepted(t *testing.T) {
	newFakeTenant(t, "cp-tenant", true, false)

	missingTenant := &observabilityv1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "cp-missing-tenant", Namespace: "default"},
		Spec: observabilityv1alpha1.ContactPointSpec{TenantRef: "cp-nope", Webhook: []observabilityv1alpha1.WebhookConfig{{URL: "http://x"}}}}
	createAndCleanup(t, missingTenant)
	waitCondition(t, missingTenant, observabilityv1alpha1.ConditionAccepted, metav1.ConditionFalse, observabilityv1alpha1.ReasonTenantNotFound)

	inline := &observabilityv1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "cp-inline", Namespace: "default"},
		Spec: observabilityv1alpha1.ContactPointSpec{TenantRef: "cp-tenant", Webhook: []observabilityv1alpha1.WebhookConfig{{URL: "http://x"}}}}
	createAndCleanup(t, inline)
	waitCondition(t, inline, observabilityv1alpha1.ConditionAccepted, metav1.ConditionTrue, observabilityv1alpha1.ReasonAccepted)

	po := &observabilityv1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "cp-po", Namespace: "default"},
		Spec: observabilityv1alpha1.ContactPointSpec{TenantRef: "cp-tenant", Pushover: []observabilityv1alpha1.PushoverConfig{{
			UserKeySecretRef: observabilityv1alpha1.SecretKeyRef{Name: "cp-po-secret", Key: "user"},
			TokenSecretRef:   observabilityv1alpha1.SecretKeyRef{Name: "cp-po-secret", Key: "token"}}}}}
	createAndCleanup(t, po)
	waitCondition(t, po, observabilityv1alpha1.ConditionAccepted, metav1.ConditionFalse, observabilityv1alpha1.ReasonSecretNotFound)

	// Secret with only one key → still SecretNotFound, message names the key.
	sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "cp-po-secret", Namespace: "default"}, StringData: map[string]string{"user": "U"}}
	createAndCleanup(t, sec)
	waitFor(t, func() bool {
		_ = testClient.Get(testCtx, clientKey(po), po)
		c := findCond(po, observabilityv1alpha1.ConditionAccepted)
		return c.Reason == observabilityv1alpha1.ReasonSecretNotFound && c.Message == "secret default/cp-po-secret key token not found"
	})

	// Adding the missing key flips Accepted to True via the Secret watch.
	if err := testClient.Get(testCtx, clientKey(sec), sec); err != nil {
		t.Fatal(err)
	}
	sec.Data["token"] = []byte("T")
	if err := testClient.Update(testCtx, sec); err != nil {
		t.Fatal(err)
	}
	waitCondition(t, po, observabilityv1alpha1.ConditionAccepted, metav1.ConditionTrue, observabilityv1alpha1.ReasonAccepted)
}

// TestContactPointRejectsMalformedReceiverAtAccepted (alerts-operator-8ic): a receiver Alertmanager
// would refuse must be Accepted=False on the ContactPoint itself, so listChildren excludes it and
// the Tenant's document keeps compiling for everyone else -- the same shape AlertRuleGroup already
// has for a bad PromQL expression. Fixing the spec flips it back to Accepted=True.
func TestContactPointRejectsMalformedReceiverAtAccepted(t *testing.T) {
	newFakeTenant(t, "cp-invalid-tenant", true, false)

	bad := &observabilityv1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "cp-bad-url", Namespace: "default"},
		Spec: observabilityv1alpha1.ContactPointSpec{TenantRef: "cp-invalid-tenant", Webhook: []observabilityv1alpha1.WebhookConfig{{URL: "not-a-url"}}}}
	createAndCleanup(t, bad)
	waitCondition(t, bad, observabilityv1alpha1.ConditionAccepted, metav1.ConditionFalse, observabilityv1alpha1.ReasonInvalid)
	if c := findCond(bad, observabilityv1alpha1.ConditionAccepted); c.Message == "" {
		t.Fatalf("Accepted=False/Invalid must carry the receiver error, got %+v", c)
	}

	if err := testClient.Get(testCtx, clientKey(bad), bad); err != nil {
		t.Fatal(err)
	}
	bad.Spec.Webhook[0].URL = "http://hook"
	if err := testClient.Update(testCtx, bad); err != nil {
		t.Fatal(err)
	}
	waitCondition(t, bad, observabilityv1alpha1.ConditionAccepted, metav1.ConditionTrue, observabilityv1alpha1.ReasonAccepted)
}
