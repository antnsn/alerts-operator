package controller

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	observabilityv1alpha1 "github.com/antnsn/alerts-operator/api/v1alpha1"
)

// TestNotificationPolicyRejectsRouteToRejectedContactPoint (alerts-operator-8ic, Codex P1 on the
// first fix): a policy that routes to a ContactPoint which exists but is Accepted=False must not be
// Accepted itself. Otherwise the Tenant excludes the bad ContactPoint but compiles the accepted
// policy, compileRoute reports the receiver missing, and AlertmanagerSynced goes False/Invalid for
// the whole tenant -- the same blast radius, one hop removed. Fixing the ContactPoint flips the
// policy back to Accepted=True through the existing ContactPoint watch.
func TestNotificationPolicyRejectsRouteToRejectedContactPoint(t *testing.T) {
	newFakeTenant(t, "np-rej-tenant", true, false)
	bad := &observabilityv1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "np-rej-cp", Namespace: "default"},
		Spec: observabilityv1alpha1.ContactPointSpec{TenantRef: "np-rej-tenant", Webhook: []observabilityv1alpha1.WebhookConfig{{URL: "not-a-url"}}}}
	createAndCleanup(t, bad)
	waitCondition(t, bad, observabilityv1alpha1.ConditionAccepted, metav1.ConditionFalse, observabilityv1alpha1.ReasonInvalid)

	pol := &observabilityv1alpha1.NotificationPolicy{ObjectMeta: metav1.ObjectMeta{Name: "np-rej-pol", Namespace: "default"},
		Spec: observabilityv1alpha1.NotificationPolicySpec{TenantRef: "np-rej-tenant", Route: observabilityv1alpha1.Route{Receiver: "np-rej-cp"}}}
	createAndCleanup(t, pol)
	waitCondition(t, pol, observabilityv1alpha1.ConditionAccepted, metav1.ConditionFalse, observabilityv1alpha1.ReasonContactPointNotAccepted)
	if c := findCond(pol, observabilityv1alpha1.ConditionAccepted); !strings.Contains(c.Message, "default/np-rej-cp") {
		t.Fatalf("message must name the rejected ContactPoint, got %+v", c)
	}

	if err := testClient.Get(testCtx, clientKey(bad), bad); err != nil {
		t.Fatal(err)
	}
	bad.Spec.Webhook[0].URL = "http://fixed"
	if err := testClient.Update(testCtx, bad); err != nil {
		t.Fatal(err)
	}
	waitCondition(t, pol, observabilityv1alpha1.ConditionAccepted, metav1.ConditionTrue, observabilityv1alpha1.ReasonAccepted)
}
