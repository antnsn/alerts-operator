package controller

import (
	"strings"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	observabilityv1alpha1 "github.com/antnsn/alerts-operator/api/v1alpha1"
)

// TestTenantAlertmanagerNotBrokenByPolicyRoutingToRejectedContactPoint: the Tenant-level view of the
// Codex P1 on the first 8ic fix. With one good and one malformed ContactPoint and a policy whose
// child route names the malformed one, the policy is not Accepted, so the Tenant never compiles a
// document with a dangling receiver: AlertmanagerSynced is False/NoNotificationPolicy (the backend
// untouched), not False/Invalid attributed to the policy. Fixing the ContactPoint cascades:
// ContactPoint Accepted, policy Accepted, AlertmanagerSynced True with both receivers in the document.
func TestTenantAlertmanagerNotBrokenByPolicyRoutingToRejectedContactPoint(t *testing.T) {
	tn, srv := newFakeTenant(t, "tn-rej-route", true, false)
	good := &observabilityv1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "rr-good", Namespace: "default"},
		Spec: observabilityv1alpha1.ContactPointSpec{TenantRef: tn.Name, Webhook: []observabilityv1alpha1.WebhookConfig{{URL: "http://good"}}}}
	bad := &observabilityv1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "rr-bad", Namespace: "default"},
		Spec: observabilityv1alpha1.ContactPointSpec{TenantRef: tn.Name, Webhook: []observabilityv1alpha1.WebhookConfig{{URL: "not-a-url"}}}}
	pol := &observabilityv1alpha1.NotificationPolicy{ObjectMeta: metav1.ObjectMeta{Name: "rr-pol", Namespace: "default"},
		Spec: observabilityv1alpha1.NotificationPolicySpec{TenantRef: tn.Name, Route: observabilityv1alpha1.Route{Receiver: "rr-good",
			Routes: []apiextensionsv1.JSON{rawRoute(t, observabilityv1alpha1.Route{Receiver: "rr-bad", Matchers: []string{`severity="critical"`}})}}}}
	for _, o := range []observabilityv1alpha1.Conditioned{good, bad, pol} {
		createAndCleanup(t, o)
	}

	waitCondition(t, bad, observabilityv1alpha1.ConditionAccepted, metav1.ConditionFalse, observabilityv1alpha1.ReasonInvalid)
	waitCondition(t, pol, observabilityv1alpha1.ConditionAccepted, metav1.ConditionFalse, observabilityv1alpha1.ReasonContactPointNotAccepted)
	waitCondition(t, tn, observabilityv1alpha1.ConditionAlertmanagerSynced, metav1.ConditionFalse, observabilityv1alpha1.ReasonNoNotificationPolicy)
	if srv.Alertmanager("1") != nil {
		t.Fatalf("no document may be written while the only policy is rejected: %+v", srv.Alertmanager("1"))
	}

	if err := testClient.Get(testCtx, clientKey(bad), bad); err != nil {
		t.Fatal(err)
	}
	bad.Spec.Webhook[0].URL = "http://fixed"
	if err := testClient.Update(testCtx, bad); err != nil {
		t.Fatal(err)
	}
	waitCondition(t, pol, observabilityv1alpha1.ConditionAccepted, metav1.ConditionTrue, observabilityv1alpha1.ReasonAccepted)
	waitCondition(t, tn, observabilityv1alpha1.ConditionAlertmanagerSynced, metav1.ConditionTrue, observabilityv1alpha1.ReasonSynced)
	am := srv.Alertmanager("1")
	if am == nil || !strings.Contains(am.Config, "- name: default/rr-good") || !strings.Contains(am.Config, "- name: default/rr-bad") {
		t.Fatalf("after the fix both receivers must be in the document: %+v", am)
	}
}
