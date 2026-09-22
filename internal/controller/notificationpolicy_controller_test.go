package controller

import (
	"strings"
	"testing"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	observabilityv1alpha1 "github.com/antnsn/alerts-operator/api/v1alpha1"
)

func TestRouteDepthAndWinner(t *testing.T) {
	r := observabilityv1alpha1.Route{Receiver: "a", Routes: []apiextensionsv1.JSON{{Raw: []byte(`{"receiver":"b","routes":[{"receiver":"c"}]}`)}}}
	d, err := routeDepth(&r)
	if err != nil {
		t.Fatal(err)
	}
	if d != 3 {
		t.Fatalf("depth %d", d)
	}
	now := metav1.Now()
	earlier := metav1.NewTime(now.Add(-time.Hour))
	items := []observabilityv1alpha1.NotificationPolicy{
		{ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "x", CreationTimestamp: now}},
		{ObjectMeta: metav1.ObjectMeta{Name: "z", Namespace: "a", CreationTimestamp: earlier}},
		{ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "a", CreationTimestamp: earlier}},
	}
	if w := policyWinner(items); w == nil || w.Namespace != "a" || w.Name != "c" {
		t.Fatalf("winner %+v", w)
	}
	if policyWinner(nil) != nil {
		t.Fatal("empty → nil")
	}
}

func TestNotificationPolicyAccepted(t *testing.T) {
	newFakeTenant(t, "np-tenant", true, false)
	keep := &observabilityv1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "np-keep", Namespace: "default"},
		Spec: observabilityv1alpha1.ContactPointSpec{TenantRef: "np-tenant", Webhook: []observabilityv1alpha1.WebhookConfig{{URL: "http://keep"}}}}
	createAndCleanup(t, keep)

	pol := &observabilityv1alpha1.NotificationPolicy{ObjectMeta: metav1.ObjectMeta{Name: "np-a", Namespace: "default"},
		Spec: observabilityv1alpha1.NotificationPolicySpec{TenantRef: "np-tenant", Route: observabilityv1alpha1.Route{Receiver: "np-keep",
			Routes: []apiextensionsv1.JSON{{Raw: []byte(`{"receiver":"np-later"}`)}}}}}
	createAndCleanup(t, pol)
	waitCondition(t, pol, observabilityv1alpha1.ConditionAccepted, metav1.ConditionFalse, observabilityv1alpha1.ReasonContactPointNotFound)
	if !strings.Contains(findCond(pol, observabilityv1alpha1.ConditionAccepted).Message, "np-later") {
		t.Fatalf("message: %q", findCond(pol, observabilityv1alpha1.ConditionAccepted).Message)
	}

	// ContactPoint appears → policy flips to Accepted via the ContactPoint watch.
	later := &observabilityv1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "np-later", Namespace: "default"},
		Spec: observabilityv1alpha1.ContactPointSpec{TenantRef: "np-tenant", Webhook: []observabilityv1alpha1.WebhookConfig{{URL: "http://later"}}}}
	createAndCleanup(t, later)
	waitCondition(t, pol, observabilityv1alpha1.ConditionAccepted, metav1.ConditionTrue, observabilityv1alpha1.ReasonAccepted)

	// ContactPoint bound to a different tenant does not count.
	otherTenantCP := &observabilityv1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "np-other", Namespace: "default"},
		Spec: observabilityv1alpha1.ContactPointSpec{TenantRef: "np-tenant-2", Webhook: []observabilityv1alpha1.WebhookConfig{{URL: "http://o"}}}}
	createAndCleanup(t, otherTenantCP)
	wrong := &observabilityv1alpha1.NotificationPolicy{ObjectMeta: metav1.ObjectMeta{Name: "np-wrong", Namespace: "default"},
		Spec: observabilityv1alpha1.NotificationPolicySpec{TenantRef: "np-tenant", Route: observabilityv1alpha1.Route{Receiver: "np-other"}}}
	createAndCleanup(t, wrong)
	waitFor(t, func() bool {
		_ = testClient.Get(testCtx, clientKey(wrong), wrong)
		c := findCond(wrong, observabilityv1alpha1.ConditionAccepted)
		return c.Status == metav1.ConditionFalse && (c.Reason == observabilityv1alpha1.ReasonContactPointNotFound || c.Reason == observabilityv1alpha1.ReasonConflict)
	})

	// Second valid policy for the same tenant → Conflict naming the winner.
	second := &observabilityv1alpha1.NotificationPolicy{ObjectMeta: metav1.ObjectMeta{Name: "np-b", Namespace: "default"},
		Spec: observabilityv1alpha1.NotificationPolicySpec{TenantRef: "np-tenant", Route: observabilityv1alpha1.Route{Receiver: "np-keep"}}}
	createAndCleanup(t, second)
	waitCondition(t, second, observabilityv1alpha1.ConditionAccepted, metav1.ConditionFalse, observabilityv1alpha1.ReasonConflict)
	if !strings.Contains(findCond(second, observabilityv1alpha1.ConditionAccepted).Message, "default/np-a") {
		t.Fatalf("conflict message should name winner: %q", findCond(second, observabilityv1alpha1.ConditionAccepted).Message)
	}
	// Winner stays accepted.
	waitCondition(t, pol, observabilityv1alpha1.ConditionAccepted, metav1.ConditionTrue, observabilityv1alpha1.ReasonAccepted)

	// Deleting the winner promotes the second.
	if err := testClient.Delete(testCtx, pol); err != nil {
		t.Fatal(err)
	}
	waitCondition(t, second, observabilityv1alpha1.ConditionAccepted, metav1.ConditionTrue, observabilityv1alpha1.ReasonAccepted)
}

func TestNotificationPolicyInvalidRouteJSON(t *testing.T) {
	newFakeTenant(t, "np-tenant-3", true, false)
	keep := &observabilityv1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "np-keep3", Namespace: "default"},
		Spec: observabilityv1alpha1.ContactPointSpec{TenantRef: "np-tenant-3", Webhook: []observabilityv1alpha1.WebhookConfig{{URL: "http://keep3"}}}}
	createAndCleanup(t, keep)

	// A malformed child route (routes: [42]) fails ChildRoutes() decoding at reconcile time.
	pol := &observabilityv1alpha1.NotificationPolicy{ObjectMeta: metav1.ObjectMeta{Name: "np-badjson", Namespace: "default"},
		Spec: observabilityv1alpha1.NotificationPolicySpec{TenantRef: "np-tenant-3", Route: observabilityv1alpha1.Route{Receiver: "np-keep3",
			Routes: []apiextensionsv1.JSON{{Raw: []byte("42")}}}}}
	createAndCleanup(t, pol)
	waitCondition(t, pol, observabilityv1alpha1.ConditionAccepted, metav1.ConditionFalse, observabilityv1alpha1.ReasonInvalid)
}
