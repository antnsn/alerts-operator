package controller

import (
	"encoding/json"
	"fmt"
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

// nestedRouteJSON builds a Route JSON blob that nests depth levels deep (a single child chain),
// root counted as depth 1.
func nestedRouteJSON(depth int) []byte {
	if depth <= 1 {
		return []byte(`{"receiver":"leaf"}`)
	}
	return []byte(fmt.Sprintf(`{"receiver":"r","routes":[%s]}`, nestedRouteJSON(depth-1)))
}

// TestRouteDepthCappedForDeepTree guards against the recursion in routeDepth fully decoding an
// attacker-sized route tree before validate ever gets to compare it against maxRouteDepth (see
// Codex review finding on notificationpolicy_controller.go). A route tree far deeper than
// maxRouteDepth must report a depth beyond the limit without walking (and re-decoding) the whole
// tree: if the cap weren't applied, the returned depth would equal realDepth instead of being
// capped near maxRouteDepth.
func TestRouteDepthCappedForDeepTree(t *testing.T) {
	const realDepth = 500
	var root observabilityv1alpha1.Route
	if err := json.Unmarshal(nestedRouteJSON(realDepth), &root); err != nil {
		t.Fatal(err)
	}
	d, err := routeDepth(&root)
	if err != nil {
		t.Fatal(err)
	}
	if want := maxRouteDepth + 2; d != want {
		t.Fatalf("expected capped depth %d (not the real depth %d), got %d", want, realDepth, d)
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

	// Second valid policy for the same tenant → Conflict naming the winner (np-a, still the oldest).
	//
	// Created (and named) before "wrong" below on purpose: CreationTimestamp only has 1s
	// resolution, so after np-a is deleted, policyWinner among {second, wrong} must not depend on
	// which wall-clock second either Create happened to land in. Creating "second" first makes its
	// CreationTimestamp never later than "wrong"'s, and "np-b" also sorts before "np-wrong"
	// lexicographically, so it wins the post-promotion race whether or not the two Creates tie.
	second := &observabilityv1alpha1.NotificationPolicy{ObjectMeta: metav1.ObjectMeta{Name: "np-b", Namespace: "default"},
		Spec: observabilityv1alpha1.NotificationPolicySpec{TenantRef: "np-tenant", Route: observabilityv1alpha1.Route{Receiver: "np-keep"}}}
	createAndCleanup(t, second)
	waitCondition(t, second, observabilityv1alpha1.ConditionAccepted, metav1.ConditionFalse, observabilityv1alpha1.ReasonConflict)
	if !strings.Contains(findCond(second, observabilityv1alpha1.ConditionAccepted).Message, "default/np-a") {
		t.Fatalf("conflict message should name winner: %q", findCond(second, observabilityv1alpha1.ConditionAccepted).Message)
	}
	// Winner stays accepted.
	waitCondition(t, pol, observabilityv1alpha1.ConditionAccepted, metav1.ConditionTrue, observabilityv1alpha1.ReasonAccepted)

	// ContactPoint bound to a different tenant does not count. "wrong" is created after "second"
	// (see comment above) so it can never outrank "second" for the post-promotion winner slot.
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

	// Deleting the winner promotes "second" (deterministically: see comment above).
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
