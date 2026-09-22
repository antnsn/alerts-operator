package controller

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	observabilityv1alpha1 "github.com/antnsn/alerts-operator/api/v1alpha1"
)

func TestTenantReadyWithoutChildren(t *testing.T) {
	// Loki-only: no Alertmanager, so Ready does not depend on a NotificationPolicy.
	tn, _ := newFakeTenant(t, "tn-loki-only", false, true)
	waitCondition(t, tn, observabilityv1alpha1.ConditionReady, metav1.ConditionTrue, "")
	if !controllerutil.ContainsFinalizer(tn, tenantFinalizer) {
		t.Fatalf("finalizer missing: %v", tn.Finalizers)
	}
	if tn.Status.ObservedGeneration != tn.Generation {
		t.Fatalf("observedGeneration %d != %d", tn.Status.ObservedGeneration, tn.Generation)
	}
	mimirCond := findCond(tn, observabilityv1alpha1.ConditionMimirRulesSynced)
	if mimirCond.Status != metav1.ConditionUnknown || mimirCond.Reason != observabilityv1alpha1.ReasonNotConfigured {
		t.Fatalf("unconfigured backend should be Unknown/NotConfigured: %+v", mimirCond)
	}
	amCond := findCond(tn, observabilityv1alpha1.ConditionAlertmanagerSynced)
	if amCond.Status != metav1.ConditionUnknown || amCond.Reason != observabilityv1alpha1.ReasonNotConfigured {
		t.Fatalf("unconfigured alertmanager should be Unknown/NotConfigured: %+v", amCond)
	}
}

func TestTenantListChildrenFiltersAccepted(t *testing.T) {
	tn, _ := newFakeTenant(t, "tn-children", true, true)
	good := &observabilityv1alpha1.AlertRuleGroup{ObjectMeta: metav1.ObjectMeta{Name: "tn-good", Namespace: "default"},
		Spec: observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: tn.Name, Backend: "mimir", Groups: ruleGroups("up == 0")}}
	bad := &observabilityv1alpha1.AlertRuleGroup{ObjectMeta: metav1.ObjectMeta{Name: "tn-bad", Namespace: "default"},
		Spec: observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: tn.Name, Backend: "mimir", Groups: ruleGroups("up{")}}
	lokiG := &observabilityv1alpha1.AlertRuleGroup{ObjectMeta: metav1.ObjectMeta{Name: "tn-loki", Namespace: "default"},
		Spec: observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: tn.Name, Backend: "loki", Groups: ruleGroups(`{a="b"}`)}}
	cp := &observabilityv1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "tn-cp", Namespace: "default"},
		Spec: observabilityv1alpha1.ContactPointSpec{TenantRef: tn.Name, Webhook: []observabilityv1alpha1.WebhookConfig{{URL: "http://x"}}}}
	pol := &observabilityv1alpha1.NotificationPolicy{ObjectMeta: metav1.ObjectMeta{Name: "tn-pol", Namespace: "default"},
		Spec: observabilityv1alpha1.NotificationPolicySpec{TenantRef: tn.Name, Route: observabilityv1alpha1.Route{Receiver: "tn-cp"}}}
	for _, o := range []observabilityv1alpha1.Conditioned{good, bad, lokiG, cp, pol} {
		createAndCleanup(t, o)
	}
	waitCondition(t, good, observabilityv1alpha1.ConditionAccepted, metav1.ConditionTrue, "")
	waitCondition(t, bad, observabilityv1alpha1.ConditionAccepted, metav1.ConditionFalse, "")
	waitCondition(t, lokiG, observabilityv1alpha1.ConditionAccepted, metav1.ConditionTrue, "")
	waitCondition(t, pol, observabilityv1alpha1.ConditionAccepted, metav1.ConditionTrue, "")

	// listChildren queries via client.MatchingFields (index.IndexTenantRef), which is a
	// controller-runtime cache feature: it must go through the cache-backed testCacheClient,
	// not the uncached testClient (see suite_test.go), and cache reads are eventually
	// consistent with the writes just observed via waitCondition above, hence waitFor.
	r := &TenantReconciler{Client: testCacheClient}
	var ch *children
	waitFor(t, func() bool {
		var err error
		ch, err = r.listChildren(testCtx, tn)
		return err == nil && len(ch.MimirGroups) == 1 && len(ch.LokiGroups) == 1 && len(ch.ContactPoints) == 1 && ch.Policy != nil
	})
	if ch.MimirGroups[0].Name != "tn-good" {
		t.Fatalf("mimir groups: %+v", ch.MimirGroups)
	}
	if ch.Policy.Name != "tn-pol" {
		t.Fatalf("children: %+v", ch)
	}
}
