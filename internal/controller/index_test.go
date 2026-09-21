package controller

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	observabilityv1alpha1 "github.com/antnsn/alerts-operator/api/v1alpha1"
	"github.com/antnsn/alerts-operator/internal/index"
)

func TestIndexers(t *testing.T) {
	cpA := &observabilityv1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "idx-a", Namespace: "default"}, Spec: observabilityv1alpha1.ContactPointSpec{TenantRef: "idx-tenant-1",
		Pushover: []observabilityv1alpha1.PushoverConfig{{UserKeySecretRef: observabilityv1alpha1.SecretKeyRef{Name: "po", Key: "user"}, TokenSecretRef: observabilityv1alpha1.SecretKeyRef{Name: "po", Key: "token"}}}}}
	cpB := &observabilityv1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "idx-b", Namespace: "default"}, Spec: observabilityv1alpha1.ContactPointSpec{TenantRef: "idx-tenant-2",
		Webhook: []observabilityv1alpha1.WebhookConfig{{URLSecretRef: &observabilityv1alpha1.SecretKeyRef{Name: "hook", Key: "url"}}}}}
	arg := &observabilityv1alpha1.AlertRuleGroup{ObjectMeta: metav1.ObjectMeta{Name: "idx-arg", Namespace: "default"}, Spec: observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: "idx-tenant-1", Backend: "mimir",
		Groups: []observabilityv1alpha1.RuleGroup{{Name: "g", Rules: []observabilityv1alpha1.Rule{{Alert: "A", Expr: "up == 0"}}}}}}
	pol := &observabilityv1alpha1.NotificationPolicy{ObjectMeta: metav1.ObjectMeta{Name: "idx-pol", Namespace: "default"}, Spec: observabilityv1alpha1.NotificationPolicySpec{TenantRef: "idx-tenant-1", Route: observabilityv1alpha1.Route{Receiver: "idx-a"}}}
	for _, o := range []client.Object{cpA, cpB, arg, pol} {
		if err := testClient.Create(testCtx, o); err != nil {
			t.Fatal(err)
		}
		defer func(o client.Object) { _ = testClient.Delete(testCtx, o) }(o)
	}

	waitFor(t, func() bool {
		var cps observabilityv1alpha1.ContactPointList
		if err := testClient.List(testCtx, &cps, client.MatchingFields{index.IndexTenantRef: "idx-tenant-1"}); err != nil {
			return false
		}
		return len(cps.Items) == 1 && cps.Items[0].Name == "idx-a"
	})
	waitFor(t, func() bool {
		var cps observabilityv1alpha1.ContactPointList
		if err := testClient.List(testCtx, &cps, client.InNamespace("default"), client.MatchingFields{index.IndexSecretRefs: "po"}); err != nil {
			return false
		}
		return len(cps.Items) == 1 && cps.Items[0].Name == "idx-a"
	})
	waitFor(t, func() bool {
		var cps observabilityv1alpha1.ContactPointList
		if err := testClient.List(testCtx, &cps, client.MatchingFields{index.IndexSecretRefs: "hook"}); err != nil {
			return false
		}
		return len(cps.Items) == 1 && cps.Items[0].Name == "idx-b"
	})
	waitFor(t, func() bool {
		var args observabilityv1alpha1.AlertRuleGroupList
		var pols observabilityv1alpha1.NotificationPolicyList
		if err := testClient.List(testCtx, &args, client.MatchingFields{index.IndexTenantRef: "idx-tenant-1"}); err != nil {
			return false
		}
		if err := testClient.List(testCtx, &pols, client.MatchingFields{index.IndexTenantRef: "idx-tenant-1"}); err != nil {
			return false
		}
		return len(args.Items) == 1 && len(pols.Items) == 1
	})
}
