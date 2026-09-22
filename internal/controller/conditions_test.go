package controller

import (
	"context"
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	observabilityv1alpha1 "github.com/antnsn/alerts-operator/api/v1alpha1"
	"github.com/antnsn/alerts-operator/internal/backend"
)

func TestSetConditionReportsChange(t *testing.T) {
	var conds []metav1.Condition
	if !setCondition(&conds, observabilityv1alpha1.ConditionAccepted, metav1.ConditionTrue, observabilityv1alpha1.ReasonAccepted, "", 1) {
		t.Fatal("first set should report change")
	}
	if setCondition(&conds, observabilityv1alpha1.ConditionAccepted, metav1.ConditionTrue, observabilityv1alpha1.ReasonAccepted, "", 1) {
		t.Fatal("identical set should not report change")
	}
	if !setCondition(&conds, observabilityv1alpha1.ConditionAccepted, metav1.ConditionFalse, observabilityv1alpha1.ReasonTenantNotFound, "x", 2) {
		t.Fatal("status change should report change")
	}
	if conds[0].ObservedGeneration != 2 || conds[0].Message != "x" {
		t.Fatalf("condition not updated: %+v", conds[0])
	}
}

func TestMapToTenant(t *testing.T) {
	cp := &observabilityv1alpha1.ContactPoint{Spec: observabilityv1alpha1.ContactPointSpec{TenantRef: "t1"}}
	pol := &observabilityv1alpha1.NotificationPolicy{Spec: observabilityv1alpha1.NotificationPolicySpec{TenantRef: "t2"}}
	arg := &observabilityv1alpha1.AlertRuleGroup{Spec: observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: "t3"}}
	if got := mapToTenant(context.Background(), cp); len(got) != 1 || got[0].Name != "t1" || got[0].Namespace != "" {
		t.Fatalf("cp: %v", got)
	}
	if got := mapToTenant(context.Background(), pol); len(got) != 1 || got[0].Name != "t2" {
		t.Fatalf("pol: %v", got)
	}
	if got := mapToTenant(context.Background(), arg); len(got) != 1 || got[0].Name != "t3" {
		t.Fatalf("arg: %v", got)
	}
	if got := mapToTenant(context.Background(), &observabilityv1alpha1.Tenant{}); got != nil {
		t.Fatalf("tenant should not map: %v", got)
	}
}

func TestPatchStatusSkipsWhenUnchanged(t *testing.T) {
	obj := &observabilityv1alpha1.Tenant{}
	obj.Name = "never-created"
	unchanged := false
	// Would fail with NotFound if a patch were sent; a skipped patch returns nil.
	if err := patchStatus(testCtx, testClient, obj, func() {}, &unchanged); err != nil {
		t.Fatalf("unchanged patch should be skipped: %v", err)
	}
	changed := true
	if err := patchStatus(testCtx, testClient, obj, func() {}, &changed); err == nil {
		t.Fatal("changed patch must be sent (and fail NotFound here)")
	}
}

func TestSyncedFromErr(t *testing.T) {
	cases := []struct {
		err    error
		status metav1.ConditionStatus
		reason string
	}{
		{nil, metav1.ConditionTrue, observabilityv1alpha1.ReasonSynced},
		{&backend.StatusError{Status: 503}, metav1.ConditionFalse, observabilityv1alpha1.ReasonBackendUnavailable},
		{errors.New("dial tcp: refused"), metav1.ConditionFalse, observabilityv1alpha1.ReasonBackendUnavailable},
		{&backend.StatusError{Status: 400, Body: "bad"}, metav1.ConditionFalse, observabilityv1alpha1.ReasonRejected},
	}
	for _, c := range cases {
		s, r, _ := syncedFromErr(c.err)
		if s != c.status || r != c.reason {
			t.Fatalf("%v → %s/%s", c.err, s, r)
		}
	}
}
