package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

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

// TestSetConditionTruncatesMessage guards against metav1.Condition's Message MaxLength=32768
// (enforced by the generated CRD schema): callers' spec fields (e.g. AlertRuleGroup's
// tenantRef or a rule's expr) have no MaxLength of their own, and some backend/parser errors
// echo the offending input back verbatim, so an oversized message could otherwise produce a
// status patch the API server rejects, leaving the condition never recorded. setCondition
// truncates centrally so every reconciler is protected, not just the ones that remember to.
func TestSetConditionTruncatesMessage(t *testing.T) {
	var conds []metav1.Condition
	long := strings.Repeat("x", maxConditionMessage+1000)
	if !setCondition(&conds, observabilityv1alpha1.ConditionAccepted, metav1.ConditionFalse, observabilityv1alpha1.ReasonInvalidRule, long, 1) {
		t.Fatal("first set should report change")
	}
	if len(conds[0].Message) > maxConditionMessage {
		t.Fatalf("condition message length %d exceeds max %d", len(conds[0].Message), maxConditionMessage)
	}
	if !strings.HasSuffix(conds[0].Message, "… [truncated]") {
		t.Fatalf("truncated message should carry a marker suffix, got tail %q", conds[0].Message[len(conds[0].Message)-30:])
	}

	short := "group g rule 0: expr: parse error"
	if !setCondition(&conds, observabilityv1alpha1.ConditionAccepted, metav1.ConditionFalse, observabilityv1alpha1.ReasonInvalidRule, short, 2) {
		t.Fatal("message change should report change")
	}
	if conds[0].Message != short {
		t.Fatalf("short message should pass through unchanged, got %q", conds[0].Message)
	}
}

// TestTruncateMessage covers the helper directly, in addition to TestSetConditionTruncatesMessage
// exercising it through setCondition.
func TestTruncateMessage(t *testing.T) {
	short := "group g rule 0: expr: parse error"
	if got := truncateMessage(short); got != short {
		t.Fatalf("short message should be unchanged, got %q", got)
	}
	long := strings.Repeat("x", maxConditionMessage+1000)
	got := truncateMessage(long)
	if len(got) > maxConditionMessage {
		t.Fatalf("truncated message length %d exceeds max %d", len(got), maxConditionMessage)
	}
	if !strings.HasSuffix(got, "… [truncated]") {
		t.Fatalf("truncated message should carry a marker suffix, got tail %q", got[len(got)-30:])
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

// TestSetConditionKeepsLastTransitionTimeWhenStatusUnchanged: re-applying a condition whose
// Status did not change must not move lastTransitionTime, even when reason/message/generation
// differ; only a Status flip is a transition. (Bead d2e, secondary symptom.)
func TestSetConditionKeepsLastTransitionTimeWhenStatusUnchanged(t *testing.T) {
	var conds []metav1.Condition
	setCondition(&conds, "Synced", metav1.ConditionTrue, "Synced", "", 1)
	past := metav1.NewTime(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	conds[0].LastTransitionTime = past

	setCondition(&conds, "Synced", metav1.ConditionTrue, "Synced", "", 1)
	if !conds[0].LastTransitionTime.Equal(&past) {
		t.Fatalf("identical re-apply moved lastTransitionTime to %v", conds[0].LastTransitionTime)
	}
	setCondition(&conds, "Synced", metav1.ConditionTrue, "Synced", "re-verified", 2)
	if !conds[0].LastTransitionTime.Equal(&past) {
		t.Fatalf("same-status re-apply with new message/generation moved lastTransitionTime to %v", conds[0].LastTransitionTime)
	}
	if conds[0].ObservedGeneration != 2 || conds[0].Message != "re-verified" {
		t.Fatalf("message/generation must still update, got %+v", conds[0])
	}
	setCondition(&conds, "Synced", metav1.ConditionFalse, "Rejected", "boom", 2)
	if conds[0].LastTransitionTime.Equal(&past) {
		t.Fatal("a real status flip must move lastTransitionTime")
	}
}
