package controller

import (
	"context"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/antnsn/alerts-operator/api/v1alpha1"
	"github.com/antnsn/alerts-operator/internal/backend"
)

// setCondition upserts a condition and reports whether anything changed.
func setCondition(conds *[]metav1.Condition, typ string, status metav1.ConditionStatus, reason, msg string, gen int64) bool {
	return meta.SetStatusCondition(conds, metav1.Condition{
		Type: typ, Status: status, Reason: reason, Message: msg, ObservedGeneration: gen,
	})
}

// patchStatus applies mutate to obj and merge-patches the status subresource.
// The optimistic lock makes a concurrent status writer (e.g. the Tenant reconciler
// setting Synced while a child sets Accepted) surface as a conflict → requeue,
// instead of one side silently overwriting the other's conditions.
//
// mutate reports through *changed whether anything differs; when it says false the
// patch is skipped. This matters: an optimistic-lock patch always carries
// metadata.resourceVersion, so an unconditional patch bumps the object, fires a
// watch event and re-enqueues every controller watching it — a hot loop.
func patchStatus(ctx context.Context, c client.Client, obj client.Object, mutate func(), changed *bool) error {
	base := obj.DeepCopyObject().(client.Object)
	mutate()
	if changed != nil && !*changed {
		return nil
	}
	return c.Status().Patch(ctx, obj, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
}

func tenantRequest(name string) reconcile.Request {
	return reconcile.Request{NamespacedName: types.NamespacedName{Name: name}}
}

// tenantRefOf returns spec.tenantRef for the namespaced kinds, "" otherwise.
func tenantRefOf(o client.Object) string {
	switch t := o.(type) {
	case *v1alpha1.ContactPoint:
		return t.Spec.TenantRef
	case *v1alpha1.NotificationPolicy:
		return t.Spec.TenantRef
	case *v1alpha1.AlertRuleGroup:
		return t.Spec.TenantRef
	}
	return ""
}

// mapToTenant enqueues the Tenant a child references. Works for delete events too,
// since the event object still carries spec.
func mapToTenant(_ context.Context, o client.Object) []reconcile.Request {
	if name := tenantRefOf(o); name != "" {
		return []reconcile.Request{tenantRequest(name)}
	}
	return nil
}

// syncedFromErr maps a backend error to a Synced-style condition.
func syncedFromErr(err error) (metav1.ConditionStatus, string, string) {
	switch {
	case err == nil:
		return metav1.ConditionTrue, v1alpha1.ReasonSynced, ""
	case backend.IsRejected(err):
		return metav1.ConditionFalse, v1alpha1.ReasonRejected, err.Error()
	case backend.IsUnavailable(err):
		return metav1.ConditionFalse, v1alpha1.ReasonBackendUnavailable, err.Error()
	default:
		return metav1.ConditionFalse, v1alpha1.ReasonInvalid, err.Error()
	}
}
