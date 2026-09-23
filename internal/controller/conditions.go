package controller

import (
	"context"
	"time"
	"unicode/utf8"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/antnsn/alerts-operator/api/v1alpha1"
	"github.com/antnsn/alerts-operator/internal/backend"
)

// maxConditionMessage matches metav1.Condition's Message MaxLength (32768), enforced by the
// generated CRD schema, minus headroom for the truncation marker. Spec fields such as
// tenantRef or a rule's expr/interval/for/keep_firing_for have no MaxLength of their own, and
// some backend/parser errors echo the offending input back verbatim, so an oversized value
// could otherwise produce a status patch the API server rejects -- leaving the condition never
// recorded and the object stuck retrying the same failing patch forever. Bounding it centrally
// in setCondition protects every reconciler, not just the ones that remember to do it locally.
const maxConditionMessage = 32768 - 256

// truncateMessage bounds msg to fit metav1.Condition's Message field, truncating on a UTF-8
// rune boundary so the result is always valid.
func truncateMessage(msg string) string {
	if len(msg) <= maxConditionMessage {
		return msg
	}
	const suffix = "… [truncated]"
	cut := maxConditionMessage - len(suffix)
	for cut > 0 && !utf8.RuneStart(msg[cut]) {
		cut--
	}
	return msg[:cut] + suffix
}

// setCondition upserts a condition and reports whether anything changed. msg is truncated to
// fit metav1.Condition's Message MaxLength before being recorded.
func setCondition(conds *[]metav1.Condition, typ string, status metav1.ConditionStatus, reason, msg string, gen int64) bool {
	return meta.SetStatusCondition(conds, metav1.Condition{
		Type: typ, Status: status, Reason: reason, Message: truncateMessage(msg), ObservedGeneration: gen,
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

// requeueOnConflict turns a 409 from an optimistic-lock write into a quiet requeue. The lock is
// doing its job -- a concurrent writer (typically a child's own reconciler patching Accepted in
// the same second the Tenant patches Synced) won the race, and the next pass starts from the fresh
// object -- so it is not a reconciler error: returning it as one made controller-runtime log
// "ERROR Reconciler error" with a stack trace on every burst of child changes, although the retry
// always converged. Any other error is returned unchanged.
func requeueOnConflict(ctx context.Context, err error, what string) (ctrl.Result, error) {
	if err == nil {
		return ctrl.Result{}, nil
	}
	if !apierrors.IsConflict(err) {
		return ctrl.Result{}, err
	}
	log.FromContext(ctx).V(1).Info("optimistic-lock conflict, requeueing", "write", what, "err", err.Error())
	return ctrl.Result{RequeueAfter: conflictRequeueAfter}, nil
}

// conflictRequeueAfter is how soon a conflicted write is retried. Short, because the object the
// next pass needs is already in the API server; long enough that the concurrent writer's own
// watch event has usually been processed first, so the retry does not lose the same race again.
const conflictRequeueAfter = 500 * time.Millisecond

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
