package controller

import (
	"context"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/antnsn/alerts-operator/api/v1alpha1"
	"github.com/antnsn/alerts-operator/internal/backend"
	"github.com/antnsn/alerts-operator/internal/compile"
)

// syncRules makes the backend's rule namespaces under the tenant prefix match the accepted groups.
// Namespaces in keep — backend namespaces of AlertRuleGroups whose Accepted condition is stale for
// the current generation (see staleGeneration) — are left alone this pass: they're skipped in the
// diff loop (in practice desired never contains them, since groups only has accepted-current
// AlertRuleGroups) and, critically, excluded from the prune loop, so a spec edit racing ahead of its
// own child reconciler can never make the Tenant reconciler prune a namespace whose new content just
// hasn't been validated yet. It sets Mimir/LokiRulesSynced on the tenant and Synced on every group.
// The returned error is non-nil only when the backend was unavailable (caller backs off).
func (r *TenantReconciler) syncRules(ctx context.Context, tenant *v1alpha1.Tenant, store backend.RuleStore, be v1alpha1.Backend, groups []v1alpha1.AlertRuleGroup, keep map[string]bool) (int32, error) {
	condType := v1alpha1.ConditionMimirRulesSynced
	if be == v1alpha1.BackendLoki {
		condType = v1alpha1.ConditionLokiRulesSynced
	}
	prefix := tenant.Prefix() + "/"
	desired := compile.Rules(tenant.Prefix(), groups)

	actual, err := store.List(ctx)
	if err != nil {
		status, reason, msg := syncedFromErr(err)
		setCondition(&tenant.Status.Conditions, condType, status, reason, msg, tenant.Generation)
		r.Recorder.Eventf(tenant, corev1.EventTypeWarning, "RulesListFailed", "%s: %v", be, err)
		for i := range groups {
			r.setChildSynced(ctx, &groups[i], err)
		}
		if backend.IsUnavailable(err) {
			return 0, err
		}
		return 0, nil // 4xx: wait for the next change or resync, no backoff loop
	}

	nsErr := map[string]error{}
	var count int32
	for ns, want := range desired {
		if keep[ns] {
			continue
		}
		count += int32(len(want))
		have := actual[ns]
		if compile.RulesEqual(have, want) {
			continue
		}
		haveByName := map[string]backend.RuleGroup{}
		for _, g := range have {
			haveByName[g.Name] = g
		}
		wantByName := map[string]bool{}
		for _, g := range want {
			wantByName[g.Name] = true
			if h, ok := haveByName[g.Name]; ok && compile.RulesEqual([]backend.RuleGroup{h}, []backend.RuleGroup{g}) {
				continue
			}
			if err := store.SetGroup(ctx, ns, g); err != nil {
				nsErr[ns] = err
				break
			}
		}
		if nsErr[ns] != nil {
			continue
		}
		for name := range haveByName {
			if !wantByName[name] {
				if err := store.DeleteGroup(ctx, ns, name); err != nil {
					nsErr[ns] = err
					break
				}
			}
		}
	}
	// Note: if spec.rulesNamespacePrefix changes, namespaces under the OLD prefix fall outside both
	// `desired` and this HasPrefix check and so are never pruned -- deliberately: this loop only
	// ever touches what it can positively confirm it owns under the tenant's *current* prefix, per
	// the ownership rule above. Reclaiming the old prefix's namespaces would require trusting that a
	// prefix change is a same-tenant rename rather than, say, a deliberate handoff/abandonment, which
	// this reconciler has no way to distinguish -- getting that wrong means deleting rules someone
	// else now owns. Treated as a known limitation of a rare, deliberate admin action, not fixed here.
	var pruneErrs []error
	for ns := range actual {
		if keep[ns] {
			continue
		}
		if _, wanted := desired[ns]; strings.HasPrefix(ns, prefix) && !wanted {
			if err := store.DeleteNamespace(ctx, ns); err != nil {
				pruneErrs = append(pruneErrs, err)
			}
		}
	}

	var all []error
	for i := range groups {
		ns := compile.BackendNamespace(tenant.Prefix(), groups[i].Namespace, groups[i].Name)
		r.setChildSynced(ctx, &groups[i], nsErr[ns])
		if nsErr[ns] != nil {
			all = append(all, nsErr[ns])
			r.Recorder.Eventf(&groups[i], corev1.EventTypeWarning, "SyncFailed", "%s: %v", be, nsErr[ns])
		}
	}
	all = append(all, pruneErrs...)
	worst := worstErr(all)
	status, reason, msg := syncedFromErr(worst)
	setCondition(&tenant.Status.Conditions, condType, status, reason, msg, tenant.Generation)
	if worst != nil && backend.IsUnavailable(worst) {
		return count, worst
	}
	return count, nil
}

// worstErr prefers an unavailable error (retryable) over a rejection.
func worstErr(errs []error) error {
	var first error
	for _, e := range errs {
		if e == nil {
			continue
		}
		if backend.IsUnavailable(e) {
			return e
		}
		if first == nil {
			first = e
		}
	}
	return first
}

// setChildSynced patches Synced on a child from a backend sync error, classified via syncedFromErr
// (built for backend/transport errors — see IsUnavailable's "anything that isn't a *StatusError
// counts as unavailable" default). Patch failures are logged, not returned: the next Tenant
// reconcile repeats the patch.
func (r *TenantReconciler) setChildSynced(ctx context.Context, obj v1alpha1.Conditioned, syncErr error) {
	status, reason, msg := syncedFromErr(syncErr)
	r.setChildStatus(ctx, obj, status, reason, msg)
}

// setChildStatus patches Synced on a child to an explicit status/reason/message. Patch failures are
// logged, not returned: the next Tenant reconcile repeats the patch.
//
// The dedup check compares ObservedGeneration in addition to status/reason/message: without that, a
// child edited to new content that happens to sync to the same outcome as before (e.g. True/Synced/""
// both times) would skip the patch and never get its Synced condition's ObservedGeneration bumped to
// the new generation, leaving it permanently stale even though the object really was re-verified.
func (r *TenantReconciler) setChildStatus(ctx context.Context, obj v1alpha1.Conditioned, status metav1.ConditionStatus, reason, msg string) {
	conds := obj.GetConditions()
	if c := findCondition(conds, v1alpha1.ConditionSynced); c != nil && c.Status == status && c.Reason == reason && c.Message == msg && c.ObservedGeneration == obj.GetGeneration() {
		return
	}
	err := patchStatus(ctx, r.Client, obj, func() {
		conds := obj.GetConditions()
		setCondition(&conds, v1alpha1.ConditionSynced, status, reason, msg, obj.GetGeneration())
		obj.SetConditions(conds)
	}, nil) // the early return above already guarantees a real change
	if err != nil {
		log.FromContext(ctx).Info("failed to patch Synced on child", "object", obj.GetNamespace()+"/"+obj.GetName(), "err", err.Error())
	}
}

func findCondition(conds []metav1.Condition, typ string) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == typ {
			return &conds[i]
		}
	}
	return nil
}
