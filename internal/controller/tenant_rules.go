package controller

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/antnsn/alerts-operator/api/v1alpha1"
	"github.com/antnsn/alerts-operator/internal/backend"
	"github.com/antnsn/alerts-operator/internal/compile"
	"github.com/antnsn/alerts-operator/internal/metrics"
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
	target := metrics.TargetMimirRules
	if be == v1alpha1.BackendLoki {
		condType = v1alpha1.ConditionLokiRulesSynced
		target = metrics.TargetLokiRules
	}
	desired := compile.Rules(be, tenant.Prefix(), groups)
	// Computed once, up front: this is "how many groups the accepted AlertRuleGroups call for," not
	// a backend-confirmed count -- the diff loop below reports the same desired total even when some
	// per-namespace writes fail, so a List failure must report it too rather than 0. Zeroing it out
	// on a transient list error would misreport "no rule groups" in status while the groups most
	// likely remain installed exactly as before; we just couldn't confirm that this pass.
	var desiredCount int32
	for ns, want := range desired {
		if !keep[ns] {
			desiredCount += int32(len(want))
		}
	}

	actual, err := store.List(ctx)
	if err != nil {
		status, reason, msg := syncedFromErr(err)
		setCondition(&tenant.Status.Conditions, condType, status, reason, msg, tenant.Generation)
		r.Recorder.Eventf(tenant, corev1.EventTypeWarning, "RulesListFailed", "%s: %v", be, err)
		for i := range groups {
			r.setChildSynced(ctx, &groups[i], err)
		}
		metrics.Observe(tenant.Name, target, err)
		if backend.IsUnavailable(err) {
			return desiredCount, err
		}
		return desiredCount, nil // 4xx: wait for the next change or resync, no backoff loop
	}

	nsErr := map[string]error{}
	// kept tracks whether this pass actually excluded a namespace present in the backend because of
	// keep (as opposed to keep simply containing no-op entries for the *other* backend's namespaces
	// -- keep is shared across both syncRules calls in one Reconcile, and desired never contains a
	// kept namespace at all, see the diff loop below, so only the prune loop's `range actual` can
	// ever observe a real hit for this backend).
	var kept bool
	for ns, want := range desired {
		if keep[ns] {
			kept = true
			continue
		}
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
	// This loop only ever touches what it can positively confirm it owns under the tenant's
	// *current* prefix. spec.tenantId and spec.rulesNamespacePrefix are both CEL-immutable
	// (api/v1alpha1/tenant_types.go) precisely so that "current org + current prefix" is also the
	// only combination this Tenant has ever used: were either field mutable, namespaces under the
	// OLD one would fall outside both `desired` and this ownership check and so would never be
	// pruned, silently orphaning them (duplicate, un-pruned rules/alerts indefinitely) -- and
	// blindly reclaiming an old prefix on a rename would risk deleting rules that now belong to a
	// different tenant/purpose, which this reconciler has no way to distinguish from a rename.
	//
	// Immutability pins what THIS Tenant owned in the past; conflictingTenant covers the other half
	// -- whether some OTHER Tenant CR owns the same namespaces right now. Only the namespace prune
	// needs that guard: a namespace in `desired` is derived from one of our own children
	// (<prefix><sep><child namespace><sep><name>), and a child has exactly one tenantRef, so no other
	// Tenant can desire it and the per-group deletes in the diff loop above are never ambiguous.
	conflict, conflictErr := r.conflictingTenant(ctx, tenant, be, true)
	var pruneErrs []error
	switch {
	case conflictErr != nil:
		// Ownership could not be established at all. Same rule as a real collision -- never delete
		// on an unverified claim -- but recorded as a failure so the pass is retried rather than
		// reported as complete.
		pruneErrs = append(pruneErrs, fmt.Errorf("cannot verify rule namespace ownership: %w", conflictErr))
	case conflict != "":
		// Another Tenant computes these same namespaces as "mine, but not desired" and would delete
		// ours, as we would delete its: writes are convergent, deletes are not. Skip the prune
		// entirely for this backend. The cost is a genuinely orphaned namespace surviving until
		// someone removes it by hand; the alternative is deleting live alert rules.
		r.Recorder.Eventf(tenant, corev1.EventTypeWarning, "RuleNamespaceOwnershipConflict",
			"%s: Tenant %q claims the same rule namespaces (tenantId %q, address %s, prefix %q); pruning skipped",
			be, conflict, tenant.Spec.TenantID, backendAddress(tenant, be), tenant.Prefix())
	default:
		for ns := range actual {
			if keep[ns] {
				kept = true
				continue
			}
			// compile.OwnsNamespace, never a bare strings.HasPrefix on the unslashed prefix: the
			// match has to land on a segment boundary with *this backend's* separator ("/" for
			// Mimir, "_" for Loki), or "alerts-operator" would also claim -- and delete --
			// "alerts-operator-other/default/x" / "alerts-operator-other_default_x".
			if _, wanted := desired[ns]; compile.OwnsNamespace(be, tenant.Prefix(), ns) && !wanted {
				if err := store.DeleteNamespace(ctx, ns); err != nil {
					pruneErrs = append(pruneErrs, err)
				}
			}
		}
	}

	var all []error
	for i := range groups {
		ns := compile.BackendNamespace(be, tenant.Prefix(), groups[i].Namespace, groups[i].Name)
		r.setChildSynced(ctx, &groups[i], nsErr[ns])
		if nsErr[ns] != nil {
			all = append(all, nsErr[ns])
			r.Recorder.Eventf(&groups[i], corev1.EventTypeWarning, "SyncFailed", "%s: %v", be, nsErr[ns])
		}
	}
	all = append(all, pruneErrs...)
	worst := worstErr(all)
	status, reason, msg := syncedFromErr(worst)
	switch {
	case worst != nil:
		// A live backend failure is the more actionable signal; syncedFromErr's verdict stands.
	case conflict != "":
		// Not Pending: unlike a stale-generation child this does not resolve on its own, and unlike
		// a backend error it will not clear on a retry. It is a misconfiguration an operator has to
		// resolve, so it reports False (which propagates to Ready) and names the other claimant.
		status, reason, msg = metav1.ConditionFalse, v1alpha1.ReasonConflict, fmt.Sprintf(
			"Tenant %q claims the same %s rule namespaces (tenantId %q, address %s, rulesNamespacePrefix %q); "+
				"this Tenant's own groups were written but pruning is disabled until exactly one Tenant claims them",
			conflict, be, tenant.Spec.TenantID, backendAddress(tenant, be), tenant.Prefix())
	case kept:
		// A stale-generation AlertRuleGroup's namespace exists in the backend and was excluded from
		// this pass's diff/prune (see keep's doc comment): we genuinely don't know whether it matches
		// its just-edited spec yet. Reporting True here would claim completeness this pass never
		// established for that namespace -- report the same Unknown/Pending signal syncAlertmanager
		// uses for its own stale-generation case (PendingAM), which Ready's aggregation already knows
		// how to handle (preserve the prior Ready value rather than jumping to True).
		status, reason, msg = metav1.ConditionUnknown, v1alpha1.ReasonPending, "waiting for child validation"
	}
	setCondition(&tenant.Status.Conditions, condType, status, reason, msg, tenant.Generation)
	metrics.Observe(tenant.Name, target, worst)
	if worst != nil && backend.IsUnavailable(worst) {
		return desiredCount, worst
	}
	return desiredCount, nil
}

// conflictingTenant returns the name of another Tenant CR whose claim on one backend target is
// indistinguishable from this one's, or "" when this Tenant is the sole claimant. withPrefix
// selects which of the two ownership keys is meant; see sharesBackendTarget.
//
// Nothing in the Mimir or Loki API carries the owning Tenant, and both the namespace scheme and the
// Alertmanager document's address are fixed by the design, so two Tenants on the same key are
// genuinely indistinguishable in the backend -- the collision can only be caught here, at reconcile
// time, by looking at the CRs.
//
// Mimir and Loki are evaluated independently: two Tenants sharing one Mimir may have separate Lokis
// or none at all, so a collision on one backend must not stop the other's prune.
//
// Terminating Tenants still count: their backend state stays live until their own finalizer removes
// it, and the CR disappearing is what clears the conflict.
func (r *TenantReconciler) conflictingTenant(ctx context.Context, tenant *v1alpha1.Tenant, be v1alpha1.Backend, withPrefix bool) (string, error) {
	if backendAddress(tenant, be) == "" {
		return "", nil // backend not configured: syncRules/syncAlertmanager aren't called for it
	}
	var tenants v1alpha1.TenantList
	if err := r.List(ctx, &tenants); err != nil {
		return "", err
	}
	// Lowest name wins rather than first-listed, so the reported name is stable across reconciles
	// when three or more Tenants collide (a name that flapped would rewrite the condition message
	// and re-enqueue on every pass).
	conflict := ""
	for i := range tenants.Items {
		other := &tenants.Items[i]
		if !sharesBackendTarget(tenant, other, be, withPrefix) {
			continue
		}
		if conflict == "" || other.Name < conflict {
			conflict = other.Name
		}
	}
	return conflict, nil
}

// sharesBackendTarget reports whether other is a different Tenant CR that owns the same backend
// target as tenant on backend be. It is the single definition of "the same thing in the backend"
// in this codebase: the steady-state guards (conflictingTenant, for the rule-namespace prune and
// the Alertmanager write) and the delete-time guard (claimants, tenant_finalizer.go) must agree on
// it, or one half would refuse to write what the other half is willing to delete.
//
// Two keys, deliberately not the same one -- withPrefix selects between them:
//
//   - withPrefix=true, rule namespaces: (tenantId, backend address, effective prefix). That is
//     exactly what decides both what store.List returns and what the prune loop reads as "mine":
//     X-Scope-OrgID comes from spec.tenantId, the endpoint from spec.{mimir,loki}.address, and the
//     <prefix><sep><k8s namespace><sep><name> scheme from Prefix() (sep is "/" on Mimir and "_" on
//     Loki -- see compile.BackendNamespace). Two Tenants with different prefixes genuinely do not
//     collide here.
//   - withPrefix=false, the Alertmanager document: (tenantId, backend address) only. GET/POST/
//     DELETE /api/v1/alerts is scoped solely by X-Scope-OrgID and the backend, so two Tenants
//     sharing those share one live document even when their prefixes differ -- and
//     rulesNamespacePrefix exists precisely to let several Tenants share one org, so that pair is
//     a natural configuration rather than an exotic one.
//
// Compared on Prefix(), not spec.rulesNamespacePrefix: an unset field and an explicit
// "alerts-operator" name the same namespaces (a Tenant stored before +kubebuilder:default existed
// reads back defaulted, but a raw-field comparison would still miss the pair).
//
// The address match is textual, so two spellings of one endpoint (an IP and a DNS name, two
// Services in front of the same ruler) are not detected; only a trailing slash is normalised away
// (backendAddress). That residual case is the one the design's "one Tenant per (org, backend)"
// expectation covers.
func sharesBackendTarget(tenant, other *v1alpha1.Tenant, be v1alpha1.Backend, withPrefix bool) bool {
	if other.Name == tenant.Name {
		return false
	}
	if other.Spec.TenantID != tenant.Spec.TenantID {
		return false
	}
	addr := backendAddress(tenant, be)
	if addr == "" || backendAddress(other, be) != addr {
		return false
	}
	return !withPrefix || other.Prefix() == tenant.Prefix()
}

// backendAddress returns tenant's address for backend be with any trailing slash removed, or ""
// when that backend isn't configured.
func backendAddress(tenant *v1alpha1.Tenant, be v1alpha1.Backend) string {
	spec := tenant.Spec.Mimir
	if be == v1alpha1.BackendLoki {
		spec = tenant.Spec.Loki
	}
	if spec == nil {
		return ""
	}
	return strings.TrimRight(spec.Address, "/")
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
