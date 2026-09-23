package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"

	"github.com/antnsn/alerts-operator/api/v1alpha1"
	"github.com/antnsn/alerts-operator/internal/backend"
	"github.com/antnsn/alerts-operator/internal/compile"
)

// finalize removes everything this tenant owns in its backends: the Alertmanager config and every
// rule namespace under the tenant's effective prefix (tenant.Prefix(), not the raw
// spec.rulesNamespacePrefix field). Any failure on an un-skipped backend keeps the finalizer and is
// retried on the next reconcile (see the finalizer branch in TenantReconciler.Reconcile, which
// returns the error and never removes the finalizer when finalize fails).
//
// Ownership collisions: when another Tenant CR currently shares this Tenant's ownership key for a
// backend target, deleting is not distinguishable from deleting that other Tenant's live state, so
// it is skipped for that target rather than attempted or treated as a failure requiring retry. Two
// separate keys are in play, deliberately not the same one:
//   - Rule namespaces use (tenantId, backend address, effective prefix) -- the exact key
//     conflictingTenant checks for the steady-state prune in tenant_rules.go, since
//     <prefix><sep><k8s-namespace><sep><name> is genuinely scoped by prefix. sep is "/" on Mimir
//     and "_" on Loki (compile.BackendNamespace), so the delete sweep below is run per backend
//     with that backend named explicitly -- never once with a separator assumed from context.
//   - The Alertmanager document uses (tenantId, backend address) only, with no prefix at all:
//     POST/GET/DELETE /api/v1/alerts is scoped solely by X-Scope-OrgID and the backend, so two
//     Tenants with the same tenantId and address share one live document even when they use
//     different rulesNamespacePrefix values -- using conflictingTenant's prefix-inclusive key here
//     would (and, before this fix, did) let one such Tenant's finalizer wipe the other's config.
//
// Skipping (not erroring) on a collision is deliberate: erroring would block finalizer removal
// forever, and the operator's actual fix for a namespace collision -- typically deleting one of the
// two colliding Tenants -- could then never finish, since finishing it is exactly the action that
// would be permanently blocked. But a plain "defer to whichever other Tenant shares this key" is not
// enough by itself: if that other Tenant is *also* being deleted right now, it will never survive to
// do the deferred work either, and the shared backend state leaks forever with no CR left to even
// retry it. finalizeOwner (below) is the deterministic tie-break that makes exactly one member of a
// set of Tenants deleted together actually perform the cleanup, rather than every member deferring to
// every other member.
func (r *TenantReconciler) finalize(ctx context.Context, tenant *v1alpha1.Tenant) error {
	if tenant.Spec.Mimir != nil {
		mc, err := r.mimirClient(ctx, tenant)
		if err != nil {
			return fmt.Errorf("mimir client: %w", err)
		}

		// DELETE /api/v1/alerts wipes this tenant's whole Alertmanager config in Mimir. It is
		// permitted here, in the finalizer, and nowhere else in this codebase.
		//
		// Only attempted when this Tenant actually wrote the document that is currently live at its
		// *current* address. status.alertmanagerConfigHash is set only once syncAlertmanager has
		// confirmed the backend holds its compiled config (tenant_alertmanager.go): a Tenant with
		// spec.mimir set but no accepted NotificationPolicy never calls store.Set at all (ch.Policy ==
		// nil short-circuits before any backend I/O), so it has nothing of its own to delete --
		// unconditionally deleting here could destroy a hand-written or externally-managed document
		// this operator never claimed by writing (Codex P2-1, task-20 review round 1). The address
		// comparison closes a second gap found reviewing that same fix (Codex P1, fix round 1):
		// spec.mimir.address is mutable, so a non-empty hash confirmed against an address this Tenant
		// has since been repointed away from is not evidence about the document at its current one --
		// without this check, a stale hash from an old address would still pass the "did I write
		// something" gate and let this Tenant delete whatever happens to live at its new address.
		addr := backendAddress(tenant, v1alpha1.BackendMimir)
		if tenant.Status.AlertmanagerConfigHash != "" && tenant.Status.AlertmanagerConfigAddress == addr {
			amClaimants, err := r.claimants(ctx, tenant, v1alpha1.BackendMimir, false)
			if err != nil {
				return fmt.Errorf("mimir: cannot verify alertmanager ownership: %w", err)
			}
			// Only a claimant that itself wrote a document *at this same address* is a valid Tenant to
			// defer to: one that never writes (no accepted NotificationPolicy), or whose own hash is
			// stale from a different address, will never assert, overwrite, or delete this Tenant's
			// document either, so deferring to it strands the config forever with no CR left
			// describing it (Codex P2-2, task-20 review round 1).
			if owner := finalizeOwner(tenant, writingClaimants(amClaimants, addr)); owner != "" {
				r.Recorder.Eventf(tenant, corev1.EventTypeWarning, "FinalizeSkipped",
					"mimir: Tenant %q shares tenantId %q and address %s; alertmanager config left untouched for it to reconcile",
					owner, tenant.Spec.TenantID, addr)
			} else if err := mc.Delete(ctx); err != nil {
				return fmt.Errorf("delete alertmanager config: %w", err)
			}
		}

		ruleClaimants, err := r.claimants(ctx, tenant, v1alpha1.BackendMimir, true)
		if err != nil {
			return fmt.Errorf("mimir: cannot verify rule namespace ownership: %w", err)
		}
		if owner := finalizeOwner(tenant, ruleClaimants); owner != "" {
			r.Recorder.Eventf(tenant, corev1.EventTypeWarning, "FinalizeSkipped",
				"mimir: Tenant %q shares tenantId %q, address %s, rulesNamespacePrefix %q; rule namespaces left untouched for it to reconcile",
				owner, tenant.Spec.TenantID, backendAddress(tenant, v1alpha1.BackendMimir), tenant.Prefix())
		} else if err := deleteOwnedNamespaces(ctx, mc, v1alpha1.BackendMimir, tenant.Prefix()); err != nil {
			return fmt.Errorf("mimir rules: %w", err)
		}
	}

	if tenant.Spec.Loki != nil {
		lc, err := r.lokiClient(ctx, tenant)
		if err != nil {
			return fmt.Errorf("loki client: %w", err)
		}
		claimants, err := r.claimants(ctx, tenant, v1alpha1.BackendLoki, true)
		if err != nil {
			return fmt.Errorf("loki: cannot verify rule namespace ownership: %w", err)
		}
		if owner := finalizeOwner(tenant, claimants); owner != "" {
			r.Recorder.Eventf(tenant, corev1.EventTypeWarning, "FinalizeSkipped",
				"loki: Tenant %q shares tenantId %q, address %s, rulesNamespacePrefix %q; rule namespaces left untouched for it to reconcile",
				owner, tenant.Spec.TenantID, backendAddress(tenant, v1alpha1.BackendLoki), tenant.Prefix())
		} else if err := deleteOwnedNamespaces(ctx, lc, v1alpha1.BackendLoki, tenant.Prefix()); err != nil {
			return fmt.Errorf("loki rules: %w", err)
		}
	}

	return nil
}

// claimants returns the other Tenants that currently share ownership, for backend be, of what this
// Tenant is about to delete. Ownership is sharesBackendTarget (tenant_rules.go), the one definition
// this codebase has of "the same thing in the backend", shared with the steady-state guards so the
// delete path and the write path can never disagree about it: withPrefix must be true for a
// rule-namespace delete and false for the Alertmanager delete, whose backend key has no prefix
// component at all.
//
// Reads via apiReader (the uncached API server reader), not the cache-backed embedded Client: this
// result feeds straight into finalizeOwner's decision to remove this Tenant's finalizer, which never
// gets retried once made. A cache read that lagged behind a colliding Tenant's own just-completed
// deletion could make two finalizers each defer to the other -- one sees the other as already
// terminating and defers via the tie-break, not realising the peer had earlier deferred back to a
// third, already-gone Tenant it had cached as still "live" -- leaving neither to actually clean up
// (a Codex finding on this task: overlapping, not just simultaneous, deletions can still race).
func (r *TenantReconciler) claimants(ctx context.Context, tenant *v1alpha1.Tenant, be v1alpha1.Backend, withPrefix bool) ([]v1alpha1.Tenant, error) {
	return claimantsVia(ctx, r.apiReader(), tenant, be, withPrefix)
}

// finalizeOwner decides, among tenant and its claimants for one backend target, which single Tenant
// is responsible for deleting that target right now.
//
// It returns "" when tenant itself must proceed: either there are no claimants (no collision at
// all), or every claimant is itself being deleted and tenant has the lowest name among
// {tenant} ∪ {terminating claimants} -- the deterministic tie-break that guarantees exactly one
// member of a set of Tenants being deleted together actually performs the cleanup (the Codex P1
// finding this fixes: without it, two colliding Tenants deleted concurrently would each see the
// other as "the survivor" that will clean up later, both skip, and the shared backend state leaks
// forever with no CR left to retry it).
//
// It returns another Tenant's name when tenant may safely skip: that Tenant is not being deleted
// (DeletionTimestamp.IsZero()), so it continues to exist after tenant's CR is gone and its own next
// reconcile -- conflictingTenant's prune-skip clearing, or its own Alertmanager sync -- picks up
// whatever tenant leaves behind.
func finalizeOwner(tenant *v1alpha1.Tenant, claimants []v1alpha1.Tenant) string {
	lowest := tenant.Name
	for i := range claimants {
		other := &claimants[i]
		if other.DeletionTimestamp.IsZero() {
			return other.Name // a live claimant exists: defer to it unconditionally
		}
		if other.Name < lowest {
			lowest = other.Name
		}
	}
	if lowest == tenant.Name {
		return "" // no claimants, or tenant is the deterministically appointed cleaner
	}
	return lowest
}

// deleteOwnedNamespaces deletes every rule namespace owned by prefix on backend be. Ownership is
// compile.OwnsNamespace -- a segment-boundary match on prefix + that backend's separator, never a
// bare strings.HasPrefix on the unslashed prefix -- which keeps "alerts-operator" from claiming a
// differently-owned "alerts-operator-other/..." (Mimir) or "alerts-operator-other_..." (Loki).
//
// be is passed explicitly rather than inferred from store's concrete type: the separator must be
// stated by the caller that knows which backend it is finalizing.
func deleteOwnedNamespaces(ctx context.Context, store backend.RuleStore, be v1alpha1.Backend, prefix string) error {
	actual, err := store.List(ctx)
	if err != nil {
		return err
	}
	for ns := range actual {
		if compile.OwnsNamespace(be, prefix, ns) {
			if err := store.DeleteNamespace(ctx, ns); err != nil {
				return err
			}
		}
	}
	return nil
}
