package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/antnsn/alerts-operator/api/v1alpha1"
	"github.com/antnsn/alerts-operator/internal/backend"
	"github.com/antnsn/alerts-operator/internal/compile"
	"github.com/antnsn/alerts-operator/internal/metrics"
)

// syncAlertmanager compiles and pushes the tenant's Alertmanager document. It sets
// AlertmanagerSynced on the tenant and Synced on the policy and contact points. The returned error
// is non-nil only when the backend was unavailable, or when another Tenant's claim on the document
// could not be checked at all (see the ownership guard below) -- both cases the caller must back
// off and retry rather than treat as a settled verdict.
func (r *TenantReconciler) syncAlertmanager(ctx context.Context, tenant *v1alpha1.Tenant, store backend.AlertmanagerStore, ch *children) error {
	gen := tenant.Generation
	setTenant := func(status metav1.ConditionStatus, reason, msg string) {
		setCondition(&tenant.Status.Conditions, v1alpha1.ConditionAlertmanagerSynced, status, reason, msg, gen)
	}
	setChildren := func(err error) {
		if ch.Policy != nil {
			r.setChildSynced(ctx, ch.Policy, err)
		}
		for i := range ch.ContactPoints {
			r.setChildSynced(ctx, &ch.ContactPoints[i], err)
		}
	}

	// A ContactPoint or the NotificationPolicy was just edited and hasn't been re-validated by its
	// own reconciler yet (see listChildren/staleGeneration). Compiling now would risk either
	// pushing a stale config or, if the edit dropped the last accepted policy, misreporting
	// NoNotificationPolicy for what is really a transient gap. Wait: don't compile, don't POST.
	if ch.PendingAM {
		setTenant(metav1.ConditionUnknown, v1alpha1.ReasonPending, "waiting for child validation")
		return nil
	}

	if ch.Policy == nil {
		setTenant(metav1.ConditionFalse, v1alpha1.ReasonNoNotificationPolicy, "no accepted NotificationPolicy references this Tenant")
		return nil
	}

	templates, err := r.loadTemplates(ctx, tenant)
	if err != nil {
		setTenant(metav1.ConditionFalse, v1alpha1.ReasonInvalid, err.Error())
		r.Recorder.Eventf(tenant, corev1.EventTypeWarning, "TemplatesInvalid", "%v", err)
		return nil
	}

	cfg, err := compile.Alertmanager(compile.AlertmanagerInput{
		Policy: ch.Policy, ContactPoints: ch.ContactPoints, Secrets: r.secretResolver(ctx), Templates: templates,
	})
	if err != nil {
		var ae *compile.AttributedError
		if errors.As(err, &ae) {
			// Explicit False/Invalid here, not setChildSynced(..., ae.Err): ae.Err is a compile/
			// validation failure, never a backend error, so it must not go through syncedFromErr
			// (built for backend/transport errors -- IsUnavailable treats anything that isn't a
			// *backend.StatusError as unavailable, which would misreport a bad webhook URL as
			// BackendUnavailable instead of Invalid).
			for i := range ch.ContactPoints {
				cp := &ch.ContactPoints[i]
				if ae.Kind == "ContactPoint" && cp.Namespace == ae.Namespace && cp.Name == ae.Name {
					r.setChildStatus(ctx, cp, metav1.ConditionFalse, v1alpha1.ReasonInvalid, ae.Err.Error())
					r.Recorder.Eventf(cp, corev1.EventTypeWarning, "CompileFailed", "%v", ae.Err)
				}
			}
			if ae.Kind == "NotificationPolicy" {
				r.setChildStatus(ctx, ch.Policy, metav1.ConditionFalse, v1alpha1.ReasonInvalid, ae.Err.Error())
				r.Recorder.Eventf(ch.Policy, corev1.EventTypeWarning, "CompileFailed", "%v", ae.Err)
			}
		}
		setTenant(metav1.ConditionFalse, v1alpha1.ReasonInvalid, err.Error())
		return nil
	}

	// Ownership guard, the steady-state counterpart to the finalizer's (tenant_finalizer.go) and
	// the analogue of syncRules' prune guard -- with the opposite verdict about writing, because the
	// two targets differ in kind.
	//
	// For rule namespaces the ruling is "ambiguous ownership never deletes, but still writes": two
	// Tenants' rule groups occupy different namespaces, so their writes are convergent and only the
	// prune is destructive. The Alertmanager document has no such separation. There is exactly one
	// document per (tenantId, address) -- POST /api/v1/alerts replaces it whole -- so a write here
	// *is* a delete of whatever the other claimant put there. Under the same rule ("never destroy on
	// an unverified claim") the write must be refused.
	//
	// But only against a peer that has something there to destroy. conflictingAlertmanagerWriter
	// narrows the claimant set to Tenants that have themselves written the document at this address,
	// the same narrowing the finalizer has always applied (writingClaimants, below). A Tenant with
	// spec.mimir set but no accepted NotificationPolicy short-circuits at the ch.Policy == nil branch
	// above, before any backend I/O, so it is structurally incapable of writing the document and
	// nothing of its can be destroyed by writing it. Counting such a peer -- which the first version
	// of this guard did -- froze the document for a "platform Tenant owns notifications, team Tenants
	// own rules under their own rulesNamespacePrefix" layout, which is exactly the arrangement
	// rulesNamespacePrefix exists to support, with no remedy that preserves it.
	//
	// So this is first-writer-wins. Does it converge? Yes, and the worst case is bounded:
	//
	//   - Steady state, peer appears after this Tenant wrote: the peer sees a writing claimant and
	//     refuses; this Tenant sees no writing claimant and carries on owning the document at
	//     Ready=True. Only the newcomer is flagged, which is both correct and the more useful
	//     signal.
	//   - Both intend to write, neither has yet: the guard is evaluated before the hash+recency
	//     skip, and a Tenant that has written keeps its hash forever (nothing clears it), so each
	//     Tenant writes at most once before it observes a peer's hash. If the cache is current, the
	//     second one already sees the first's hash and refuses -- one writer, no overwrite. If both
	//     reconcile inside the cache-lag window they each write once and each record a hash; from
	//     the next pass on *both* see a writing peer and both refuse permanently. The document is
	//     whichever landed last and it stops changing.
	//
	// Either way it settles after at most one write per Tenant. It does not oscillate: the previous
	// behaviour's 5-minute alternation was possible only because nothing ever recorded that someone
	// else had written, and refusing does not clear this Tenant's own hash.
	//
	// What the owners see on a real collision: AlertmanagerSynced=False/Conflict naming the peer, an
	// AlertmanagerOwnershipConflict warning event, Ready=False/Conflict, and children
	// Synced=False/Conflict. Rules are untouched -- syncRules keeps its own per-backend verdict, so
	// two Tenants with different prefixes go on syncing their rule groups normally.
	conflict, err := r.conflictingAlertmanagerWriter(ctx, tenant)
	if err != nil {
		// Ownership could not be established at all: same rule as a real collision (never write on
		// an unverified claim), but Unknown/Pending rather than False, and returned so the caller
		// backs off and retries instead of leaving a transient apiserver failure to sit until the
		// next resync. Unknown/Pending also makes Ready preserve its previous value rather than
		// asserting a verdict this pass never established.
		msg := fmt.Sprintf("cannot verify alertmanager ownership: %v", err)
		setTenant(metav1.ConditionUnknown, v1alpha1.ReasonPending, msg)
		return fmt.Errorf("%s", msg)
	}
	if conflict != "" {
		msg := fmt.Sprintf(
			"Tenant %q has already written the Alertmanager document for tenantId %q at %s; "+
				"/api/v1/alerts is scoped by tenantId and address only, never by rulesNamespacePrefix, "+
				"so writing it here would replace that Tenant's routing -- give one of them its own "+
				"tenantId or address, or delete the one that should not own notifications",
			conflict, tenant.Spec.TenantID, backendAddress(tenant, v1alpha1.BackendMimir))
		setTenant(metav1.ConditionFalse, v1alpha1.ReasonConflict, msg)
		if ch.Policy != nil {
			r.setChildStatus(ctx, ch.Policy, metav1.ConditionFalse, v1alpha1.ReasonConflict, msg)
		}
		for i := range ch.ContactPoints {
			r.setChildStatus(ctx, &ch.ContactPoints[i], metav1.ConditionFalse, v1alpha1.ReasonConflict, msg)
		}
		r.Recorder.Eventf(tenant, corev1.EventTypeWarning, "AlertmanagerOwnershipConflict", "%s", msg)
		return nil
	}

	hash := compile.HashAlertmanager(cfg)
	// Keyed on gen and the resolved basic-auth credentials, not just hash+recency: a Tenant whose
	// Policy/ContactPoints are unchanged (same hash) but whose spec.mimir.address or spec.tenantId
	// just changed (gen) -- or whose backend-auth Secret was just rotated, which the Secret watch
	// enqueues without touching gen at all -- would otherwise report Synced from a cache entry that
	// verified a *different* backend/tenant/credential, skipping any real check until the resync
	// interval next elapses.
	auth := r.authFingerprint(ctx, tenant)
	if last := r.lastSync(tenant.Name); hash == tenant.Status.AlertmanagerConfigHash && last.gen == gen && last.auth == auth && time.Since(last.at) < tenant.Resync() {
		setTenant(metav1.ConditionTrue, v1alpha1.ReasonSynced, "")
		setChildren(nil)
		return nil
	}

	current, err := store.Get(ctx)
	if err != nil {
		status, reason, msg := syncedFromErr(err)
		setTenant(status, reason, msg)
		setChildren(err)
		r.Recorder.Eventf(tenant, corev1.EventTypeWarning, "AlertmanagerGetFailed", "%v", err)
		metrics.Observe(tenant.Name, metrics.TargetAlertmanager, err)
		if backend.IsUnavailable(err) {
			return err
		}
		return nil
	}
	if current == nil || current.Config != cfg.Config || !maps.Equal(current.TemplateFiles, cfg.TemplateFiles) {
		if err := store.Set(ctx, cfg); err != nil {
			status, reason, msg := syncedFromErr(err)
			setTenant(status, reason, msg)
			setChildren(err)
			r.Recorder.Eventf(tenant, corev1.EventTypeWarning, "AlertmanagerSetFailed", "%v", err)
			metrics.Observe(tenant.Name, metrics.TargetAlertmanager, err)
			if backend.IsUnavailable(err) {
				return err
			}
			return nil
		}
	}
	tenant.Status.AlertmanagerConfigHash = hash
	// Recorded together with the hash, in the same in-memory mutation that Reconcile patches as one
	// unit: the finalizer (tenant_finalizer.go) trusts AlertmanagerConfigHash as evidence this Tenant
	// wrote the document only when this address still matches spec.mimir.address at delete time --
	// spec.mimir.address is mutable, so a hash confirmed against an address this Tenant has since
	// moved off of is not evidence about what (if anything) it wrote at the new one.
	tenant.Status.AlertmanagerConfigAddress = backendAddress(tenant, v1alpha1.BackendMimir)
	r.markSynced(tenant.Name, gen, auth)
	metrics.Observe(tenant.Name, metrics.TargetAlertmanager, nil)
	setTenant(metav1.ConditionTrue, v1alpha1.ReasonSynced, "")
	setChildren(nil)
	return nil
}

// conflictingAlertmanagerWriter returns the name of another Tenant CR that has itself written the
// Alertmanager document this Tenant is about to write, or "" when there is none.
//
// Two conditions, and both are needed:
//
//   - sharesBackendTarget(..., withPrefix=false): the peer addresses the same document. GET/POST/
//     DELETE /api/v1/alerts is scoped by X-Scope-OrgID and the backend URL alone, so this is
//     (tenantId, address) with no prefix component -- the key the finalizer has always used for
//     this target, now the same predicate.
//   - writingClaimants: the peer has actually written it, at this address. Sharing the key only
//     means a peer *can read* the document; a Tenant that never writes has nothing there for this
//     Tenant's write to destroy, so it is not a claimant to defer to. Without this, any Tenant
//     sharing the org -- including a rules-only one that can never POST -- would disable
//     Alertmanager for the Tenant that legitimately owns it.
//
// Terminating peers still count, as they do for rule namespaces: their document stays live until
// their own finalizer removes it. That is deliberate and its cost is tracked (alerts-operator-bqf).
func (r *TenantReconciler) conflictingAlertmanagerWriter(ctx context.Context, tenant *v1alpha1.Tenant) (string, error) {
	cs, err := claimantsVia(ctx, r.Client, tenant, v1alpha1.BackendMimir, false)
	if err != nil {
		return "", err
	}
	return lowestName(writingClaimants(cs, backendAddress(tenant, v1alpha1.BackendMimir))), nil
}

// writingClaimants filters claimants to those that have themselves written an Alertmanager document
// *at addr* -- the address this Tenant and every claimant in the slice share (claimantsVia with
// withPrefix=false already filtered on it, so passing it again here is just reusing that same value,
// not a new comparison basis). Sharing tenantId+address only guarantees a claimant can *read* the
// document; only a claimant whose own AlertmanagerConfigHash was confirmed *at this address* will
// ever assert, overwrite or delete it there (Codex P2-2, task-20 review round 1). The address check
// additionally excludes a claimant whose hash is non-empty but stale from an address it has since
// been repointed away from -- the same gap fixed for this Tenant's own finalizer gate (Codex P1, fix
// round 1).
//
// Both Alertmanager paths use it, and must: the finalizer, to decide whether it may delete the
// document (tenant_finalizer.go), and conflictingAlertmanagerWriter above, to decide whether it may
// write it. Applying it on one side only is how the write path came to refuse writes for peers the
// delete path was perfectly willing to ignore.
//
// Not used for rule-namespace claimants: a rule claimant's ownership is established by sharing the
// prefix itself, so any such claimant's own prune loop reclaims the residue once it stops seeing
// this Tenant as a conflict.
func writingClaimants(claimants []v1alpha1.Tenant, addr string) []v1alpha1.Tenant {
	var out []v1alpha1.Tenant
	for _, c := range claimants {
		if c.Status.AlertmanagerConfigHash != "" && c.Status.AlertmanagerConfigAddress == addr {
			out = append(out, c)
		}
	}
	return out
}

// amSyncState records when, at what Tenant generation, and against what backend-auth credentials
// syncAlertmanager last confirmed the backend actually holds the compiled config -- gen and auth are
// what let the hash+recency skip above notice a backend/tenant/credential identity change
// (spec.mimir.address, spec.tenantId, or the referenced auth Secret's content) even when the
// compiled document's content, and therefore its hash, hasn't changed.
type amSyncState struct {
	at   time.Time
	gen  int64
	auth string
}

func (r *TenantReconciler) lastSync(name string) amSyncState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastAMSync[name]
}

func (r *TenantReconciler) markSynced(name string, gen int64, auth string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lastAMSync == nil {
		r.lastAMSync = map[string]amSyncState{}
	}
	r.lastAMSync[name] = amSyncState{at: time.Now(), gen: gen, auth: auth}
}

// authFingerprint returns a value that changes whenever the Tenant's Mimir basic-auth credentials
// do, so the hash+recency skip above can't mistake a credential rotation -- which the Secret watch
// enqueues without touching Generation at all -- for "nothing changed." Best-effort: a resolution
// error yields a fingerprint that includes the error text, which simply won't match whatever was
// cached, forcing a real backend check rather than trusting a fingerprint we couldn't confirm.
func (r *TenantReconciler) authFingerprint(ctx context.Context, tenant *v1alpha1.Tenant) string {
	if tenant.Spec.Mimir == nil {
		return ""
	}
	o, err := r.backendOptions(ctx, tenant, tenant.Spec.Mimir)
	if err != nil {
		return "err:" + err.Error()
	}
	if o.BasicAuth == nil {
		return "none"
	}
	sum := sha256.Sum256([]byte(o.BasicAuth.Username + "\x00" + o.BasicAuth.Password))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// secretResolver reads Secret keys through the cached client.
func (r *TenantReconciler) secretResolver(ctx context.Context) compile.SecretResolver {
	return func(namespace, name, key string) (string, error) {
		var sec corev1.Secret
		if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &sec); err != nil {
			if apierrors.IsNotFound(err) {
				return "", fmt.Errorf("not found")
			}
			return "", err
		}
		v, ok := sec.Data[key]
		if !ok {
			return "", fmt.Errorf("key not found")
		}
		return string(v), nil
	}
}

// loadTemplates returns the ConfigMap data behind spec.alertmanager.templatesRef, or nil.
func (r *TenantReconciler) loadTemplates(ctx context.Context, tenant *v1alpha1.Tenant) (map[string]string, error) {
	if tenant.Spec.Alertmanager == nil || tenant.Spec.Alertmanager.TemplatesRef == nil {
		return nil, nil
	}
	ref := tenant.Spec.Alertmanager.TemplatesRef
	var cm corev1.ConfigMap
	if err := r.Get(ctx, types.NamespacedName{Namespace: ref.Namespace, Name: ref.Name}, &cm); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("templates ConfigMap %s not found", ref)
		}
		return nil, err
	}
	if len(cm.Data) == 0 {
		return nil, nil
	}
	return maps.Clone(cm.Data), nil
}
