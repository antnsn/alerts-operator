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
// AlertmanagerSynced on the tenant and Synced on the policy and contact points. The returned
// error is non-nil only when the backend was unavailable.
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
	r.markSynced(tenant.Name, gen, auth)
	metrics.Observe(tenant.Name, metrics.TargetAlertmanager, nil)
	setTenant(metav1.ConditionTrue, v1alpha1.ReasonSynced, "")
	setChildren(nil)
	return nil
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
