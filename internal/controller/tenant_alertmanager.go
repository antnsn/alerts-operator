package controller

import (
	"context"
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
			for i := range ch.ContactPoints {
				cp := &ch.ContactPoints[i]
				if ae.Kind == "ContactPoint" && cp.Namespace == ae.Namespace && cp.Name == ae.Name {
					r.setChildSynced(ctx, cp, ae.Err)
					r.Recorder.Eventf(cp, corev1.EventTypeWarning, "CompileFailed", "%v", ae.Err)
				}
			}
			if ae.Kind == "NotificationPolicy" {
				r.setChildSynced(ctx, ch.Policy, ae.Err)
				r.Recorder.Eventf(ch.Policy, corev1.EventTypeWarning, "CompileFailed", "%v", ae.Err)
			}
		}
		setTenant(metav1.ConditionFalse, v1alpha1.ReasonInvalid, err.Error())
		return nil
	}

	hash := compile.HashAlertmanager(cfg)
	if hash == tenant.Status.AlertmanagerConfigHash && time.Since(r.lastSync(tenant.Name)) < tenant.Resync() {
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
			if backend.IsUnavailable(err) {
				return err
			}
			return nil
		}
	}
	tenant.Status.AlertmanagerConfigHash = hash
	r.markSynced(tenant.Name)
	setTenant(metav1.ConditionTrue, v1alpha1.ReasonSynced, "")
	setChildren(nil)
	return nil
}

func (r *TenantReconciler) lastSync(name string) time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastAMSync[name]
}

func (r *TenantReconciler) markSynced(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lastAMSync == nil {
		r.lastAMSync = map[string]time.Time{}
	}
	r.lastAMSync[name] = time.Now()
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
