/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/antnsn/alerts-operator/api/v1alpha1"
	"github.com/antnsn/alerts-operator/internal/backend"
	"github.com/antnsn/alerts-operator/internal/backend/loki"
	"github.com/antnsn/alerts-operator/internal/backend/mimir"
	"github.com/antnsn/alerts-operator/internal/compile"
	"github.com/antnsn/alerts-operator/internal/index"
)

const tenantFinalizer = "observability.antnsn.dev/tenant"

// MimirClient is what the Tenant reconciler needs from Mimir.
type MimirClient interface {
	backend.RuleStore
	backend.AlertmanagerStore
}

// TenantReconciler is the single writer to Mimir and Loki.
type TenantReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	NewMimir func(backend.Options) MimirClient
	NewLoki  func(backend.Options) backend.RuleStore

	mu         sync.Mutex //nolint:unused // guards lastAMSync; taken into use by Task 19's syncAlertmanager
	lastAMSync map[string]time.Time
}

// children are the Accepted CRs referencing one Tenant, plus what's known about children that are
// accepted for a stale generation (see staleGeneration): their backend state must survive this pass
// untouched rather than being treated as no-longer-desired and pruned.
type children struct {
	ContactPoints []v1alpha1.ContactPoint
	Policy        *v1alpha1.NotificationPolicy
	MimirGroups   []v1alpha1.AlertRuleGroup
	LokiGroups    []v1alpha1.AlertRuleGroup

	KeepNamespaces map[string]bool // backend namespaces of stale-generation AlertRuleGroups
	PendingAM      bool            // a ContactPoint or NotificationPolicy referencing this tenant is stale-generation
}

// +kubebuilder:rbac:groups=observability.antnsn.dev,resources=tenants,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=observability.antnsn.dev,resources=tenants/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=observability.antnsn.dev,resources=tenants/finalizers,verbs=update
// +kubebuilder:rbac:groups=observability.antnsn.dev,resources=contactpoints;notificationpolicies;alertrulegroups,verbs=get;list;watch
// +kubebuilder:rbac:groups=observability.antnsn.dev,resources=contactpoints/status;notificationpolicies/status;alertrulegroups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=secrets;configmaps,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile drives one Tenant to its desired backend state: it lists Accepted children, resolves
// backend clients, syncs Mimir/Loki rules and Alertmanager config, and aggregates the result into
// the Ready condition.
func (r *TenantReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var tenant v1alpha1.Tenant
	if err := r.Get(ctx, req.NamespacedName, &tenant); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !tenant.DeletionTimestamp.IsZero() {
		if !controllerutil.ContainsFinalizer(&tenant, tenantFinalizer) {
			return ctrl.Result{}, nil
		}
		if err := r.finalize(ctx, &tenant); err != nil {
			deletingChanged := false
			_ = patchStatus(ctx, r.Client, &tenant, func() {
				deletingChanged = setCondition(&tenant.Status.Conditions, v1alpha1.ConditionReady, metav1.ConditionFalse, v1alpha1.ReasonDeleting, err.Error(), tenant.Generation)
			}, &deletingChanged)
			r.Recorder.Eventf(&tenant, corev1.EventTypeWarning, "FinalizeFailed", "%v", err)
			return ctrl.Result{}, err
		}
		controllerutil.RemoveFinalizer(&tenant, tenantFinalizer)
		return ctrl.Result{}, r.Update(ctx, &tenant)
	}

	if controllerutil.AddFinalizer(&tenant, tenantFinalizer) {
		return ctrl.Result{}, r.Update(ctx, &tenant) // Update triggers a new reconcile.
	}

	// Snapshot before the sync functions mutate tenant.Status in memory; the final
	// status patch is the diff against this snapshot.
	base := tenant.DeepCopy()

	ch, err := r.listChildren(ctx, &tenant)
	if err != nil {
		return ctrl.Result{}, err
	}

	var firstErr error
	keep := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	gen := tenant.Generation

	if tenant.Spec.Mimir != nil {
		mc, err := r.mimirClient(ctx, &tenant)
		if err != nil {
			setCondition(&tenant.Status.Conditions, v1alpha1.ConditionMimirRulesSynced, metav1.ConditionFalse, v1alpha1.ReasonInvalid, err.Error(), gen)
			setCondition(&tenant.Status.Conditions, v1alpha1.ConditionAlertmanagerSynced, metav1.ConditionFalse, v1alpha1.ReasonInvalid, err.Error(), gen)
		} else {
			n, err := r.syncRules(ctx, &tenant, mc, v1alpha1.BackendMimir, ch.MimirGroups, ch.KeepNamespaces)
			keep(err)
			tenant.Status.RuleGroups.Mimir = n
			keep(r.syncAlertmanager(ctx, &tenant, mc, ch))
		}
	} else {
		setCondition(&tenant.Status.Conditions, v1alpha1.ConditionMimirRulesSynced, metav1.ConditionUnknown, v1alpha1.ReasonNotConfigured, "spec.mimir not set", gen)
		setCondition(&tenant.Status.Conditions, v1alpha1.ConditionAlertmanagerSynced, metav1.ConditionUnknown, v1alpha1.ReasonNotConfigured, "spec.mimir not set", gen)
		tenant.Status.RuleGroups.Mimir = 0
	}

	if tenant.Spec.Loki != nil {
		lc, err := r.lokiClient(ctx, &tenant)
		if err != nil {
			setCondition(&tenant.Status.Conditions, v1alpha1.ConditionLokiRulesSynced, metav1.ConditionFalse, v1alpha1.ReasonInvalid, err.Error(), gen)
		} else {
			n, err := r.syncRules(ctx, &tenant, lc, v1alpha1.BackendLoki, ch.LokiGroups, ch.KeepNamespaces)
			keep(err)
			tenant.Status.RuleGroups.Loki = n
		}
	} else {
		setCondition(&tenant.Status.Conditions, v1alpha1.ConditionLokiRulesSynced, metav1.ConditionUnknown, v1alpha1.ReasonNotConfigured, "spec.loki not set", gen)
		tenant.Status.RuleGroups.Loki = 0
	}

	// Ready = every configured target condition is True. A False condition always wins. An Unknown
	// condition with ReasonNotConfigured is a backend the tenant doesn't use and never affects
	// Ready. An Unknown condition with ReasonPending (syncAlertmanager, when ch.PendingAM: a
	// ContactPoint/NotificationPolicy edit that hasn't been re-validated yet) means this pass has no
	// new information, so Ready keeps whatever value it already had rather than jumping to True.
	readyStatus, readyReason, readyMsg := metav1.ConditionTrue, v1alpha1.ReasonSynced, ""
	pending := false
	for _, typ := range []string{v1alpha1.ConditionAlertmanagerSynced, v1alpha1.ConditionMimirRulesSynced, v1alpha1.ConditionLokiRulesSynced} {
		c := meta.FindStatusCondition(tenant.Status.Conditions, typ)
		if c == nil || c.Status == metav1.ConditionUnknown {
			if c != nil && c.Reason == v1alpha1.ReasonPending {
				pending = true
			}
			continue
		}
		if c.Status == metav1.ConditionFalse {
			readyStatus, readyReason, readyMsg = metav1.ConditionFalse, c.Reason, typ+": "+c.Message
			pending = false
			break
		}
	}
	if pending {
		if prev := meta.FindStatusCondition(base.Status.Conditions, v1alpha1.ConditionReady); prev != nil {
			readyStatus, readyReason, readyMsg = prev.Status, prev.Reason, prev.Message
		}
	}
	setCondition(&tenant.Status.Conditions, v1alpha1.ConditionReady, readyStatus, readyReason, readyMsg, gen)

	tenant.Status.ObservedGeneration = gen
	// Only patch when status really changed (see patchStatus for why an unconditional
	// optimistic-lock patch would loop).
	if !equality.Semantic.DeepEqual(base.Status, tenant.Status) {
		if err := r.Status().Patch(ctx, &tenant, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
			return ctrl.Result{}, err
		}
	}
	if firstErr != nil {
		return ctrl.Result{}, firstErr
	}
	// A pending child or a kept-but-not-yet-revalidated namespace means this Tenant is sitting on
	// stale exclusions; the child's own status patch also re-enqueues via the watch, but this is a
	// safety net so we don't wait a full resync interval to pick the fix up.
	requeue := tenant.Resync()
	if ch.PendingAM || len(ch.KeepNamespaces) > 0 {
		requeue = 5 * time.Second
	}
	return ctrl.Result{RequeueAfter: requeue}, nil
}

// listChildren returns the Accepted children of a tenant, split by kind and backend, plus what's
// pending: children (any of the three kinds) whose Accepted condition is stale — missing, or
// ObservedGeneration < Generation — because a spec edit bumped Generation and this child's own
// reconciler hasn't run yet. Those are neither desired (they're not in the returned slices) nor
// prunable (an AlertRuleGroup's namespace lands in KeepNamespaces; a stale ContactPoint or
// NotificationPolicy sets PendingAM). An object with Accepted=False at its current generation is a
// validated rejection, not pending: it's simply excluded, same as before.
func (r *TenantReconciler) listChildren(ctx context.Context, tenant *v1alpha1.Tenant) (*children, error) {
	sel := client.MatchingFields{index.IndexTenantRef: tenant.Name}
	ch := &children{KeepNamespaces: map[string]bool{}}

	var cps v1alpha1.ContactPointList
	if err := r.List(ctx, &cps, sel); err != nil {
		return nil, err
	}
	for _, cp := range cps.Items {
		switch {
		case acceptedCurrent(&cp):
			ch.ContactPoints = append(ch.ContactPoints, cp)
		case staleGeneration(&cp):
			ch.PendingAM = true
		}
	}

	var pols v1alpha1.NotificationPolicyList
	if err := r.List(ctx, &pols, sel); err != nil {
		return nil, err
	}
	var accepted []v1alpha1.NotificationPolicy
	for _, p := range pols.Items {
		switch {
		case acceptedCurrent(&p):
			accepted = append(accepted, p)
		case staleGeneration(&p):
			ch.PendingAM = true
		}
	}
	ch.Policy = policyWinner(accepted)

	var args v1alpha1.AlertRuleGroupList
	if err := r.List(ctx, &args, sel); err != nil {
		return nil, err
	}
	for _, a := range args.Items {
		switch {
		case acceptedCurrent(&a):
			switch a.Spec.Backend {
			case v1alpha1.BackendMimir:
				ch.MimirGroups = append(ch.MimirGroups, a)
			case v1alpha1.BackendLoki:
				ch.LokiGroups = append(ch.LokiGroups, a)
			}
		case staleGeneration(&a):
			ch.KeepNamespaces[compile.BackendNamespace(tenant.Prefix(), a.Namespace, a.Name)] = true
		}
	}
	return ch, nil
}

// acceptedCurrent is true only when Accepted=True was set for the object's current generation.
// A freshly edited spec still carries the previous generation's Accepted=True until its own
// reconciler runs; without this check the Tenant could push an unvalidated spec to the backend.
func acceptedCurrent(obj v1alpha1.Conditioned) bool {
	c := meta.FindStatusCondition(obj.GetConditions(), v1alpha1.ConditionAccepted)
	return c != nil && c.Status == metav1.ConditionTrue && c.ObservedGeneration == obj.GetGeneration()
}

// staleGeneration is true when obj's Accepted condition has not yet been evaluated for its current
// generation: missing, or ObservedGeneration < Generation. That's the window right after a spec
// edit, before the child's own reconciler runs — not the same as Accepted=False at the current
// generation, which is a validated rejection and must stay excluded/prunable.
func staleGeneration(obj v1alpha1.Conditioned) bool {
	c := meta.FindStatusCondition(obj.GetConditions(), v1alpha1.ConditionAccepted)
	return c == nil || c.ObservedGeneration < obj.GetGeneration()
}

// backendOptions builds client options, resolving basic auth from a Secret with keys username/password.
func (r *TenantReconciler) backendOptions(ctx context.Context, tenant *v1alpha1.Tenant, spec *v1alpha1.BackendSpec) (backend.Options, error) {
	o := backend.Options{Address: spec.Address, TenantID: tenant.Spec.TenantID}
	if spec.Auth != nil && spec.Auth.BasicAuthSecretRef != nil {
		ref := spec.Auth.BasicAuthSecretRef
		var sec corev1.Secret
		if err := r.Get(ctx, types.NamespacedName{Namespace: ref.Namespace, Name: ref.Name}, &sec); err != nil {
			if errors.IsNotFound(err) {
				return o, fmt.Errorf("basic auth secret %s not found", ref)
			}
			return o, err
		}
		u, uok := sec.Data["username"]
		p, pok := sec.Data["password"]
		if !uok || !pok {
			return o, fmt.Errorf("basic auth secret %s must have keys username and password", ref)
		}
		o.BasicAuth = &backend.BasicAuth{Username: string(u), Password: string(p)}
	}
	return o, nil
}

func (r *TenantReconciler) mimirClient(ctx context.Context, tenant *v1alpha1.Tenant) (MimirClient, error) {
	o, err := r.backendOptions(ctx, tenant, tenant.Spec.Mimir)
	if err != nil {
		return nil, err
	}
	if r.NewMimir != nil {
		return r.NewMimir(o), nil
	}
	return mimir.New(o), nil
}

func (r *TenantReconciler) lokiClient(ctx context.Context, tenant *v1alpha1.Tenant) (backend.RuleStore, error) {
	o, err := r.backendOptions(ctx, tenant, tenant.Spec.Loki)
	if err != nil {
		return nil, err
	}
	if r.NewLoki != nil {
		return r.NewLoki(o), nil
	}
	return loki.New(o), nil
}

// --- stubs replaced in Tasks 19 and 20 ---

// syncRules is a stub: Task 19 fills in the real Mimir/Loki rule sync. It sets the backend's
// RulesSynced condition to True and reports the desired group count.
func (r *TenantReconciler) syncRules(_ context.Context, tenant *v1alpha1.Tenant, _ backend.RuleStore, be v1alpha1.Backend, groups []v1alpha1.AlertRuleGroup, _ map[string]bool) (int32, error) {
	typ := v1alpha1.ConditionMimirRulesSynced
	if be == v1alpha1.BackendLoki {
		typ = v1alpha1.ConditionLokiRulesSynced
	}
	setCondition(&tenant.Status.Conditions, typ, metav1.ConditionTrue, v1alpha1.ReasonSynced, "", tenant.Generation)
	return int32(len(groups)), nil
}

// syncAlertmanager is a stub: Task 19 fills in the real Alertmanager config sync. It sets
// AlertmanagerSynced to True.
func (r *TenantReconciler) syncAlertmanager(_ context.Context, tenant *v1alpha1.Tenant, _ backend.AlertmanagerStore, _ *children) error {
	setCondition(&tenant.Status.Conditions, v1alpha1.ConditionAlertmanagerSynced, metav1.ConditionTrue, v1alpha1.ReasonSynced, "", tenant.Generation)
	return nil
}

// finalize is a stub: Task 20 fills in the real backend cleanup (deleting the tenant's rule
// groups and Alertmanager config).
func (r *TenantReconciler) finalize(_ context.Context, _ *v1alpha1.Tenant) error { return nil }

// --- watches ---

// secretToTenants maps a Secret to tenants using it for basic auth and to tenants of ContactPoints referencing it.
func (r *TenantReconciler) secretToTenants(ctx context.Context, o client.Object) []reconcile.Request {
	seen := map[string]bool{}
	var out []reconcile.Request
	add := func(name string) {
		if name != "" && !seen[name] {
			seen[name] = true
			out = append(out, tenantRequest(name))
		}
	}
	var tenants v1alpha1.TenantList
	if err := r.List(ctx, &tenants); err == nil {
		for _, t := range tenants.Items {
			for _, b := range []*v1alpha1.BackendSpec{t.Spec.Mimir, t.Spec.Loki} {
				if b != nil && b.Auth != nil && b.Auth.BasicAuthSecretRef != nil &&
					b.Auth.BasicAuthSecretRef.Namespace == o.GetNamespace() && b.Auth.BasicAuthSecretRef.Name == o.GetName() {
					add(t.Name)
				}
			}
		}
	}
	var cps v1alpha1.ContactPointList
	if err := r.List(ctx, &cps, client.InNamespace(o.GetNamespace()), client.MatchingFields{index.IndexSecretRefs: o.GetName()}); err == nil {
		for _, cp := range cps.Items {
			add(cp.Spec.TenantRef)
		}
	}
	return out
}

// configMapToTenants maps a ConfigMap to tenants whose alertmanager.templatesRef points at it.
func (r *TenantReconciler) configMapToTenants(ctx context.Context, o client.Object) []reconcile.Request {
	var tenants v1alpha1.TenantList
	if err := r.List(ctx, &tenants); err != nil {
		return nil
	}
	var out []reconcile.Request
	for _, t := range tenants.Items {
		ref := t.Spec.Alertmanager
		if ref != nil && ref.TemplatesRef != nil && ref.TemplatesRef.Namespace == o.GetNamespace() && ref.TemplatesRef.Name == o.GetName() {
			out = append(out, tenantRequest(t.Name))
		}
	}
	return out
}

// SetupWithManager wires the Tenant reconciler's watches: children (ContactPoint,
// NotificationPolicy, AlertRuleGroup) via mapToTenant, plus Secret and ConfigMap watches that
// resolve indirectly to affected tenants.
func (r *TenantReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Recorder == nil {
		// GetEventRecorderFor is deprecated in favor of GetEventRecorder, but that returns a
		// different (recorder.EventRecorder) type; Recorder's field type is record.EventRecorder
		// (k8s.io/client-go/tools/record), so this is the correct constructor for it.
		r.Recorder = mgr.GetEventRecorderFor("tenant-controller") //nolint:staticcheck // record.EventRecorder is the field type
	}
	r.lastAMSync = map[string]time.Time{}
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.Tenant{}).
		Watches(&v1alpha1.ContactPoint{}, handler.EnqueueRequestsFromMapFunc(mapToTenant)).
		Watches(&v1alpha1.NotificationPolicy{}, handler.EnqueueRequestsFromMapFunc(mapToTenant)).
		Watches(&v1alpha1.AlertRuleGroup{}, handler.EnqueueRequestsFromMapFunc(mapToTenant)).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.secretToTenants)).
		Watches(&corev1.ConfigMap{}, handler.EnqueueRequestsFromMapFunc(r.configMapToTenants)).
		Named("tenant").
		Complete(r)
}
