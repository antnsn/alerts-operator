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
	"sort"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/antnsn/alerts-operator/api/v1alpha1"
	"github.com/antnsn/alerts-operator/internal/compile"
	"github.com/antnsn/alerts-operator/internal/index"
)

const maxRouteDepth = 10

// NotificationPolicyReconciler validates the route tree (shape, matchers, durations, inhibit
// rules), receiver references and per-tenant uniqueness.
type NotificationPolicyReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=observability.antnsn.dev,resources=notificationpolicies,verbs=get;list;watch
// +kubebuilder:rbac:groups=observability.antnsn.dev,resources=notificationpolicies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=observability.antnsn.dev,resources=contactpoints,verbs=get;list;watch
// +kubebuilder:rbac:groups=observability.antnsn.dev,resources=tenants,verbs=get;list;watch

// Reconcile validates the NotificationPolicy's route tree, receiver references and per-tenant
// uniqueness, then sets the Accepted condition.
func (r *NotificationPolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var pol v1alpha1.NotificationPolicy
	if err := r.Get(ctx, req.NamespacedName, &pol); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	status, reason, msg, err := r.validate(ctx, &pol)
	if err != nil {
		return ctrl.Result{}, err
	}
	changed := false
	err = patchStatus(ctx, r.Client, &pol, func() {
		changed = setCondition(&pol.Status.Conditions, v1alpha1.ConditionAccepted, status, reason, msg, pol.Generation)
		changed = changed || pol.Status.ObservedGeneration != pol.Generation
		pol.Status.ObservedGeneration = pol.Generation
	}, &changed)
	return ctrl.Result{}, err
}

// validate checks the referenced Tenant, the route tree depth and decode, every receiver
// reference, and per-tenant uniqueness (oldest policy wins).
func (r *NotificationPolicyReconciler) validate(ctx context.Context, pol *v1alpha1.NotificationPolicy) (metav1.ConditionStatus, string, string, error) {
	var tenant v1alpha1.Tenant
	if err := r.Get(ctx, types.NamespacedName{Name: pol.Spec.TenantRef}, &tenant); errors.IsNotFound(err) {
		return metav1.ConditionFalse, v1alpha1.ReasonTenantNotFound, fmt.Sprintf("Tenant %q not found", pol.Spec.TenantRef), nil
	} else if err != nil {
		return "", "", "", err
	}
	depth, err := routeDepth(&pol.Spec.Route)
	if err != nil {
		return metav1.ConditionFalse, v1alpha1.ReasonInvalid, err.Error(), nil
	}
	if depth > maxRouteDepth {
		return metav1.ConditionFalse, v1alpha1.ReasonInvalid, fmt.Sprintf("route tree depth %d exceeds %d", depth, maxRouteDepth), nil
	}
	receivers, err := pol.Spec.Route.Receivers()
	if err != nil {
		return metav1.ConditionFalse, v1alpha1.ReasonInvalid, err.Error(), nil
	}
	for _, name := range receivers {
		var cp v1alpha1.ContactPoint
		err := r.Get(ctx, types.NamespacedName{Namespace: pol.Namespace, Name: name}, &cp)
		if errors.IsNotFound(err) || (err == nil && cp.Spec.TenantRef != pol.Spec.TenantRef) {
			return metav1.ConditionFalse, v1alpha1.ReasonContactPointNotFound,
				fmt.Sprintf("receiver %q: no ContactPoint %s/%s for tenant %q", name, pol.Namespace, name, pol.Spec.TenantRef), nil
		}
		if err != nil {
			return "", "", "", err
		}
	}
	// Matchers, durations, group_by and inhibit rules are checked here with Alertmanager's own
	// loader, so a policy the Tenant compile would reject is refused on this object at Accepted time
	// -- the same gate ContactPoint and AlertRuleGroup have (alerts-operator-8ic). Receiver names
	// were resolved above; the validator only substitutes placeholders for them.
	if verr := compile.ValidateNotificationPolicy(pol); verr != nil {
		return metav1.ConditionFalse, v1alpha1.ReasonInvalid, verr.Error(), nil
	}
	var all v1alpha1.NotificationPolicyList
	if err := r.List(ctx, &all, client.MatchingFields{index.IndexTenantRef: pol.Spec.TenantRef}); err != nil {
		return "", "", "", err
	}
	if w := policyWinner(all.Items); w != nil && (w.Namespace != pol.Namespace || w.Name != pol.Name) {
		return metav1.ConditionFalse, v1alpha1.ReasonConflict,
			fmt.Sprintf("Tenant %q already has NotificationPolicy %s/%s (oldest wins)", pol.Spec.TenantRef, w.Namespace, w.Name), nil
	}
	return metav1.ConditionTrue, v1alpha1.ReasonAccepted, "", nil
}

// routeDepth returns the depth of the route tree; a single root is 1. It decodes children via
// ChildRoutes() and returns the first decode error, which the caller treats as Invalid. Descent
// stops once the tree is already deeper than maxRouteDepth, so a maliciously deep (or wide-and-deep)
// route tree can't force unbounded recursive JSON decoding before validate's depth check runs --
// each level's ChildRoutes() call parses the remaining subtree, so unmarshaling every level of an
// attacker-sized tree would otherwise cost roughly O(depth^2).
func routeDepth(r *v1alpha1.Route) (int, error) {
	return routeDepthCapped(r, maxRouteDepth+1)
}

// routeDepthCapped mirrors routeDepth but refuses to decode more than budget further levels: once
// budget is exhausted it returns 1 without calling ChildRoutes(), so the caller sees a depth
// beyond maxRouteDepth (guaranteeing validate rejects it) without paying for the rest of the tree.
func routeDepthCapped(r *v1alpha1.Route, budget int) (int, error) {
	if budget <= 0 {
		return 1, nil
	}
	children, err := r.ChildRoutes()
	if err != nil {
		return 0, err
	}
	maxDepth := 0
	for i := range children {
		d, err := routeDepthCapped(&children[i], budget-1)
		if err != nil {
			return 0, err
		}
		if d > maxDepth {
			maxDepth = d
		}
	}
	return maxDepth + 1, nil
}

// policyWinner picks the one policy that counts for a tenant: oldest first, then "<ns>/<name>".
func policyWinner(items []v1alpha1.NotificationPolicy) *v1alpha1.NotificationPolicy {
	if len(items) == 0 {
		return nil
	}
	sorted := make([]v1alpha1.NotificationPolicy, len(items))
	copy(sorted, items)
	sort.SliceStable(sorted, func(i, j int) bool {
		ti, tj := sorted[i].CreationTimestamp.Time, sorted[j].CreationTimestamp.Time
		if !ti.Equal(tj) {
			return ti.Before(tj)
		}
		return sorted[i].Namespace+"/"+sorted[i].Name < sorted[j].Namespace+"/"+sorted[j].Name
	})
	return &sorted[0]
}

// policiesOfTenant enqueues all policies sharing a tenant (used for both Tenant and NotificationPolicy events).
func (r *NotificationPolicyReconciler) policiesOfTenant(ctx context.Context, tenantName string) []reconcile.Request {
	var list v1alpha1.NotificationPolicyList
	if err := r.List(ctx, &list, client.MatchingFields{index.IndexTenantRef: tenantName}); err != nil {
		return nil
	}
	return requestsFor(list.Items)
}

func (r *NotificationPolicyReconciler) contactPointToPolicies(ctx context.Context, o client.Object) []reconcile.Request {
	var list v1alpha1.NotificationPolicyList
	if err := r.List(ctx, &list, client.InNamespace(o.GetNamespace())); err != nil {
		return nil
	}
	return requestsFor(list.Items)
}

// SetupWithManager sets up the controller with the Manager.
func (r *NotificationPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.NotificationPolicy{}).
		// Siblings of the same tenant must re-evaluate the winner on any policy change (incl. delete).
		Watches(&v1alpha1.NotificationPolicy{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
			return r.policiesOfTenant(ctx, tenantRefOf(o))
		})).
		Watches(&v1alpha1.ContactPoint{}, handler.EnqueueRequestsFromMapFunc(r.contactPointToPolicies)).
		Watches(&v1alpha1.Tenant{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
			return r.policiesOfTenant(ctx, o.GetName())
		})).
		Named("notificationpolicy").
		Complete(r)
}
