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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/antnsn/alerts-operator/api/v1alpha1"
	"github.com/antnsn/alerts-operator/internal/index"
)

// ContactPointReconciler validates ContactPoints (tenant + secret refs) and sets Accepted.
type ContactPointReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=observability.antnsn.dev,resources=contactpoints,verbs=get;list;watch
// +kubebuilder:rbac:groups=observability.antnsn.dev,resources=contactpoints/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=observability.antnsn.dev,resources=tenants,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// Reconcile validates the ContactPoint against its referenced Tenant and Secret refs, then
// sets the Accepted condition.
func (r *ContactPointReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var cp v1alpha1.ContactPoint
	if err := r.Get(ctx, req.NamespacedName, &cp); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	status, reason, msg := metav1.ConditionTrue, v1alpha1.ReasonAccepted, ""
	var tenant v1alpha1.Tenant
	if err := r.Get(ctx, types.NamespacedName{Name: cp.Spec.TenantRef}, &tenant); errors.IsNotFound(err) {
		status, reason, msg = metav1.ConditionFalse, v1alpha1.ReasonTenantNotFound, fmt.Sprintf("Tenant %q not found", cp.Spec.TenantRef)
	} else if err != nil {
		return ctrl.Result{}, err
	} else {
		missing, err := missingSecretRef(ctx, r.Client, cp.Namespace, cp.SecretRefs())
		if err != nil {
			return ctrl.Result{}, err
		}
		if missing != "" {
			status, reason, msg = metav1.ConditionFalse, v1alpha1.ReasonSecretNotFound, missing
		}
	}

	changed := false
	err := patchStatus(ctx, r.Client, &cp, func() {
		changed = setCondition(&cp.Status.Conditions, v1alpha1.ConditionAccepted, status, reason, msg, cp.Generation)
		changed = changed || cp.Status.ObservedGeneration != cp.Generation
		cp.Status.ObservedGeneration = cp.Generation
	}, &changed)
	return ctrl.Result{}, err
}

// missingSecretRef returns a message for the first unresolvable ref, "" if all resolve.
func missingSecretRef(ctx context.Context, c client.Client, namespace string, refs []v1alpha1.SecretKeyRef) (string, error) {
	cache := map[string]*corev1.Secret{}
	for _, ref := range refs {
		sec, ok := cache[ref.Name]
		if !ok {
			sec = &corev1.Secret{}
			err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: ref.Name}, sec)
			if errors.IsNotFound(err) {
				sec = nil
			} else if err != nil {
				return "", err
			}
			cache[ref.Name] = sec
		}
		if sec == nil {
			return fmt.Sprintf("secret %s/%s not found", namespace, ref.Name), nil
		}
		if _, ok := sec.Data[ref.Key]; !ok {
			return fmt.Sprintf("secret %s/%s key %s not found", namespace, ref.Name, ref.Key), nil
		}
	}
	return "", nil
}

func (r *ContactPointReconciler) secretToContactPoints(ctx context.Context, o client.Object) []reconcile.Request {
	var list v1alpha1.ContactPointList
	if err := r.List(ctx, &list, client.InNamespace(o.GetNamespace()), client.MatchingFields{index.IndexSecretRefs: o.GetName()}); err != nil {
		return nil
	}
	return requestsFor(list.Items)
}

func (r *ContactPointReconciler) tenantToContactPoints(ctx context.Context, o client.Object) []reconcile.Request {
	var list v1alpha1.ContactPointList
	if err := r.List(ctx, &list, client.MatchingFields{index.IndexTenantRef: o.GetName()}); err != nil {
		return nil
	}
	return requestsFor(list.Items)
}

func requestsFor[T any, PT interface {
	*T
	client.Object
}](items []T) []reconcile.Request {
	out := make([]reconcile.Request, 0, len(items))
	for i := range items {
		out = append(out, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(PT(&items[i]))})
	}
	return out
}

// SetupWithManager sets up the controller with the Manager.
func (r *ContactPointReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.ContactPoint{}).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.secretToContactPoints)).
		Watches(&v1alpha1.Tenant{}, handler.EnqueueRequestsFromMapFunc(r.tenantToContactPoints)).
		Named("contactpoint").
		Complete(r)
}
