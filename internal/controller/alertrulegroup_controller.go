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

// Package controller contains the reconcilers for the observability.antnsn.dev APIs.
package controller

import (
	"context"
	"fmt"

	"github.com/prometheus/common/model"
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

// AlertRuleGroupReconciler validates AlertRuleGroups and sets Accepted. It never writes to a backend.
type AlertRuleGroupReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=observability.antnsn.dev,resources=alertrulegroups,verbs=get;list;watch
// +kubebuilder:rbac:groups=observability.antnsn.dev,resources=alertrulegroups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=observability.antnsn.dev,resources=tenants,verbs=get;list;watch

// Reconcile validates the AlertRuleGroup against its referenced Tenant and rule syntax, then
// sets the Accepted condition. It never writes to a backend -- that is the Tenant reconciler's job.
func (r *AlertRuleGroupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var arg v1alpha1.AlertRuleGroup
	if err := r.Get(ctx, req.NamespacedName, &arg); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	status, reason, msg := metav1.ConditionTrue, v1alpha1.ReasonAccepted, ""
	backendNS := ""

	var tenant v1alpha1.Tenant
	switch err := r.Get(ctx, types.NamespacedName{Name: arg.Spec.TenantRef}, &tenant); {
	case errors.IsNotFound(err):
		status, reason, msg = metav1.ConditionFalse, v1alpha1.ReasonTenantNotFound, fmt.Sprintf("Tenant %q not found", arg.Spec.TenantRef)
	case err != nil:
		return ctrl.Result{}, err
	default:
		backendNS = compile.BackendNamespace(arg.Spec.Backend, tenant.Prefix(), arg.Namespace, arg.Name)
		switch {
		case arg.Spec.Backend == v1alpha1.BackendMimir && tenant.Spec.Mimir == nil,
			arg.Spec.Backend == v1alpha1.BackendLoki && tenant.Spec.Loki == nil:
			status, reason, msg = metav1.ConditionFalse, v1alpha1.ReasonBackendNotConfigured, fmt.Sprintf("Tenant %q has no %s backend", tenant.Name, arg.Spec.Backend)
		default:
			if verr := validateRuleGroups(arg.Spec.Backend, arg.Spec.Groups); verr != nil {
				status, reason, msg = metav1.ConditionFalse, v1alpha1.ReasonInvalidRule, verr.Error()
			}
		}
	}

	changed := false
	err := patchStatus(ctx, r.Client, &arg, func() {
		changed = setCondition(&arg.Status.Conditions, v1alpha1.ConditionAccepted, status, reason, msg, arg.Generation)
		changed = changed || arg.Status.ObservedGeneration != arg.Generation || arg.Status.BackendNamespace != backendNS
		arg.Status.ObservedGeneration = arg.Generation
		arg.Status.BackendNamespace = backendNS
	}, &changed)
	return ctrl.Result{}, err
}

// validateRuleGroups checks durations for all backends and PromQL syntax for Mimir.
// LogQL is validated by Loki itself at sync time.
func validateRuleGroups(be v1alpha1.Backend, groups []v1alpha1.RuleGroup) error {
	for _, g := range groups {
		if g.Interval != "" {
			if _, err := model.ParseDuration(g.Interval); err != nil {
				return fmt.Errorf("group %s: interval: %w", g.Name, err)
			}
		}
		for i, rule := range g.Rules {
			if rule.For != "" {
				if _, err := model.ParseDuration(rule.For); err != nil {
					return fmt.Errorf("group %s rule %d: for: %w", g.Name, i, err)
				}
			}
			if rule.KeepFiringFor != "" {
				if _, err := model.ParseDuration(rule.KeepFiringFor); err != nil {
					return fmt.Errorf("group %s rule %d: keep_firing_for: %w", g.Name, i, err)
				}
			}
			if be == v1alpha1.BackendMimir {
				if err := compile.ValidatePromQL(rule.Expr); err != nil {
					return fmt.Errorf("group %s rule %d: expr: %w", g.Name, i, err)
				}
			}
		}
	}
	return nil
}

// tenantToRuleGroups re-enqueues every AlertRuleGroup that references a changed Tenant.
func (r *AlertRuleGroupReconciler) tenantToRuleGroups(ctx context.Context, o client.Object) []reconcile.Request {
	var list v1alpha1.AlertRuleGroupList
	if err := r.List(ctx, &list, client.MatchingFields{index.IndexTenantRef: o.GetName()}); err != nil {
		return nil
	}
	return requestsFor(list.Items)
}

// SetupWithManager sets up the controller with the Manager.
func (r *AlertRuleGroupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.AlertRuleGroup{}).
		Watches(&v1alpha1.Tenant{}, handler.EnqueueRequestsFromMapFunc(r.tenantToRuleGroups)).
		Named("alertrulegroup").
		Complete(r)
}
