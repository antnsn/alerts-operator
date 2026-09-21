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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// Backend specifies which backend (Mimir or Loki) to use for alert rules.
// +kubebuilder:validation:Enum=mimir;loki
type Backend string

const (
	// BackendMimir uses Grafana Mimir for alert rules.
	BackendMimir Backend = "mimir"
	// BackendLoki uses Grafana Loki for alert rules.
	BackendLoki Backend = "loki"
)

// Rule is one alerting or recording rule (PrometheusRule-compatible shape).
// +kubebuilder:validation:XValidation:rule="has(self.alert) != has(self.record)",message="exactly one of alert or record must be set"
type Rule struct {
	// +kubebuilder:validation:MinLength=1
	// +optional
	Record string `json:"record,omitempty"`
	// +kubebuilder:validation:MinLength=1
	// +optional
	Alert string `json:"alert,omitempty"`
	// Expr must be a YAML string. PrometheusRule allows a bare number (IntOrString); quote it here, e.g. expr: "1".
	// +kubebuilder:validation:MinLength=1
	Expr string `json:"expr"`
	// +optional
	For string `json:"for,omitempty"`
	// +optional
	KeepFiringFor string `json:"keep_firing_for,omitempty"`
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
}

// RuleGroup is a collection of alert and recording rules grouped by name.
type RuleGroup struct {
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
	// +optional
	Interval string `json:"interval,omitempty"`
	// +kubebuilder:validation:MinItems=1
	Rules []Rule `json:"rules"`
}

// AlertRuleGroupSpec defines the desired state of AlertRuleGroup.
// Group-name uniqueness is enforced by +listType=map on Groups (no CEL needed).
type AlertRuleGroupSpec struct {
	// TenantRef is the name of the cluster-scoped Tenant.
	// +kubebuilder:validation:MinLength=1
	TenantRef string  `json:"tenantRef"`
	Backend   Backend `json:"backend"`
	// +kubebuilder:validation:MinItems=1
	// +listType=map
	// +listMapKey=name
	Groups []RuleGroup `json:"groups"`
}

// AlertRuleGroupStatus defines the observed state of AlertRuleGroup.
type AlertRuleGroupStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// BackendNamespace is the rule namespace used in the backend, <prefix>/<namespace>/<name>.
	// +optional
	BackendNamespace string `json:"backendNamespace,omitempty"`
}

// AlertRuleGroup is the Schema for the alertrulegroups API.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Tenant",type=string,JSONPath=`.spec.tenantRef`
// +kubebuilder:printcolumn:name="Backend",type=string,JSONPath=`.spec.backend`
// +kubebuilder:printcolumn:name="Accepted",type=string,JSONPath=`.status.conditions[?(@.type=="Accepted")].status`
// +kubebuilder:printcolumn:name="Synced",type=string,JSONPath=`.status.conditions[?(@.type=="Synced")].status`
type AlertRuleGroup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              AlertRuleGroupSpec   `json:"spec"`
	Status            AlertRuleGroupStatus `json:"status,omitempty"`
}

// AlertRuleGroupList contains a list of AlertRuleGroup.
// +kubebuilder:object:root=true
type AlertRuleGroupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AlertRuleGroup `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &AlertRuleGroup{}, &AlertRuleGroupList{})
		return nil
	})
}
