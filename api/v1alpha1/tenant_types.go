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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	// DefaultRulesNamespacePrefix is the default namespace prefix for rule scoping.
	DefaultRulesNamespacePrefix = "alerts-operator"
	// DefaultResyncInterval is the default drift-repair period.
	DefaultResyncInterval = 5 * time.Minute
)

// BackendSpec describes how to reach one Mimir or Loki instance.
type BackendSpec struct {
	// Address is the base URL of the backend gateway, e.g. http://mimir-distributed-nginx.mimir:80
	// +kubebuilder:validation:Pattern=`^https?://`
	Address string `json:"address"`
	// +optional
	Auth *BackendAuth `json:"auth,omitempty"`
}

// BackendAuth describes authentication credentials for a backend.
type BackendAuth struct {
	// BasicAuthSecretRef points at a Secret with keys `username` and `password`.
	// +optional
	BasicAuthSecretRef *NamespacedName `json:"basicAuthSecretRef,omitempty"`
}

// AlertmanagerSpec describes how to reach an Alertmanager instance.
type AlertmanagerSpec struct {
	// TemplatesRef points at a ConfigMap whose entries become Alertmanager template_files.
	// +optional
	TemplatesRef *NamespacedName `json:"templatesRef,omitempty"`
}

// TenantSpec defines the desired state of Tenant.
// +kubebuilder:validation:XValidation:rule="has(self.mimir) || has(self.loki)",message="at least one of spec.mimir or spec.loki must be set"
// +kubebuilder:validation:XValidation:rule="!has(self.alertmanager) || has(self.mimir)",message="spec.alertmanager requires spec.mimir"
type TenantSpec struct {
	// TenantID is sent as X-Scope-OrgID on every backend request.
	// +kubebuilder:validation:MinLength=1
	TenantID string `json:"tenantId"`
	// +optional
	Mimir *BackendSpec `json:"mimir,omitempty"`
	// +optional
	Loki *BackendSpec `json:"loki,omitempty"`
	// +optional
	Alertmanager *AlertmanagerSpec `json:"alertmanager,omitempty"`
	// RulesNamespacePrefix scopes which backend rule namespaces this operator owns. Default "alerts-operator".
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9_.-]+$`
	// +optional
	RulesNamespacePrefix string `json:"rulesNamespacePrefix,omitempty"`
	// ResyncInterval is the drift-repair period. Default 5m.
	// +optional
	ResyncInterval *metav1.Duration `json:"resyncInterval,omitempty"`
}

// RuleGroupCounts tracks the number of rule groups per backend.
type RuleGroupCounts struct {
	Mimir int32 `json:"mimir"`
	Loki  int32 `json:"loki"`
}

// TenantStatus defines the observed state of Tenant.
type TenantStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// +optional
	AlertmanagerConfigHash string `json:"alertmanagerConfigHash,omitempty"`
	// +optional
	RuleGroups RuleGroupCounts `json:"ruleGroups,omitempty"`
}

// Tenant is the Schema for the tenants API.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:printcolumn:name="Tenant",type=string,JSONPath=`.spec.tenantId`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:validation:XValidation:rule="!has(self.spec.resyncInterval) || self.spec.resyncInterval.matches('^[0-9]+(ns|us|ms|s|m|h)([0-9]+(ns|us|ms|s|m|h))*$')",message="spec.resyncInterval must be a valid Go duration (e.g. '5m', '1h30m')"
type Tenant struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TenantSpec   `json:"spec"`
	Status TenantStatus `json:"status,omitempty"`
}

// Prefix returns the effective rules namespace prefix, using the default if not specified.
func (t *Tenant) Prefix() string {
	if t.Spec.RulesNamespacePrefix == "" {
		return DefaultRulesNamespacePrefix
	}
	return t.Spec.RulesNamespacePrefix
}

// Resync returns the effective resync interval, using the default if not specified.
func (t *Tenant) Resync() time.Duration {
	if t.Spec.ResyncInterval == nil || t.Spec.ResyncInterval.Duration <= 0 {
		return DefaultResyncInterval
	}
	return t.Spec.ResyncInterval.Duration
}

// TenantList contains a list of Tenant.
// +kubebuilder:object:root=true
type TenantList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Tenant `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &Tenant{}, &TenantList{})
		return nil
	})
}
