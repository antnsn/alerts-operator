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
	// Immutable, for the same reason as RulesNamespacePrefix below: tenantId, the backend address
	// and the prefix are the three coordinates that decide which backend state this Tenant owns,
	// and the reconciler only ever lists and prunes within the *current* ones. Changing tenantId
	// leaves every rule namespace and the Alertmanager config live in the old org, where neither
	// the prune loop nor the finalizer will ever look again -- duplicate, un-pruned rules/alerts
	// indefinitely, not even recoverable by deleting the Tenant. (The addresses stay mutable:
	// repointing at a moved gateway is legitimate operations, and the orphaning that can cause is
	// documented rather than forbidden.) Unlike RulesNamespacePrefix this needs no default marker
	// to make the transition rule bite -- the field is required, so oldSelf always exists.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="tenantId is immutable"
	TenantID string `json:"tenantId"`
	// +optional
	Mimir *BackendSpec `json:"mimir,omitempty"`
	// +optional
	Loki *BackendSpec `json:"loki,omitempty"`
	// +optional
	Alertmanager *AlertmanagerSpec `json:"alertmanager,omitempty"`
	// RulesNamespacePrefix scopes which backend rule namespaces this operator owns. Default "alerts-operator".
	// Immutable: the Tenant reconciler only prunes backend rule namespaces it can positively confirm
	// it owns under the *current* prefix, so changing this after creation would silently orphan every
	// namespace already written under the old prefix (duplicate, un-pruned rules/alerts indefinitely).
	// Defaulted (not left to Prefix()'s Go-side fallback alone) so the field is always materialised
	// in the stored object: "self == oldSelf" is a transition rule and is not evaluated against a
	// field that was absent on the old object, so an unset-then-set edit would otherwise bypass
	// immutability entirely -- the field must actually be present from creation for oldSelf to exist.
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9_.-]+$`
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="rulesNamespacePrefix is immutable"
	// +kubebuilder:default=alerts-operator
	// +optional
	RulesNamespacePrefix string `json:"rulesNamespacePrefix,omitempty"`
	// ResyncInterval is the drift-repair period. Default 5m.
	// +optional
	ResyncInterval *metav1.Duration `json:"resyncInterval,omitempty"`
}

// RuleGroupCounts tracks the number of rule groups per backend.
//
// Both fields carry +kubebuilder:default=0 despite being required: a status patch is a JSON merge
// patch built from a before/after diff (see patchStatus in internal/controller/conditions.go), so
// the very first time only one of Mimir/Loki moves off its Go zero value, the generated patch omits
// the untouched sibling entirely. Applied to a stored object that has never had a ruleGroups key at
// all, that produces a partial {mimir: 1} (or {loki: 1}) object missing its required sibling, which
// CRD structural-schema validation rejects -- permanently, since the same partial diff recurs every
// reconcile. The default lets the API server's structural-schema defaulting fill the missing sibling
// in before required-validation runs, so a partial patch still resolves to a valid object.
type RuleGroupCounts struct {
	// +kubebuilder:default=0
	Mimir int32 `json:"mimir"`
	// +kubebuilder:default=0
	Loki int32 `json:"loki"`
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
