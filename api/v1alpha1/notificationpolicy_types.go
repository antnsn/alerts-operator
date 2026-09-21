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

// Route is an Alertmanager routing tree node. Receiver names refer to ContactPoints in the policy's namespace.
type Route struct {
	// +kubebuilder:validation:MinLength=1
	Receiver string `json:"receiver"`
	// +optional
	GroupBy []string `json:"groupBy,omitempty"`
	// +optional
	GroupWait string `json:"groupWait,omitempty"`
	// +optional
	GroupInterval string `json:"groupInterval,omitempty"`
	// +optional
	RepeatInterval string `json:"repeatInterval,omitempty"`
	// Matchers use Alertmanager matcher syntax, e.g. `severity="critical"`.
	// +optional
	Matchers []string `json:"matchers,omitempty"`
	// +optional
	Continue bool `json:"continue,omitempty"`
	// Routes are child routes. Schemaless because the type is recursive; validated at reconcile time.
	// +kubebuilder:validation:Schemaless
	// +kubebuilder:pruning:PreserveUnknownFields
	// +optional
	Routes []Route `json:"routes,omitempty"`
}

// Receivers returns the unique receiver names in the tree, depth-first, in first-seen order.
func (r *Route) Receivers() []string {
	seen := map[string]bool{}
	var out []string
	var walk func(*Route)
	walk = func(n *Route) {
		if !seen[n.Receiver] {
			seen[n.Receiver] = true
			out = append(out, n.Receiver)
		}
		for i := range n.Routes {
			walk(&n.Routes[i])
		}
	}
	walk(r)
	return out
}

// InhibitRule mirrors an Alertmanager inhibition rule.
type InhibitRule struct {
	// +optional
	SourceMatchers []string `json:"sourceMatchers,omitempty"`
	// +optional
	TargetMatchers []string `json:"targetMatchers,omitempty"`
	// +optional
	Equal []string `json:"equal,omitempty"`
}

// NotificationPolicySpec defines the desired state of NotificationPolicy.
type NotificationPolicySpec struct {
	// +kubebuilder:validation:MinLength=1
	TenantRef string `json:"tenantRef"`
	Route     Route  `json:"route"`
	// +optional
	InhibitRules []InhibitRule `json:"inhibitRules,omitempty"`
}

// NotificationPolicyStatus defines the observed state of NotificationPolicy.
type NotificationPolicyStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// NotificationPolicy is the Schema for the notificationpolicies API.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Tenant",type=string,JSONPath=`.spec.tenantRef`
// +kubebuilder:printcolumn:name="Accepted",type=string,JSONPath=`.status.conditions[?(@.type=="Accepted")].status`
// +kubebuilder:printcolumn:name="Synced",type=string,JSONPath=`.status.conditions[?(@.type=="Synced")].status`
type NotificationPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              NotificationPolicySpec   `json:"spec"`
	Status            NotificationPolicyStatus `json:"status,omitempty"`
}

// NotificationPolicyList contains a list of NotificationPolicy.
// +kubebuilder:object:root=true
type NotificationPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NotificationPolicy `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &NotificationPolicy{}, &NotificationPolicyList{})
		return nil
	})
}
