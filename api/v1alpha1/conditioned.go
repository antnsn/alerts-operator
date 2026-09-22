package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Conditioned is implemented by every kind that carries status.conditions.
// +kubebuilder:object:generate=false
type Conditioned interface {
	client.Object
	GetConditions() []metav1.Condition
	SetConditions([]metav1.Condition)
}

// GetConditions returns status.conditions.
func (t *Tenant) GetConditions() []metav1.Condition { return t.Status.Conditions }

// SetConditions replaces status.conditions.
func (t *Tenant) SetConditions(c []metav1.Condition) { t.Status.Conditions = c }

// GetConditions returns status.conditions.
func (c *ContactPoint) GetConditions() []metav1.Condition { return c.Status.Conditions }

// SetConditions replaces status.conditions.
func (c *ContactPoint) SetConditions(v []metav1.Condition) { c.Status.Conditions = v }

// GetConditions returns status.conditions.
func (n *NotificationPolicy) GetConditions() []metav1.Condition { return n.Status.Conditions }

// SetConditions replaces status.conditions.
func (n *NotificationPolicy) SetConditions(v []metav1.Condition) { n.Status.Conditions = v }

// GetConditions returns status.conditions.
func (a *AlertRuleGroup) GetConditions() []metav1.Condition { return a.Status.Conditions }

// SetConditions replaces status.conditions.
func (a *AlertRuleGroup) SetConditions(v []metav1.Condition) { a.Status.Conditions = v }

var (
	_ Conditioned = (*Tenant)(nil)
	_ Conditioned = (*ContactPoint)(nil)
	_ Conditioned = (*NotificationPolicy)(nil)
	_ Conditioned = (*AlertRuleGroup)(nil)
)
