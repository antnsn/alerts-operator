package v1alpha1

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestConditionedImplementations(t *testing.T) {
	objs := []Conditioned{&Tenant{}, &ContactPoint{}, &NotificationPolicy{}, &AlertRuleGroup{}}
	for _, o := range objs {
		o.SetConditions([]metav1.Condition{{Type: "X", Status: metav1.ConditionTrue}})
		if got := o.GetConditions(); len(got) != 1 || got[0].Type != "X" {
			t.Fatalf("%T round trip failed: %+v", o, got)
		}
	}
}
