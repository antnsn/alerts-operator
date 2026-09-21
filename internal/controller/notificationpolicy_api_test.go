package controller

import (
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	observabilityv1alpha1 "github.com/antnsn/alerts-operator/api/v1alpha1"
)

func TestNotificationPolicyCEL(t *testing.T) {
	obj := &observabilityv1alpha1.NotificationPolicy{ObjectMeta: metav1.ObjectMeta{Name: "cel-a", Namespace: "default"},
		Spec: observabilityv1alpha1.NotificationPolicySpec{TenantRef: "t", Route: observabilityv1alpha1.Route{}}}
	if err := testClient.Create(testCtx, obj); err == nil {
		t.Fatalf("expected error for missing route.receiver")
	}
	obj.Spec.Route.Receiver = "keep"
	obj.Spec.Route.Routes = []observabilityv1alpha1.Route{{Receiver: "po", Matchers: []string{`severity="critical"`}}}
	if err := testClient.Create(testCtx, obj); err != nil {
		t.Fatalf("create: %v", err)
	}
	got := &observabilityv1alpha1.NotificationPolicy{}
	if err := testClient.Get(testCtx, client.ObjectKeyFromObject(obj), got); err != nil {
		t.Fatal(err)
	}
	if len(got.Spec.Route.Routes) != 1 || got.Spec.Route.Routes[0].Receiver != "po" {
		t.Fatalf("nested routes not round-tripped: %+v", got.Spec.Route)
	}
	_ = testClient.Delete(testCtx, obj)
}

func TestRouteReceivers(t *testing.T) {
	r := observabilityv1alpha1.Route{Receiver: "a", Routes: []observabilityv1alpha1.Route{{Receiver: "b", Routes: []observabilityv1alpha1.Route{{Receiver: "a"}}}, {Receiver: "c"}}}
	if got := r.Receivers(); !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		t.Fatalf("got %v", got)
	}
}
