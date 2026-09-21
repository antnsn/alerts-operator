package controller

import (
	"reflect"
	"strings"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
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
	obj.Spec.Route.Routes = []apiextensionsv1.JSON{{Raw: []byte(`{"receiver":"po","matchers":["severity=\"critical\""]}`)}}
	if err := testClient.Create(testCtx, obj); err != nil {
		t.Fatalf("create: %v", err)
	}
	got := &observabilityv1alpha1.NotificationPolicy{}
	if err := testClient.Get(testCtx, client.ObjectKeyFromObject(obj), got); err != nil {
		t.Fatal(err)
	}
	children, err := got.Spec.Route.ChildRoutes()
	if err != nil {
		t.Fatal(err)
	}
	if len(children) != 1 || children[0].Receiver != "po" {
		t.Fatalf("nested routes not round-tripped: %+v", got.Spec.Route)
	}
	_ = testClient.Delete(testCtx, obj)
}

func TestRouteReceivers(t *testing.T) {
	r := observabilityv1alpha1.Route{Receiver: "a", Routes: []apiextensionsv1.JSON{
		{Raw: []byte(`{"receiver":"b","routes":[{"receiver":"a"}]}`)},
		{Raw: []byte(`{"receiver":"c"}`)},
	}}
	got, err := r.Receivers()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		t.Fatalf("got %v", got)
	}

	bad := observabilityv1alpha1.Route{Receiver: "a", Routes: []apiextensionsv1.JSON{{Raw: []byte("42")}}}
	if _, err := bad.Receivers(); err == nil || !strings.Contains(err.Error(), "routes[0]") {
		t.Fatalf("expected decode error mentioning routes[0], got %v", err)
	}
}
