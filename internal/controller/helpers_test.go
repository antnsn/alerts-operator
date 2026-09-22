package controller

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	observabilityv1alpha1 "github.com/antnsn/alerts-operator/api/v1alpha1"
	"github.com/antnsn/alerts-operator/internal/backend/fake"
)

// newFakeTenant creates a Tenant whose selected backends point at a fresh fake server.
func newFakeTenant(t *testing.T, name string, withMimir, withLoki bool) (*observabilityv1alpha1.Tenant, *fake.Server) {
	t.Helper()
	s := fake.New()
	tn := &observabilityv1alpha1.Tenant{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: observabilityv1alpha1.TenantSpec{TenantID: "1"}}
	if withMimir {
		tn.Spec.Mimir = &observabilityv1alpha1.BackendSpec{Address: s.URL}
	}
	if withLoki {
		tn.Spec.Loki = &observabilityv1alpha1.BackendSpec{Address: s.URL}
	}
	if err := testClient.Create(testCtx, tn); err != nil {
		s.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = testClient.Delete(testCtx, tn)
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if err := testClient.Get(testCtx, client.ObjectKeyFromObject(tn), &observabilityv1alpha1.Tenant{}); errors.IsNotFound(err) {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		s.Close()
	})
	return tn, s
}

// waitCondition polls until obj has the condition with the given status (and reason, if non-empty).
func waitCondition(t *testing.T, obj observabilityv1alpha1.Conditioned, typ string, status metav1.ConditionStatus, reason string) {
	t.Helper()
	key := client.ObjectKeyFromObject(obj)
	var last *metav1.Condition
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if err := testClient.Get(testCtx, key, obj); err == nil {
			last = meta.FindStatusCondition(obj.GetConditions(), typ)
			if last != nil && last.Status == status && (reason == "" || last.Reason == reason) {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("%s %s: wanted %s=%s/%s, last seen %+v", key, typ, typ, status, reason, last)
}

func createAndCleanup(t *testing.T, obj client.Object) {
	t.Helper()
	if err := testClient.Create(testCtx, obj); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = testClient.Delete(testCtx, obj) })
}

func clientKey(obj client.Object) client.ObjectKey { return client.ObjectKeyFromObject(obj) }
