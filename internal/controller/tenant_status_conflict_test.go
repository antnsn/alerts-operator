package controller

import (
	"context"
	"errors"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	observabilityv1alpha1 "github.com/antnsn/alerts-operator/api/v1alpha1"
	fakebackend "github.com/antnsn/alerts-operator/internal/backend/fake"
)

// tenantsGR is the GroupResource a real API server names in a 409 on a Tenant write.
var tenantsGR = schema.GroupResource{Group: "observability.antnsn.dev", Resource: "tenants"}

// conflictOnce builds a fake client that answers the first call of the intercepted write with a
// 409 Conflict shaped exactly like the API server's ("the object has been modified"), then behaves
// normally. status=true intercepts the status subresource patch; false intercepts the main Update.
func conflictOnce(t *testing.T, status bool, objs ...client.Object) (client.Client, *int) {
	t.Helper()
	calls := 0
	conflict := func(name string) error {
		calls++
		if calls == 1 {
			return apierrors.NewConflict(tenantsGR, name, errors.New("the object has been modified; please apply your changes to the latest version and try again"))
		}
		return nil
	}
	funcs := interceptor.Funcs{}
	if status {
		funcs.SubResourcePatch = func(ctx context.Context, c client.Client, _ string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			if err := conflict(obj.GetName()); err != nil {
				return err
			}
			return c.Status().Patch(ctx, obj, patch, opts...)
		}
	} else {
		funcs.Update = func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if err := conflict(obj.GetName()); err != nil {
				return err
			}
			return c.Update(ctx, obj, opts...)
		}
	}
	return newChildrenFakeClientBuilder(objs...).WithInterceptorFuncs(funcs).Build(), &calls
}

// TestTenantStatusConflictIsRequeuedNotErrored covers bead d2e: when two children change in the
// same second, the Tenant's optimistic-lock status patch loses to a concurrent writer and comes
// back 409. Returning that as a reconcile error made controller-runtime log
// "ERROR Reconciler error" with a stack trace on every such burst, although the retry always
// succeeded. A conflict is the optimistic lock doing its job: requeue quietly and converge.
func TestTenantStatusConflictIsRequeuedNotErrored(t *testing.T) {
	s := fakebackend.New()
	t.Cleanup(s.Close)

	tn := amPeer("status-conflict", "1", s.URL, "sc")
	tn.Generation = 1
	controllerutil.AddFinalizer(tn, tenantFinalizer) // skip the add-finalizer Update path
	c, calls := conflictOnce(t, true, tn)
	r := &TenantReconciler{Client: c, Scheme: scheme.Scheme, Recorder: record.NewFakeRecorder(20)}

	res, err := r.Reconcile(context.Background(), tenantRequest(tn.Name))
	if err != nil {
		t.Fatalf("a 409 on the status patch must not surface as a reconcile error, got %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Fatalf("a 409 on the status patch must requeue, got %+v", res)
	}
	if *calls != 1 {
		t.Fatalf("expected exactly one (conflicting) status patch attempt, got %d", *calls)
	}

	// The retry converges: the second pass patches for real and Ready lands.
	if _, err := r.Reconcile(context.Background(), tenantRequest(tn.Name)); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	var got observabilityv1alpha1.Tenant
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(tn), &got); err != nil {
		t.Fatal(err)
	}
	if meta.FindStatusCondition(got.Status.Conditions, observabilityv1alpha1.ConditionReady) == nil {
		t.Fatalf("Ready must be recorded once the conflict clears, got %+v", got.Status.Conditions)
	}
}

// TestTenantFinalizerUpdateConflictIsRequeuedNotErrored: the same 409 on the finalizer-adding
// Update (a child's own reconciler and the Tenant's first pass racing on a fresh object) is
// equally benign and must not be logged as a reconciler error either.
func TestTenantFinalizerUpdateConflictIsRequeuedNotErrored(t *testing.T) {
	s := fakebackend.New()
	t.Cleanup(s.Close)

	tn := amPeer("finalizer-conflict", "1", s.URL, "fc")
	tn.Generation = 1
	c, calls := conflictOnce(t, false, tn)
	r := &TenantReconciler{Client: c, Scheme: scheme.Scheme, Recorder: record.NewFakeRecorder(20)}

	res, err := r.Reconcile(context.Background(), tenantRequest(tn.Name))
	if err != nil {
		t.Fatalf("a 409 on the finalizer Update must not surface as a reconcile error, got %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Fatalf("a 409 on the finalizer Update must requeue, got %+v", res)
	}
	if *calls != 1 {
		t.Fatalf("expected exactly one (conflicting) Update attempt, got %d", *calls)
	}
	if _, err := r.Reconcile(context.Background(), tenantRequest(tn.Name)); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	var got observabilityv1alpha1.Tenant
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(tn), &got); err != nil {
		t.Fatal(err)
	}
	if !controllerutil.ContainsFinalizer(&got, tenantFinalizer) {
		t.Fatal("finalizer must be present once the conflict clears")
	}
}
