package controller

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// lastAppliedAnnotation carries a JSON copy of the whole object, payload included, on anything
// created or updated with `kubectl apply` without server-side apply. Stripping a Secret's Data but
// leaving this behind would keep the secret material in the cache under a different key.
const lastAppliedAnnotation = "kubectl.kubernetes.io/last-applied-configuration"

// CacheOptions is the manager cache restriction the reconcilers in this package require. It must be
// passed to ctrl.NewManager together with UncachedObjects on the client, and the two belong
// together: this makes the Secret and ConfigMap informers hold no payload, and that makes every
// read of one go to the API server instead of the cache.
//
// Why the informers exist at all. Both watches are cluster-wide and have to be: a ContactPoint's
// secretKeyRef, a Tenant's backend basicAuthSecretRef and a Tenant's alertmanager.templatesRef name
// a (namespace, name) that is only known from the CRs, so there is no namespace or field selector
// that could be fixed at startup. Without the watches, a Secret appearing after the ContactPoint
// that references it would leave that ContactPoint at Accepted=False/SecretNotFound forever -- the
// ContactPoint reconciler has no resync of its own, and only the Secret event re-enqueues it.
//
// Why they hold no payload. An unrestricted informer for these two types keeps every Secret and
// every ConfigMap in the cluster, with its data, in memory: Helm release secrets
// (sh.helm.release.v1.*) are hundreds of KiB each, and CA bundles and dashboard ConfigMaps are
// comparable, against the chart's 256Mi limit -- an OOMKill, not a slowdown, and an OOMKill during
// a Tenant deletion wedges that Tenant Terminating because the finalizer needs a running operator.
// It is also state the operator has no business holding: it needs a Secret's value at the instant
// it compiles a document, not a resident copy of every Secret in the cluster. So the cache keeps
// identity and metadata only -- enough to answer "which Tenants care about this object?" in
// secretToTenants/configMapToTenants/secretToContactPoints, which is all any watch handler here
// does -- and the values are read live.
//
// labelSelector, when non-empty, narrows the informers further to objects carrying it (the
// --watch-label-selector flag). That is a real behaviour change and is off by default: an
// unlabelled Secret still resolves, because reads bypass the cache, but a change to it no longer
// enqueues anything, so it is only picked up at the Tenant's next resync -- and a ContactPoint
// waiting on a Secret that does not exist yet would never be re-checked at all, since that
// reconciler has no resync. Use it only on a cluster where holding metadata for every Secret is
// itself too much, and label every Secret and ConfigMap the CRs reference.
func CacheOptions(labelSelector string) (cache.Options, error) {
	byObject := cache.ByObject{Transform: stripPayload}
	if labelSelector != "" {
		sel, err := labels.Parse(labelSelector)
		if err != nil {
			return cache.Options{}, fmt.Errorf("watch label selector %q: %w", labelSelector, err)
		}
		byObject.Label = sel
	}
	return cache.Options{ByObject: map[client.Object]cache.ByObject{
		&corev1.Secret{}:    byObject,
		&corev1.ConfigMap{}: byObject,
	}}, nil
}

// UncachedObjects are the types whose reads must bypass the cache, because CacheOptions strips
// their payload before it is stored. Pass as client.Options{Cache: &client.CacheOptions{DisableFor:
// UncachedObjects()}}; without it every Secret would read back with empty Data and every
// ContactPoint would report SecretNotFound.
//
// The cost is one API GET per referenced Secret/ConfigMap per reconcile rather than a cache hit.
// For this operator that is a handful of GETs per Tenant per resyncInterval, which is the right
// trade for not holding the cluster's secret material in memory.
func UncachedObjects() []client.Object {
	return []client.Object{&corev1.Secret{}, &corev1.ConfigMap{}}
}

// stripPayload removes everything but identity and user-visible metadata before an object enters
// the informer store. Applied by the informer to each object as it is decoded, before it is
// indexed, so the full object never becomes resident.
//
// Two properties this has to hold, neither of which is about tombstones -- client-go v0.37 skips the
// transformer entirely for a DeletedFinalStateUnknown and for anything already transformed
// (RealFIFO.addToItems_locked, tools/cache/the_real_fifo.go:255-281), so one never reaches here:
//
//   - Total. One function is registered for both ByObject entries, so the type switch is what lets
//     it serve Secret and ConfigMap at once, and anything else must come back unchanged rather than
//     error -- a transform that returns an error fails the FIFO write and the object is dropped
//     from the cache entirely.
//   - Idempotent. RealFIFO documents that objects handed to Replace() may already have been
//     transformed and that re-transforming them must be safe. Nil-ing fields that are already nil
//     is, which is the only reason this shape is allowed to mutate in place.
func stripPayload(in any) (any, error) {
	switch o := in.(type) {
	case *corev1.Secret:
		o.Data = nil
		o.StringData = nil
		stripMeta(&o.ManagedFields, o.Annotations)
		return o, nil
	case *corev1.ConfigMap:
		o.Data = nil
		o.BinaryData = nil
		stripMeta(&o.ManagedFields, o.Annotations)
		return o, nil
	default:
		return in, nil
	}
}

func stripMeta(managedFields *[]metav1.ManagedFieldsEntry, annotations map[string]string) {
	*managedFields = nil
	delete(annotations, lastAppliedAnnotation)
}
