package controller

import (
	"context"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// byObjectFor finds the ByObject entry for T. cache.Options.ByObject is keyed by a client.Object
// whose GVK controller-runtime resolves internally, so it cannot be looked up with a freshly
// allocated &corev1.Secret{} (map keys on a pointer compare by address).
func byObjectFor[T client.Object](t *testing.T, opts cache.Options) cache.ByObject {
	t.Helper()
	for k, v := range opts.ByObject {
		if _, ok := k.(T); ok {
			return v
		}
	}
	var zero T
	t.Fatalf("no cache.ByObject restriction for %T: the informer would hold every one in the cluster", zero)
	return cache.ByObject{}
}

// TestCacheOptionsKeepsNoSecretOrConfigMapPayload is the memory and secret-handling contract of the
// manager cache. The Tenant and ContactPoint reconcilers watch Secrets and ConfigMaps cluster-wide
// (they must: a secretKeyRef or templatesRef can name any namespace), and an unrestricted informer
// for those two types holds every Secret and ConfigMap in the cluster *with its data* in memory --
// Helm release payloads, CA bundles, dashboards -- against a 256Mi limit
// (charts/alerts-operator/values.yaml). The cache exists only to know that an object changed; the
// values are read live through the API server (UncachedObjects).
func TestCacheOptionsKeepsNoSecretOrConfigMapPayload(t *testing.T) {
	opts, err := CacheOptions("")
	if err != nil {
		t.Fatal(err)
	}

	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name: "s", Namespace: "n",
			Labels:      map[string]string{"keep": "me"},
			Annotations: map[string]string{"kubectl.kubernetes.io/last-applied-configuration": `{"data":{"password":"hunter2"}}`, "keep": "me"},
			ManagedFields: []metav1.ManagedFieldsEntry{
				{Manager: "kubectl", Operation: metav1.ManagedFieldsOperationApply},
			},
		},
		Data:       map[string][]byte{"password": []byte("hunter2")},
		StringData: map[string]string{"token": "t0ken"},
	}
	out, err := byObjectFor[*corev1.Secret](t, opts).Transform(sec)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := out.(*corev1.Secret)
	if !ok {
		t.Fatalf("transform changed the type: %T", out)
	}
	if len(got.Data) != 0 || len(got.StringData) != 0 {
		t.Fatalf("secret material must never enter the cache: %+v %+v", got.Data, got.StringData)
	}
	if got.ManagedFields != nil {
		t.Fatalf("managedFields must be dropped: %+v", got.ManagedFields)
	}
	if _, ok := got.Annotations["kubectl.kubernetes.io/last-applied-configuration"]; ok {
		t.Fatal("last-applied-configuration carries a copy of the whole object, data included")
	}
	if got.Annotations["keep"] != "me" || got.Labels["keep"] != "me" || got.Name != "s" || got.Namespace != "n" {
		t.Fatalf("identity and user metadata must survive: %+v", got.ObjectMeta)
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "n"},
		Data:       map[string]string{"tmpl": strings.Repeat("x", 1024)},
		BinaryData: map[string][]byte{"blob": []byte("xxxx")},
	}
	outCM, err := byObjectFor[*corev1.ConfigMap](t, opts).Transform(cm)
	if err != nil {
		t.Fatal(err)
	}
	gotCM := outCM.(*corev1.ConfigMap)
	if len(gotCM.Data) != 0 || len(gotCM.BinaryData) != 0 {
		t.Fatalf("configmap payload must never enter the cache: %+v %+v", gotCM.Data, gotCM.BinaryData)
	}
	if gotCM.Name != "c" {
		t.Fatalf("identity must survive: %+v", gotCM.ObjectMeta)
	}
}

// TestCacheOptionsTransformIsTotalAndIdempotent pins the two properties an informer transform must
// have. Neither is about tombstones: client-go v0.37 skips the transformer for a
// DeletedFinalStateUnknown and for already-transformed objects
// (RealFIFO.addToItems_locked, tools/cache/the_real_fifo.go:255-281), so one never reaches it.
//
//   - Total, because one function is registered for both ByObject entries and an error from a
//     transform fails the FIFO write, dropping the object from the cache.
//   - Idempotent, because RealFIFO documents that objects handed to Replace() may already have been
//     transformed -- which is what makes mutating in place safe here.
func TestCacheOptionsTransformIsTotalAndIdempotent(t *testing.T) {
	opts, err := CacheOptions("")
	if err != nil {
		t.Fatal(err)
	}
	transform := byObjectFor[*corev1.Secret](t, opts).Transform

	unrecognised := struct{ Key string }{Key: "n/s"}
	out, err := transform(unrecognised)
	if err != nil {
		t.Fatal(err)
	}
	if out != any(unrecognised) {
		t.Fatalf("an object the transform does not recognise must be returned unchanged, got %#v", out)
	}

	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "s", Namespace: "n", Labels: map[string]string{"keep": "me"}},
		Data:       map[string][]byte{"password": []byte("hunter2")},
	}
	once, err := transform(sec)
	if err != nil {
		t.Fatal(err)
	}
	twice, err := transform(once)
	if err != nil {
		t.Fatalf("re-transforming an already-transformed object must not fail: %v", err)
	}
	if !reflect.DeepEqual(once, twice) {
		t.Fatalf("transform is not idempotent: %+v then %+v", once, twice)
	}
	if s := twice.(*corev1.Secret); len(s.Data) != 0 || s.Labels["keep"] != "me" {
		t.Fatalf("a second pass must neither restore payload nor lose metadata: %+v", s)
	}
}

// TestCacheOptionsLabelSelector covers the optional narrowing knob (--watch-label-selector): on a
// cluster where even metadata for every Secret is more than the operator should hold, the informers
// can be restricted further, at the cost of only labelled objects triggering an immediate reconcile.
func TestCacheOptionsLabelSelector(t *testing.T) {
	opts, err := CacheOptions("observability.antnsn.dev/watch=true")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range []cache.ByObject{byObjectFor[*corev1.Secret](t, opts), byObjectFor[*corev1.ConfigMap](t, opts)} {
		if e.Label == nil {
			t.Fatal("label selector not applied")
		}
		if !e.Label.Matches(labels.Set{"observability.antnsn.dev/watch": "true"}) {
			t.Fatal("selector does not match a labelled object")
		}
		if e.Label.Matches(labels.Set{"other": "x"}) {
			t.Fatal("selector matches an unlabelled object")
		}
	}

	if _, err := CacheOptions("=not a selector="); err == nil {
		t.Fatal("an unparseable selector must fail at startup, not silently widen the cache")
	}
}

// TestCacheOptionsCacheHoldsNoSecretDataAgainstARealAPIServer is the end-to-end form of the same
// contract: a cache built exactly as the manager builds it, run against envtest, must serve the
// Secret's identity and nothing else -- while the same Secret read through an uncached client has
// its data. A transform wired into the wrong place, or dropped entirely, fails here.
func TestCacheOptionsCacheHoldsNoSecretDataAgainstARealAPIServer(t *testing.T) {
	opts, err := CacheOptions("")
	if err != nil {
		t.Fatal(err)
	}
	opts.Scheme = scheme.Scheme
	c, err := cache.New(testCfg, opts)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(testCtx)
	t.Cleanup(cancel)
	go func() { _ = c.Start(ctx) }()
	if !c.WaitForCacheSync(ctx) {
		t.Fatal("cache sync failed")
	}

	sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "cache-strip", Namespace: "default"},
		Data: map[string][]byte{"password": []byte("hunter2")}}
	createAndCleanup(t, sec)

	var cached corev1.Secret
	waitFor(t, func() bool { return c.Get(ctx, clientKey(sec), &cached) == nil })
	if len(cached.Data) != 0 {
		t.Fatalf("the informer store is holding secret material: %+v", cached.Data)
	}

	var live corev1.Secret
	if err := testClient.Get(testCtx, clientKey(sec), &live); err != nil {
		t.Fatal(err)
	}
	if string(live.Data["password"]) != "hunter2" {
		t.Fatalf("precondition: the API server must still hold the value the operator reads live, got %+v", live.Data)
	}
}
