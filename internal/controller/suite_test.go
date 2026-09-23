package controller

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	observabilityv1alpha1 "github.com/antnsn/alerts-operator/api/v1alpha1"
	"github.com/antnsn/alerts-operator/internal/index"
)

var (
	testCfg    *rest.Config
	testClient client.Client
	// testCacheClient is the manager's cache-backed client. Field indexers registered via
	// mgr.GetFieldIndexer() (internal/index) are a controller-runtime cache feature only —
	// the API server has no knowledge of them — so MatchingFields queries must go through
	// this client, not the uncached testClient. Reads through it are eventually consistent
	// with writes, so callers must still poll with waitFor.
	testCacheClient client.Client
	testCtx         context.Context
	testCancel      context.CancelFunc
)

// TestMain starts a shared envtest API server and a manager with all reconcilers.
func TestMain(m *testing.M) {
	testEnv := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}
	var err error
	testCfg, err = testEnv.Start()
	if err != nil {
		panic(err)
	}
	if err := observabilityv1alpha1.AddToScheme(scheme.Scheme); err != nil {
		panic(err)
	}
	// testClient is an uncached, direct client (not mgr.GetClient()): it talks straight to
	// the API server, so a Create followed immediately by a Get is guaranteed to observe the
	// write. The manager's cache-backed client syncs asynchronously and would otherwise race
	// tests that create then immediately read back.
	testClient, err = client.New(testCfg, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		panic(err)
	}
	testCtx, testCancel = context.WithCancel(context.Background())
	// Exactly the cache/client pairing cmd/main.go ships: Secret and ConfigMap informers that hold
	// no payload, and reads of those two types that bypass the cache. Wired here and not only in
	// main so that every existing test which creates an unlabelled Secret and expects the
	// reconcilers to resolve it is a live check that the shipped configuration still works.
	cacheOptions, err := CacheOptions("")
	if err != nil {
		panic(err)
	}
	mgr, err := ctrl.NewManager(testCfg, ctrl.Options{
		Scheme:  scheme.Scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
		Cache:   cacheOptions,
		Client:  client.Options{Cache: &client.CacheOptions{DisableFor: UncachedObjects()}},
	})
	if err != nil {
		panic(err)
	}
	testCacheClient = mgr.GetClient()
	if err := index.Register(testCtx, mgr); err != nil {
		panic(err)
	}
	if err := setupReconcilers(mgr); err != nil {
		panic(err)
	}
	mgrDone := make(chan error, 1)
	go func() { mgrDone <- mgr.Start(testCtx) }()

	// Race the cache sync against the manager exiting early: if mgr.Start fails
	// before it ever starts the cache, WaitForCacheSync would otherwise block
	// forever since nothing else cancels testCtx on that path.
	cacheSynced := make(chan bool, 1)
	go func() { cacheSynced <- mgr.GetCache().WaitForCacheSync(testCtx) }()
	select {
	case err := <-mgrDone:
		if err != nil {
			panic(err)
		}
		panic("manager exited before cache sync")
	case ok := <-cacheSynced:
		if !ok {
			panic("cache sync failed")
		}
	}
	code := m.Run()
	testCancel()
	// Let the manager and its reconcilers drain before the API server goes away.
	if err := <-mgrDone; err != nil {
		panic(err)
	}
	_ = testEnv.Stop()
	os.Exit(code)
}

// setupReconcilers is extended task by task as reconcilers appear.
func setupReconcilers(mgr ctrl.Manager) error {
	if err := (&TenantReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}).SetupWithManager(mgr); err != nil {
		return err
	}
	if err := (&AlertRuleGroupReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}).SetupWithManager(mgr); err != nil {
		return err
	}
	if err := (&ContactPointReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}).SetupWithManager(mgr); err != nil {
		return err
	}
	if err := (&NotificationPolicyReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}).SetupWithManager(mgr); err != nil {
		return err
	}
	return nil
}

// waitFor polls until cond returns true or 10s pass.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("condition not met within 10s")
}
