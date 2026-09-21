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
)

var (
	testCfg    *rest.Config
	testClient client.Client
	testCtx    context.Context
	testCancel context.CancelFunc
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
	testCtx, testCancel = context.WithCancel(context.Background())
	mgr, err := ctrl.NewManager(testCfg, ctrl.Options{
		Scheme:  scheme.Scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
	})
	if err != nil {
		panic(err)
	}
	testClient = mgr.GetClient()
	if err := setupReconcilers(mgr); err != nil {
		panic(err)
	}
	go func() {
		if err := mgr.Start(testCtx); err != nil {
			panic(err)
		}
	}()
	if !mgr.GetCache().WaitForCacheSync(testCtx) {
		panic("cache sync failed")
	}
	code := m.Run()
	testCancel()
	_ = testEnv.Stop()
	os.Exit(code)
}

// setupReconcilers is extended task by task as reconcilers appear.
func setupReconcilers(mgr ctrl.Manager) error { //nolint:revive // mgr will be used once reconcilers are registered here in later tasks
	return nil
}

// waitFor polls until cond returns true or 10s pass.
//
//nolint:unused // helper for controller tests added in later tasks
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
