# alerts-operator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A kubebuilder operator that compiles `Tenant`/`ContactPoint`/`NotificationPolicy`/`AlertRuleGroup` CRs into Mimir Alertmanager config and Mimir/Loki ruler rule groups, and keeps the backends in sync.

**Architecture:** Four CRDs in `observability.antnsn.dev/v1alpha1`. Child reconcilers (ContactPoint, NotificationPolicy, AlertRuleGroup) only validate and set `Accepted`. The cluster-scoped `Tenant` reconciler is the single writer: it lists accepted children, compiles one Alertmanager document and a set of rule namespaces via pure functions in `internal/compile`, diffs against the backend through `internal/backend` HTTP clients, and writes only what changed. Prune is scoped to `rulesNamespacePrefix`.

**Tech Stack:** Go 1.27, kubebuilder v4.16.0, controller-runtime v0.25.1, k8s.io/api v0.37.0, `github.com/prometheus/alertmanager/config` v0.34.1 (local AM config validation), `github.com/prometheus/prometheus/promql/parser` v0.314.0 (PromQL validation), `sigs.k8s.io/yaml` v1.6.0, envtest, Helm 3, GitHub Actions, ghcr.io.

**Spec:** `docs/superpowers/specs/2026-09-21-alerts-operator-design.md`

## Global Constraints

- Module path `github.com/antnsn/alerts-operator`; domain `antnsn.dev`; group `observability`; version `v1alpha1`.
- `Tenant` is cluster-scoped. `ContactPoint`, `NotificationPolicy`, `AlertRuleGroup` are namespaced.
- Only the Tenant reconciler may call a backend. Children never write to backends and carry no finalizers.
- Every backend request carries `X-Scope-OrgID: <spec.tenantId>`.
- Rule namespaces are `<rulesNamespacePrefix>/<k8s-namespace>/<name>`; default prefix `alerts-operator`. Path segments must be `url.PathEscape`d.
- Loki ruler: never call per-group `GET /loki/api/v1/rules/{ns}/{group}` (malformed 404 on Loki 3.6.7). Bulk `GET /loki/api/v1/rules` only.
- `DELETE /api/v1/alerts` only inside the Tenant finalizer.
- Secrets in CRs only via `secretKeyRef` into the CR's own namespace. No inline secrets.
- No admission webhooks. Validation = CEL markers + reconcile-time checks.
- No `mimirtool`/`lokitool`. Image is distroless static.
- Tests: stdlib `testing` + envtest. No testify/ginkgo. Golden files under `testdata/`.
- Commits: Conventional Commits, `--no-gpg-sign` if signing fails. Never `--no-verify`.
- Per project `CLAUDE.md`: every task runs in a subagent, and `/codex:review` runs after every task before the next one starts. Fix or explicitly dismiss every finding.
- Condition type/reason strings live in `api/v1alpha1/conditions.go` (Task 2) and are used verbatim everywhere.
- After each task: `/codex:review` (per CLAUDE.md), fix findings, then commit.

---

## File structure (final)

```
cmd/main.go                                  kubebuilder entrypoint (+ metrics registration)
api/v1alpha1/groupversion_info.go            scaffolded
api/v1alpha1/conditions.go                   condition type + reason constants
api/v1alpha1/common_types.go                 SecretKeyRef, NamespacedName
api/v1alpha1/tenant_types.go
api/v1alpha1/contactpoint_types.go
api/v1alpha1/notificationpolicy_types.go
api/v1alpha1/alertrulegroup_types.go
api/v1alpha1/zz_generated.deepcopy.go        generated
internal/backend/backend.go                  RuleGroup/Rule/AlertmanagerConfig types, interfaces, Options
internal/backend/httpx.go                    shared HTTP helper (tenant header, auth, YAML codec, error mapping)
internal/backend/mimir/client.go             RuleStore + AlertmanagerStore
internal/backend/loki/client.go              RuleStore
internal/backend/fake/server.go              in-memory Mimir+Loki HTTP server for tests
internal/compile/rules.go                    AlertRuleGroup → map[backendNamespace][]RuleGroup
internal/compile/alertmanager.go             Policy + ContactPoints + secrets + templates → AlertmanagerConfig
internal/compile/validate.go                 alertmanager/config.Load wrapper, promql check
internal/compile/testdata/*.golden.yaml
internal/index/index.go                      field indexers (tenantRef, secret refs)
internal/controller/conditions.go            status helpers
internal/controller/suite_test.go            envtest TestMain
internal/controller/contactpoint_controller.go
internal/controller/notificationpolicy_controller.go
internal/controller/alertrulegroup_controller.go
internal/controller/tenant_controller.go     Reconcile, watches, finalizer
internal/controller/tenant_alertmanager.go   syncAlertmanager
internal/controller/tenant_rules.go          syncRules (per backend)
internal/metrics/metrics.go
charts/alerts-operator/                      Helm chart
docs/argocd-health.md, docs/examples/, docs/migration.md
.github/workflows/ci.yml, release.yml
```

---

### Task 1: Scaffold repo, APIs, tooling, CI skeleton, GitHub remote

**Files:**
- Create: everything kubebuilder generates; `.beads/`; `.github/workflows/ci.yml`; `.golangci.yml`
- Modify: `Makefile` (add `helm-sync` later in Task 20 — not now), `README.md`

**Interfaces:**
- Produces: module `github.com/antnsn/alerts-operator`; packages `api/v1alpha1`, `internal/controller`; `make manifests generate test lint`; kinds `Tenant` (cluster-scoped), `ContactPoint`, `NotificationPolicy`, `AlertRuleGroup`.

- [ ] **Step 1: Install tools**

```bash
go install sigs.k8s.io/kubebuilder/v4@v4.16.0
go install sigs.k8s.io/controller-runtime/tools/setup-envtest@v0.25.1
kubebuilder version
```
Expected: prints `KubeBuilderVersion:"4.16.0"`.

- [ ] **Step 2: Init project and APIs**

```bash
cd /Users/marius/repo/antnsn/alerts-operator
mv docs .docs-tmp   # kubebuilder init refuses a dir that already holds non-hidden lowercase entries
kubebuilder init --domain antnsn.dev --repo github.com/antnsn/alerts-operator --project-name alerts-operator
kubebuilder create api --group observability --version v1alpha1 --kind Tenant --resource --controller --namespaced=false
kubebuilder create api --group observability --version v1alpha1 --kind ContactPoint --resource --controller
kubebuilder create api --group observability --version v1alpha1 --kind NotificationPolicy --resource --controller
kubebuilder create api --group observability --version v1alpha1 --kind AlertRuleGroup --resource --controller
make manifests generate
mv .docs-tmp docs
# kubebuilder scaffolds an older Go directive / controller-runtime; pin the stack from the plan header.
go mod edit -go=1.27
go get sigs.k8s.io/controller-runtime@v0.25.1
go mod tidy
grep -nE '^(ARG BASE_IMAGE|FROM)' Dockerfile
```
If the Dockerfile has `ARG BASE_IMAGE=golang:<old>`, change it to `ARG BASE_IMAGE=golang:1.27`; if the builder line is a literal `FROM golang:<old> AS builder`, change it to `golang:1.27`. The final stage must be `FROM gcr.io/distroless/static:nonroot` (kubebuilder default; leave it).
Expected: `config/crd/bases/observability.antnsn.dev_{tenants,contactpoints,notificationpolicies,alertrulegroups}.yaml` exist; `docs/superpowers/` is back in place; `go.mod` says `go 1.27` and `sigs.k8s.io/controller-runtime v0.25.1`; `go build ./...` passes.

- [ ] **Step 3: Replace Ginkgo test scaffolding with stdlib envtest suite**

Delete `internal/controller/*_controller_test.go` and `internal/controller/suite_test.go`, delete `test/` directory (kubebuilder e2e uses kind — we use the home cluster). Remove ginkgo/gomega from `go.mod`:

```bash
rm -f internal/controller/*_test.go
rm -rf test
go mod tidy
```

Create `internal/controller/suite_test.go`:

```go
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
	// testClient is an uncached, direct client (not mgr.GetClient()): it talks straight to
	// the API server, so a Create followed immediately by a Get is guaranteed to observe the
	// write. The manager's cache-backed client syncs asynchronously and would otherwise race
	// tests that create then immediately read back.
	testClient, err = client.New(testCfg, client.Options{Scheme: scheme.Scheme})
	if err != nil {
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
```

Also ensure the scaffolded `cmd/main.go` still compiles (it registers the four scaffolded reconcilers — keep them; they get real bodies in later tasks).

- [ ] **Step 4: Verify envtest runs**

Run: `make test`
Expected: PASS (no tests yet, envtest starts and stops cleanly; `setup-envtest` downloads binaries on first run).

- [ ] **Step 5: Lint config**

Create `.golangci.yml`:

```yaml
version: "2"
linters:
  default: standard
  enable: [errcheck, govet, staticcheck, unused, ineffassign, misspell, revive]
formatters:
  enable: [gofmt, goimports]
```

Run: `golangci-lint run ./...`
Expected: no findings (fix any scaffold nits).

- [ ] **Step 6: CI workflow**

Create `.github/workflows/ci.yml`:

```yaml
name: ci
on:
  pull_request:
  push:
    branches: [main]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with: { go-version-file: go.mod }
      - uses: golangci/golangci-lint-action@v8
        with: { version: v2.13.2 }
      - run: make test
      - uses: azure/setup-helm@v4
      - run: test -d charts/alerts-operator && helm lint charts/alerts-operator || echo "no chart yet"
```

- [ ] **Step 7: Beads, README, remote, first push**

```bash
bd init
cat > README.md <<'MD'
# alerts-operator

Kubernetes operator for Grafana Mimir / Loki alert rules and Mimir Alertmanager configuration.
CRDs: `Tenant`, `ContactPoint`, `NotificationPolicy`, `AlertRuleGroup` (`observability.antnsn.dev/v1alpha1`).
See `docs/superpowers/specs/2026-09-21-alerts-operator-design.md`.
MD
git add -A
git commit -m "chore: scaffold kubebuilder project with four APIs, envtest suite, CI"
gh repo create antnsn/alerts-operator --public --source=. --remote=origin --push
git status
```
Expected: `Your branch is up to date with 'origin/main'.`

---

### Task 2: Shared API types and condition constants

**Files:**
- Create: `api/v1alpha1/conditions.go`, `api/v1alpha1/common_types.go`
- Test: `api/v1alpha1/common_types_test.go`

**Interfaces:**
- Produces:
  - `type SecretKeyRef struct { Name string; Key string }`
  - `type NamespacedName struct { Namespace string; Name string }` with `func (n NamespacedName) String() string`
  - constants: `ConditionReady, ConditionAccepted, ConditionSynced, ConditionAlertmanagerSynced, ConditionMimirRulesSynced, ConditionLokiRulesSynced`; reasons `ReasonAccepted, ReasonSynced, ReasonPending, ReasonNotConfigured, ReasonTenantNotFound, ReasonBackendNotConfigured, ReasonSecretNotFound, ReasonContactPointNotFound, ReasonConflict, ReasonInvalidRule, ReasonNoNotificationPolicy, ReasonBackendUnavailable, ReasonRejected, ReasonInvalid, ReasonDeleting`

- [ ] **Step 1: Write failing test**

`api/v1alpha1/common_types_test.go`:

```go
package v1alpha1

import "testing"

func TestNamespacedNameString(t *testing.T) {
	got := NamespacedName{Namespace: "mimir", Name: "am-templates"}.String()
	if got != "mimir/am-templates" {
		t.Fatalf("got %q", got)
	}
}
```

- [ ] **Step 2: Run, expect compile failure**

Run: `go test ./api/... -run TestNamespacedNameString`
Expected: FAIL `undefined: NamespacedName`.

- [ ] **Step 3: Implement**

`api/v1alpha1/common_types.go`:

```go
package v1alpha1

// SecretKeyRef points at one key of a Secret in the same namespace as the referencing object.
type SecretKeyRef struct {
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
	// +kubebuilder:validation:MinLength=1
	Key string `json:"key"`
}

// NamespacedName references an object in an explicit namespace (used by the cluster-scoped Tenant).
type NamespacedName struct {
	// +kubebuilder:validation:MinLength=1
	Namespace string `json:"namespace"`
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
}

func (n NamespacedName) String() string { return n.Namespace + "/" + n.Name }
```

`api/v1alpha1/conditions.go`:

```go
package v1alpha1

// Condition types.
const (
	ConditionReady              = "Ready"
	ConditionAccepted           = "Accepted"
	ConditionSynced             = "Synced"
	ConditionAlertmanagerSynced = "AlertmanagerSynced"
	ConditionMimirRulesSynced   = "MimirRulesSynced"
	ConditionLokiRulesSynced    = "LokiRulesSynced"
)

// Condition reasons.
const (
	ReasonAccepted             = "Accepted"
	ReasonSynced               = "Synced"
	ReasonPending              = "Pending"
	ReasonNotConfigured        = "NotConfigured"
	ReasonTenantNotFound       = "TenantNotFound"
	ReasonBackendNotConfigured = "BackendNotConfigured"
	ReasonSecretNotFound       = "SecretNotFound"
	ReasonContactPointNotFound = "ContactPointNotFound"
	ReasonConflict             = "Conflict"
	ReasonInvalidRule          = "InvalidRule"
	ReasonNoNotificationPolicy = "NoNotificationPolicy"
	ReasonBackendUnavailable   = "BackendUnavailable"
	ReasonRejected             = "Rejected"
	ReasonInvalid              = "Invalid"
	ReasonDeleting             = "Deleting"
)
```

- [ ] **Step 4: Run tests**

Run: `go test ./api/...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add api/v1alpha1/common_types.go api/v1alpha1/common_types_test.go api/v1alpha1/conditions.go
git commit -m "feat(api): add shared refs and condition constants"
```

---

### Task 3: Tenant API type with CEL validation

**Files:**
- Modify: `api/v1alpha1/tenant_types.go`
- Test: `internal/controller/tenant_api_test.go` (envtest CEL tests)

**Interfaces:**
- Produces:
```go
type BackendSpec struct { Address string; Auth *BackendAuth }
type BackendAuth struct { BasicAuthSecretRef *NamespacedName }   // Secret keys: username, password
type AlertmanagerSpec struct { TemplatesRef *NamespacedName }
type TenantSpec struct { TenantID string; Mimir *BackendSpec; Loki *BackendSpec; Alertmanager *AlertmanagerSpec; RulesNamespacePrefix string; ResyncInterval *metav1.Duration }
type RuleGroupCounts struct { Mimir int32; Loki int32 }
type TenantStatus struct { ObservedGeneration int64; Conditions []metav1.Condition; AlertmanagerConfigHash string; RuleGroups RuleGroupCounts }
func (t *Tenant) Prefix() string        // spec.rulesNamespacePrefix or "alerts-operator"
func (t *Tenant) Resync() time.Duration // spec.resyncInterval or 5m
```

- [ ] **Step 1: Write failing envtest test**

`internal/controller/tenant_api_test.go`:

```go
package controller

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	observabilityv1alpha1 "github.com/antnsn/alerts-operator/api/v1alpha1"
)

func TestTenantCEL(t *testing.T) {
	cases := []struct {
		name    string
		spec    observabilityv1alpha1.TenantSpec
		wantErr bool
	}{
		{"mimir only", observabilityv1alpha1.TenantSpec{TenantID: "1", Mimir: &observabilityv1alpha1.BackendSpec{Address: "http://m"}}, false},
		{"loki only", observabilityv1alpha1.TenantSpec{TenantID: "1", Loki: &observabilityv1alpha1.BackendSpec{Address: "http://l"}}, false},
		{"no backend", observabilityv1alpha1.TenantSpec{TenantID: "1"}, true},
		{"alertmanager without mimir", observabilityv1alpha1.TenantSpec{TenantID: "1", Loki: &observabilityv1alpha1.BackendSpec{Address: "http://l"}, Alertmanager: &observabilityv1alpha1.AlertmanagerSpec{}}, true},
	}
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			obj := &observabilityv1alpha1.Tenant{ObjectMeta: metav1.ObjectMeta{Name: "cel-" + string(rune('a'+i))}, Spec: c.spec}
			err := testClient.Create(testCtx, obj)
			if (err != nil) != c.wantErr {
				t.Fatalf("wantErr=%v got %v", c.wantErr, err)
			}
			if err == nil {
				_ = testClient.Delete(testCtx, obj)
			}
		})
	}
}

func TestTenantDefaults(t *testing.T) {
	tn := &observabilityv1alpha1.Tenant{}
	if tn.Prefix() != "alerts-operator" {
		t.Fatalf("prefix %q", tn.Prefix())
	}
	if tn.Resync().Minutes() != 5 {
		t.Fatalf("resync %v", tn.Resync())
	}
}
```

- [ ] **Step 2: Run, expect failure**

Run: `make test`
Expected: FAIL (fields undefined).

- [ ] **Step 3: Implement types**

Replace `api/v1alpha1/tenant_types.go` body:

```go
package v1alpha1

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	DefaultRulesNamespacePrefix = "alerts-operator"
	DefaultResyncInterval       = 5 * time.Minute
)

// BackendSpec describes how to reach one Mimir or Loki instance.
type BackendSpec struct {
	// Address is the base URL of the backend gateway, e.g. http://mimir-distributed-nginx.mimir:80
	// +kubebuilder:validation:Pattern=`^https?://`
	Address string `json:"address"`
	// +optional
	Auth *BackendAuth `json:"auth,omitempty"`
}

type BackendAuth struct {
	// BasicAuthSecretRef points at a Secret with keys `username` and `password`.
	// +optional
	BasicAuthSecretRef *NamespacedName `json:"basicAuthSecretRef,omitempty"`
}

type AlertmanagerSpec struct {
	// TemplatesRef points at a ConfigMap whose entries become Alertmanager template_files.
	// +optional
	TemplatesRef *NamespacedName `json:"templatesRef,omitempty"`
}

// TenantSpec defines the desired state of Tenant.
// +kubebuilder:validation:XValidation:rule="has(self.mimir) || has(self.loki)",message="at least one of spec.mimir or spec.loki must be set"
// +kubebuilder:validation:XValidation:rule="!has(self.alertmanager) || has(self.mimir)",message="spec.alertmanager requires spec.mimir"
type TenantSpec struct {
	// TenantID is sent as X-Scope-OrgID on every backend request.
	// +kubebuilder:validation:MinLength=1
	TenantID string `json:"tenantId"`
	// +optional
	Mimir *BackendSpec `json:"mimir,omitempty"`
	// +optional
	Loki *BackendSpec `json:"loki,omitempty"`
	// +optional
	Alertmanager *AlertmanagerSpec `json:"alertmanager,omitempty"`
	// RulesNamespacePrefix scopes which backend rule namespaces this operator owns. Default "alerts-operator".
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9_.-]+$`
	// +optional
	RulesNamespacePrefix string `json:"rulesNamespacePrefix,omitempty"`
	// ResyncInterval is the drift-repair period. Default 5m.
	// +optional
	ResyncInterval *metav1.Duration `json:"resyncInterval,omitempty"`
}

type RuleGroupCounts struct {
	Mimir int32 `json:"mimir"`
	Loki  int32 `json:"loki"`
}

// TenantStatus defines the observed state of Tenant.
type TenantStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// +optional
	AlertmanagerConfigHash string `json:"alertmanagerConfigHash,omitempty"`
	// +optional
	RuleGroups RuleGroupCounts `json:"ruleGroups,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:printcolumn:name="Tenant",type=string,JSONPath=`.spec.tenantId`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type Tenant struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TenantSpec   `json:"spec,omitempty"`
	Status TenantStatus `json:"status,omitempty"`
}

func (t *Tenant) Prefix() string {
	if t.Spec.RulesNamespacePrefix == "" {
		return DefaultRulesNamespacePrefix
	}
	return t.Spec.RulesNamespacePrefix
}

func (t *Tenant) Resync() time.Duration {
	if t.Spec.ResyncInterval == nil || t.Spec.ResyncInterval.Duration <= 0 {
		return DefaultResyncInterval
	}
	return t.Spec.ResyncInterval.Duration
}

// +kubebuilder:object:root=true
type TenantList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Tenant `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Tenant{}, &TenantList{})
}
```

- [ ] **Step 4: Regenerate and test**

Run: `make manifests generate && make test`
Expected: PASS. Check `config/crd/bases/observability.antnsn.dev_tenants.yaml` contains `x-kubernetes-validations`.

- [ ] **Step 5: Commit**

```bash
git add api/v1alpha1 config/crd internal/controller/tenant_api_test.go
git commit -m "feat(api): Tenant spec/status with CEL validation and defaults"
```

---

### Task 4: AlertRuleGroup API type

**Files:**
- Modify: `api/v1alpha1/alertrulegroup_types.go`
- Test: `internal/controller/alertrulegroup_api_test.go`

**Interfaces:**
- Produces:
```go
type Backend string; const (BackendMimir Backend = "mimir"; BackendLoki Backend = "loki")
type Rule struct { Record, Alert, Expr, For, KeepFiringFor string; Labels, Annotations map[string]string }
type RuleGroup struct { Name, Interval string; Rules []Rule }
type AlertRuleGroupSpec struct { TenantRef string; Backend Backend; Groups []RuleGroup }
type AlertRuleGroupStatus struct { ObservedGeneration int64; Conditions []metav1.Condition; BackendNamespace string }
```

- [ ] **Step 1: Failing CEL test**

`internal/controller/alertrulegroup_api_test.go`:

```go
package controller

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	observabilityv1alpha1 "github.com/antnsn/alerts-operator/api/v1alpha1"
)

func TestAlertRuleGroupCEL(t *testing.T) {
	ok := observabilityv1alpha1.RuleGroup{Name: "g", Rules: []observabilityv1alpha1.Rule{{Alert: "A", Expr: "up == 0"}}}
	cases := []struct {
		name    string
		spec    observabilityv1alpha1.AlertRuleGroupSpec
		wantErr bool
	}{
		{"valid", observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: "t", Backend: "mimir", Groups: []observabilityv1alpha1.RuleGroup{ok}}, false},
		{"bad backend", observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: "t", Backend: "tempo", Groups: []observabilityv1alpha1.RuleGroup{ok}}, true},
		{"dup group", observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: "t", Backend: "loki", Groups: []observabilityv1alpha1.RuleGroup{ok, ok}}, true},
		{"rule without alert or record", observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: "t", Backend: "mimir", Groups: []observabilityv1alpha1.RuleGroup{{Name: "g", Rules: []observabilityv1alpha1.Rule{{Expr: "up"}}}}}, true},
		{"rule with both", observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: "t", Backend: "mimir", Groups: []observabilityv1alpha1.RuleGroup{{Name: "g", Rules: []observabilityv1alpha1.Rule{{Alert: "A", Record: "r", Expr: "up"}}}}}, true},
	}
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			obj := &observabilityv1alpha1.AlertRuleGroup{ObjectMeta: metav1.ObjectMeta{Name: "cel-" + string(rune('a'+i)), Namespace: "default"}, Spec: c.spec}
			err := testClient.Create(testCtx, obj)
			if (err != nil) != c.wantErr {
				t.Fatalf("wantErr=%v got %v", c.wantErr, err)
			}
			if err == nil {
				_ = testClient.Delete(testCtx, obj)
			}
		})
	}
}
```

- [ ] **Step 2: Run, expect failure**

Run: `make test`
Expected: FAIL (undefined fields).

- [ ] **Step 3: Implement**

`api/v1alpha1/alertrulegroup_types.go`:

```go
package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// +kubebuilder:validation:Enum=mimir;loki
type Backend string

const (
	BackendMimir Backend = "mimir"
	BackendLoki  Backend = "loki"
)

// Rule is one alerting or recording rule (PrometheusRule-compatible shape).
// +kubebuilder:validation:XValidation:rule="has(self.alert) != has(self.record)",message="exactly one of alert or record must be set"
type Rule struct {
	// +kubebuilder:validation:MinLength=1
	// +optional
	Record string `json:"record,omitempty"`
	// +kubebuilder:validation:MinLength=1
	// +optional
	Alert string `json:"alert,omitempty"`
	// Expr must be a YAML string. PrometheusRule allows a bare number (IntOrString); quote it here, e.g. expr: "1".
	// +kubebuilder:validation:MinLength=1
	Expr string `json:"expr"`
	// +optional
	For string `json:"for,omitempty"`
	// +optional
	KeepFiringFor string `json:"keep_firing_for,omitempty"`
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
}

type RuleGroup struct {
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
	// +optional
	Interval string `json:"interval,omitempty"`
	// +kubebuilder:validation:MinItems=1
	Rules []Rule `json:"rules"`
}

// AlertRuleGroupSpec defines the desired state of AlertRuleGroup.
// Group-name uniqueness is enforced by +listType=map on Groups (no CEL needed).
type AlertRuleGroupSpec struct {
	// TenantRef is the name of the cluster-scoped Tenant.
	// +kubebuilder:validation:MinLength=1
	TenantRef string `json:"tenantRef"`
	Backend   Backend `json:"backend"`
	// +kubebuilder:validation:MinItems=1
	// +listType=map
	// +listMapKey=name
	Groups []RuleGroup `json:"groups"`
}

type AlertRuleGroupStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// BackendNamespace is the rule namespace used in the backend, <prefix>/<namespace>/<name>.
	// +optional
	BackendNamespace string `json:"backendNamespace,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Tenant",type=string,JSONPath=`.spec.tenantRef`
// +kubebuilder:printcolumn:name="Backend",type=string,JSONPath=`.spec.backend`
// +kubebuilder:printcolumn:name="Accepted",type=string,JSONPath=`.status.conditions[?(@.type=="Accepted")].status`
// +kubebuilder:printcolumn:name="Synced",type=string,JSONPath=`.status.conditions[?(@.type=="Synced")].status`
type AlertRuleGroup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              AlertRuleGroupSpec   `json:"spec,omitempty"`
	Status            AlertRuleGroupStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type AlertRuleGroupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AlertRuleGroup `json:"items"`
}

func init() {
	SchemeBuilder.Register(&AlertRuleGroup{}, &AlertRuleGroupList{})
}
```

Note: Kubernetes CEL has no list `unique()`; duplicate group names are rejected by the API server through `+listType=map`/`+listMapKey=name` (the "dup group" case fails admission with a duplicate-key error).

- [ ] **Step 4: Regenerate and test**

Run: `make manifests generate && make test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add api/v1alpha1 config/crd internal/controller/alertrulegroup_api_test.go
git commit -m "feat(api): AlertRuleGroup with PrometheusRule-compatible groups and CEL checks"
```

---

### Task 5: ContactPoint API type

**Files:**
- Modify: `api/v1alpha1/contactpoint_types.go`
- Test: `internal/controller/contactpoint_api_test.go`

**Interfaces:**
- Produces (all receiver structs exported):
```go
type HTTPBasicAuth struct { UsernameSecretRef, PasswordSecretRef SecretKeyRef }
type HTTPConfig struct { BearerTokenSecretRef *SecretKeyRef; BasicAuth *HTTPBasicAuth }
type WebhookConfig struct { URL string; URLSecretRef *SecretKeyRef; HTTPConfig *HTTPConfig; SendResolved *bool; MaxAlerts *int32 }
type PushoverConfig struct { UserKeySecretRef, TokenSecretRef SecretKeyRef; Title, Message, URL, URLTitle, Priority, Sound string; SendResolved *bool }
type SlackConfig struct { APIURLSecretRef SecretKeyRef; Channel, Username, Title, Text, IconEmoji string; SendResolved *bool }
type DiscordConfig struct { WebhookURLSecretRef SecretKeyRef; Title, Message string; SendResolved *bool }
type TelegramConfig struct { BotTokenSecretRef SecretKeyRef; ChatID int64; ParseMode, Message string; SendResolved *bool }
type EmailConfig struct { To, From, Smarthost, Hello, AuthUsername string; AuthPasswordSecretRef *SecretKeyRef; RequireTLS *bool; SendResolved *bool }
type ContactPointSpec struct { TenantRef string; Webhook []WebhookConfig; Pushover []PushoverConfig; Slack []SlackConfig; Discord []DiscordConfig; Telegram []TelegramConfig; Email []EmailConfig }
type ContactPointStatus struct { ObservedGeneration int64; Conditions []metav1.Condition }
func (c *ContactPoint) SecretRefs() []SecretKeyRef   // every secretKeyRef in spec, in order
```

- [ ] **Step 1: Failing tests**

`internal/controller/contactpoint_api_test.go`:

```go
package controller

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	observabilityv1alpha1 "github.com/antnsn/alerts-operator/api/v1alpha1"
)

func TestContactPointCEL(t *testing.T) {
	cases := []struct {
		name    string
		spec    observabilityv1alpha1.ContactPointSpec
		wantErr bool
	}{
		{"webhook url", observabilityv1alpha1.ContactPointSpec{TenantRef: "t", Webhook: []observabilityv1alpha1.WebhookConfig{{URL: "http://x"}}}, false},
		{"no receivers", observabilityv1alpha1.ContactPointSpec{TenantRef: "t"}, true},
		{"webhook without url or ref", observabilityv1alpha1.ContactPointSpec{TenantRef: "t", Webhook: []observabilityv1alpha1.WebhookConfig{{}}}, true},
		{"webhook with both url and ref", observabilityv1alpha1.ContactPointSpec{TenantRef: "t", Webhook: []observabilityv1alpha1.WebhookConfig{{URL: "http://x", URLSecretRef: &observabilityv1alpha1.SecretKeyRef{Name: "s", Key: "k"}}}}, true},
	}
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			obj := &observabilityv1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "cel-" + string(rune('a'+i)), Namespace: "default"}, Spec: c.spec}
			err := testClient.Create(testCtx, obj)
			if (err != nil) != c.wantErr {
				t.Fatalf("wantErr=%v got %v", c.wantErr, err)
			}
			if err == nil {
				_ = testClient.Delete(testCtx, obj)
			}
		})
	}
}

func TestContactPointSecretRefs(t *testing.T) {
	cp := &observabilityv1alpha1.ContactPoint{Spec: observabilityv1alpha1.ContactPointSpec{
		Webhook:  []observabilityv1alpha1.WebhookConfig{{URLSecretRef: &observabilityv1alpha1.SecretKeyRef{Name: "keep", Key: "url"}, HTTPConfig: &observabilityv1alpha1.HTTPConfig{BearerTokenSecretRef: &observabilityv1alpha1.SecretKeyRef{Name: "keep", Key: "token"}}}},
		Pushover: []observabilityv1alpha1.PushoverConfig{{UserKeySecretRef: observabilityv1alpha1.SecretKeyRef{Name: "po", Key: "user"}, TokenSecretRef: observabilityv1alpha1.SecretKeyRef{Name: "po", Key: "token"}}},
	}}
	refs := cp.SecretRefs()
	if len(refs) != 4 || refs[0].Key != "url" || refs[3].Key != "token" {
		t.Fatalf("refs %+v", refs)
	}
}
```

- [ ] **Step 2: Run, expect failure**

Run: `make test`
Expected: FAIL.

- [ ] **Step 3: Implement**

`api/v1alpha1/contactpoint_types.go`:

```go
package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type HTTPBasicAuth struct {
	UsernameSecretRef SecretKeyRef `json:"usernameSecretRef"`
	PasswordSecretRef SecretKeyRef `json:"passwordSecretRef"`
}

type HTTPConfig struct {
	// +optional
	BearerTokenSecretRef *SecretKeyRef `json:"bearerTokenSecretRef,omitempty"`
	// +optional
	BasicAuth *HTTPBasicAuth `json:"basicAuth,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="has(self.url) != has(self.urlSecretRef)",message="exactly one of url or urlSecretRef is required"
type WebhookConfig struct {
	// +kubebuilder:validation:MinLength=1
	// +optional
	URL string `json:"url,omitempty"`
	// +optional
	URLSecretRef *SecretKeyRef `json:"urlSecretRef,omitempty"`
	// +optional
	HTTPConfig *HTTPConfig `json:"httpConfig,omitempty"`
	// +optional
	SendResolved *bool `json:"sendResolved,omitempty"`
	// +optional
	MaxAlerts *int32 `json:"maxAlerts,omitempty"`
}

type PushoverConfig struct {
	UserKeySecretRef SecretKeyRef `json:"userKeySecretRef"`
	TokenSecretRef   SecretKeyRef `json:"tokenSecretRef"`
	// +optional
	Title string `json:"title,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`
	// +optional
	URL string `json:"url,omitempty"`
	// +optional
	URLTitle string `json:"urlTitle,omitempty"`
	// +optional
	Priority string `json:"priority,omitempty"`
	// +optional
	Sound string `json:"sound,omitempty"`
	// +optional
	SendResolved *bool `json:"sendResolved,omitempty"`
}

type SlackConfig struct {
	APIURLSecretRef SecretKeyRef `json:"apiURLSecretRef"`
	// +optional
	Channel string `json:"channel,omitempty"`
	// +optional
	Username string `json:"username,omitempty"`
	// +optional
	Title string `json:"title,omitempty"`
	// +optional
	Text string `json:"text,omitempty"`
	// +optional
	IconEmoji string `json:"iconEmoji,omitempty"`
	// +optional
	SendResolved *bool `json:"sendResolved,omitempty"`
}

type DiscordConfig struct {
	WebhookURLSecretRef SecretKeyRef `json:"webhookURLSecretRef"`
	// +optional
	Title string `json:"title,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`
	// +optional
	SendResolved *bool `json:"sendResolved,omitempty"`
}

type TelegramConfig struct {
	BotTokenSecretRef SecretKeyRef `json:"botTokenSecretRef"`
	ChatID            int64        `json:"chatID"`
	// +kubebuilder:validation:Enum=MarkdownV2;Markdown;HTML;""
	// +optional
	ParseMode string `json:"parseMode,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`
	// +optional
	SendResolved *bool `json:"sendResolved,omitempty"`
}

type EmailConfig struct {
	// +kubebuilder:validation:MinLength=1
	To string `json:"to"`
	// +optional
	From string `json:"from,omitempty"`
	// +optional
	Smarthost string `json:"smarthost,omitempty"`
	// +optional
	Hello string `json:"hello,omitempty"`
	// +optional
	AuthUsername string `json:"authUsername,omitempty"`
	// +optional
	AuthPasswordSecretRef *SecretKeyRef `json:"authPasswordSecretRef,omitempty"`
	// +optional
	RequireTLS *bool `json:"requireTLS,omitempty"`
	// +optional
	SendResolved *bool `json:"sendResolved,omitempty"`
}

// ContactPointSpec mirrors Alertmanager receiver configuration. Secrets only via secretKeyRef.
// +kubebuilder:validation:XValidation:rule="(has(self.webhook) && self.webhook.size() > 0) || (has(self.pushover) && self.pushover.size() > 0) || (has(self.slack) && self.slack.size() > 0) || (has(self.discord) && self.discord.size() > 0) || (has(self.telegram) && self.telegram.size() > 0) || (has(self.email) && self.email.size() > 0)",message="at least one receiver configuration is required"
type ContactPointSpec struct {
	// +kubebuilder:validation:MinLength=1
	TenantRef string `json:"tenantRef"`
	// +optional
	Webhook []WebhookConfig `json:"webhook,omitempty"`
	// +optional
	Pushover []PushoverConfig `json:"pushover,omitempty"`
	// +optional
	Slack []SlackConfig `json:"slack,omitempty"`
	// +optional
	Discord []DiscordConfig `json:"discord,omitempty"`
	// +optional
	Telegram []TelegramConfig `json:"telegram,omitempty"`
	// +optional
	Email []EmailConfig `json:"email,omitempty"`
}

type ContactPointStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Tenant",type=string,JSONPath=`.spec.tenantRef`
// +kubebuilder:printcolumn:name="Accepted",type=string,JSONPath=`.status.conditions[?(@.type=="Accepted")].status`
// +kubebuilder:printcolumn:name="Synced",type=string,JSONPath=`.status.conditions[?(@.type=="Synced")].status`
type ContactPoint struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              ContactPointSpec   `json:"spec,omitempty"`
	Status            ContactPointStatus `json:"status,omitempty"`
}

// SecretRefs returns every secretKeyRef used by this ContactPoint, in declaration order.
func (c *ContactPoint) SecretRefs() []SecretKeyRef {
	var out []SecretKeyRef
	add := func(r *SecretKeyRef) {
		if r != nil {
			out = append(out, *r)
		}
	}
	for i := range c.Spec.Webhook {
		w := &c.Spec.Webhook[i]
		add(w.URLSecretRef)
		if w.HTTPConfig != nil {
			add(w.HTTPConfig.BearerTokenSecretRef)
			if w.HTTPConfig.BasicAuth != nil {
				add(&w.HTTPConfig.BasicAuth.UsernameSecretRef)
				add(&w.HTTPConfig.BasicAuth.PasswordSecretRef)
			}
		}
	}
	for i := range c.Spec.Pushover {
		add(&c.Spec.Pushover[i].UserKeySecretRef)
		add(&c.Spec.Pushover[i].TokenSecretRef)
	}
	for i := range c.Spec.Slack {
		add(&c.Spec.Slack[i].APIURLSecretRef)
	}
	for i := range c.Spec.Discord {
		add(&c.Spec.Discord[i].WebhookURLSecretRef)
	}
	for i := range c.Spec.Telegram {
		add(&c.Spec.Telegram[i].BotTokenSecretRef)
	}
	for i := range c.Spec.Email {
		add(c.Spec.Email[i].AuthPasswordSecretRef)
	}
	return out
}

// +kubebuilder:object:root=true
type ContactPointList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ContactPoint `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ContactPoint{}, &ContactPointList{})
}
```

- [ ] **Step 4: Regenerate and test**

Run: `make manifests generate && make test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add api/v1alpha1 config/crd internal/controller/contactpoint_api_test.go
git commit -m "feat(api): ContactPoint with Alertmanager-native receivers and secretKeyRefs"
```

---

### Task 6: NotificationPolicy API type

**Files:**
- Modify: `api/v1alpha1/notificationpolicy_types.go`
- Test: `internal/controller/notificationpolicy_api_test.go`

**Interfaces:**
- Produces:
```go
type Route struct { Receiver string; GroupBy []string; GroupWait, GroupInterval, RepeatInterval string; Matchers []string; Continue bool; Routes []apiextensionsv1.JSON }
type InhibitRule struct { SourceMatchers, TargetMatchers, Equal []string }
type NotificationPolicySpec struct { TenantRef string; Route Route; InhibitRules []InhibitRule }
type NotificationPolicyStatus struct { ObservedGeneration int64; Conditions []metav1.Condition }
func (r *Route) ChildRoutes() ([]Route, error)  // decodes Routes; wraps the first decode error as "routes[i]: <err>"
func (r *Route) Receivers() ([]string, error)   // unique receiver names in the tree, depth-first; stops and returns the first decode error
```

Note: `Route.Routes` is recursive in shape, but a recursive Go type cannot get an OpenAPI schema from controller-gen. Instead of `[]Route`, `Routes` is `[]apiextensionsv1.JSON` — raw JSON blobs with the same shape as `Route` — carrying `// +kubebuilder:validation:Schemaless` and `// +kubebuilder:pruning:PreserveUnknownFields`. Child routes are decoded with `ChildRoutes()` and the depth is validated at reconcile time (Task 17).

- [ ] **Step 1: Failing tests**

`internal/controller/notificationpolicy_api_test.go`:

```go
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
```
- [ ] **Step 2: Run, expect failure**

Run: `make test`
Expected: FAIL.

- [ ] **Step 3: Implement**

`api/v1alpha1/notificationpolicy_types.go`:

```go
package v1alpha1

import (
	"encoding/json"
	"fmt"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Route is an Alertmanager routing tree node. Receiver names refer to ContactPoints in the policy's namespace.
type Route struct {
	// +kubebuilder:validation:MinLength=1
	Receiver string `json:"receiver"`
	// +optional
	GroupBy []string `json:"groupBy,omitempty"`
	// +optional
	GroupWait string `json:"groupWait,omitempty"`
	// +optional
	GroupInterval string `json:"groupInterval,omitempty"`
	// +optional
	RepeatInterval string `json:"repeatInterval,omitempty"`
	// Matchers use Alertmanager matcher syntax, e.g. `severity="critical"`.
	// +optional
	Matchers []string `json:"matchers,omitempty"`
	// +optional
	Continue bool `json:"continue,omitempty"`
	// Routes are child routes with the same shape as Route; stored as raw JSON because the type is
	// recursive. Decoded and validated at reconcile time.
	// +kubebuilder:validation:Schemaless
	// +kubebuilder:pruning:PreserveUnknownFields
	// +optional
	Routes []apiextensionsv1.JSON `json:"routes,omitempty"`
}

// ChildRoutes decodes Routes into typed Route values, in order. It returns the first decode
// error, wrapped with the index of the offending entry.
func (r *Route) ChildRoutes() ([]Route, error) {
	out := make([]Route, len(r.Routes))
	for i, raw := range r.Routes {
		if err := json.Unmarshal(raw.Raw, &out[i]); err != nil {
			return nil, fmt.Errorf("routes[%d]: %w", i, err)
		}
	}
	return out, nil
}

// Receivers returns the unique receiver names in the tree, depth-first, in first-seen order.
// It stops and returns the first decode error encountered while walking child routes.
func (r *Route) Receivers() ([]string, error) {
	seen := map[string]bool{}
	var out []string
	var walk func(*Route) error
	walk = func(n *Route) error {
		if !seen[n.Receiver] {
			seen[n.Receiver] = true
			out = append(out, n.Receiver)
		}
		children, err := n.ChildRoutes()
		if err != nil {
			return err
		}
		for i := range children {
			if err := walk(&children[i]); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk(r); err != nil {
		return nil, err
	}
	return out, nil
}

type InhibitRule struct {
	// +optional
	SourceMatchers []string `json:"sourceMatchers,omitempty"`
	// +optional
	TargetMatchers []string `json:"targetMatchers,omitempty"`
	// +optional
	Equal []string `json:"equal,omitempty"`
}

type NotificationPolicySpec struct {
	// +kubebuilder:validation:MinLength=1
	TenantRef string `json:"tenantRef"`
	Route     Route  `json:"route"`
	// +optional
	InhibitRules []InhibitRule `json:"inhibitRules,omitempty"`
}

type NotificationPolicyStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Tenant",type=string,JSONPath=`.spec.tenantRef`
// +kubebuilder:printcolumn:name="Accepted",type=string,JSONPath=`.status.conditions[?(@.type=="Accepted")].status`
// +kubebuilder:printcolumn:name="Synced",type=string,JSONPath=`.status.conditions[?(@.type=="Synced")].status`
type NotificationPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              NotificationPolicySpec   `json:"spec,omitempty"`
	Status            NotificationPolicyStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type NotificationPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NotificationPolicy `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NotificationPolicy{}, &NotificationPolicyList{})
}
```

Run `go get k8s.io/apiextensions-apiserver@v0.37.0` (same minor as `k8s.io/api`) to pull in the `apiextensionsv1.JSON` type.

- [ ] **Step 4: Regenerate and test**

Run: `make manifests generate && make test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add api/v1alpha1 config/crd internal/controller/notificationpolicy_api_test.go
git commit -m "feat(api): NotificationPolicy with recursive route tree and inhibit rules"
```

---

### Task 7: Backend types, interfaces, shared HTTP helper

**Files:**
- Create: `internal/backend/backend.go`, `internal/backend/httpx.go`
- Test: `internal/backend/httpx_test.go`

**Interfaces:**
- Produces:
```go
package backend
type Rule struct { Record, Alert, Expr, For, KeepFiringFor string; Labels, Annotations map[string]string } // json tags: record, alert, expr, for, keep_firing_for, labels, annotations
type RuleGroup struct { Name string `json:"name"`; Interval string `json:"interval,omitempty"`; Rules []Rule `json:"rules"` }
type AlertmanagerConfig struct { Config string `json:"alertmanager_config"`; TemplateFiles map[string]string `json:"template_files,omitempty"` }
type RuleStore interface { List(ctx) (map[string][]RuleGroup, error); SetGroup(ctx, ns string, g RuleGroup) error; DeleteGroup(ctx, ns, group string) error; DeleteNamespace(ctx, ns string) error }
type AlertmanagerStore interface { Get(ctx) (*AlertmanagerConfig, error); Set(ctx, *AlertmanagerConfig) error; Delete(ctx) error }
type BasicAuth struct { Username, Password string }
type Options struct { Address, TenantID string; BasicAuth *BasicAuth; Timeout time.Duration; HTTPClient *http.Client }
type StatusError struct { Status int; Body string }; func (e *StatusError) Error() string
func IsUnavailable(err error) bool   // network error or 5xx
func IsRejected(err error) bool      // 4xx other than 404
func IsNotFound(err error) bool      // 404
type HTTP struct{...}; func NewHTTP(o Options) *HTTP
func (h *HTTP) Do(ctx, method, path string, body []byte, contentType string) (status int, respBody []byte, err error) // returns *StatusError on >=400
func (h *HTTP) GetYAML(ctx, path string, out any) (found bool, err error)  // 404 → false,nil
func (h *HTTP) PostYAML(ctx, path string, in any) error
func (h *HTTP) Delete(ctx, path string) error  // 404 tolerated
func EscapePath(segments ...string) string     // url.PathEscape each, join with "/"
```

- [ ] **Step 1: Failing tests**

`internal/backend/httpx_test.go`:

```go
package backend

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPTenantHeaderAndAuth(t *testing.T) {
	var gotTenant, gotAuth, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotTenant = r.Header.Get("X-Scope-OrgID")
		u, p, _ := r.BasicAuth()
		gotAuth = u + ":" + p
		gotPath = r.URL.EscapedPath()
		w.WriteHeader(202)
	}))
	defer srv.Close()
	h := NewHTTP(Options{Address: srv.URL, TenantID: "1", BasicAuth: &BasicAuth{Username: "u", Password: "p"}})
	if err := h.PostYAML(context.Background(), "/x/"+EscapePath("alerts-operator/ns/name"), map[string]string{"a": "b"}); err != nil {
		t.Fatal(err)
	}
	if gotTenant != "1" || gotAuth != "u:p" || gotPath != "/x/alerts-operator%2Fns%2Fname" {
		t.Fatalf("tenant=%q auth=%q path=%q", gotTenant, gotAuth, gotPath)
	}
}

func TestHTTPErrorClassification(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/500":
			w.WriteHeader(500)
		case "/400":
			http.Error(w, "bad rule", 400)
		case "/404":
			w.WriteHeader(404)
		}
	}))
	defer srv.Close()
	h := NewHTTP(Options{Address: srv.URL, TenantID: "1"})
	ctx := context.Background()
	if _, _, err := h.Do(ctx, "GET", "/500", nil, ""); !IsUnavailable(err) {
		t.Fatalf("500 should be unavailable: %v", err)
	}
	if _, _, err := h.Do(ctx, "GET", "/400", nil, ""); !IsRejected(err) || err.Error() != "backend returned 400: bad rule" {
		t.Fatalf("400: %v", err)
	}
	var out map[string]any
	if found, err := h.GetYAML(ctx, "/404", &out); found || err != nil {
		t.Fatalf("404 → found=%v err=%v", found, err)
	}
	if err := h.Delete(ctx, "/404"); err != nil {
		t.Fatalf("delete 404 should be tolerated: %v", err)
	}
	srv.Close()
	if _, _, err := h.Do(ctx, "GET", "/500", nil, ""); !IsUnavailable(err) {
		t.Fatalf("connection refused should be unavailable: %v", err)
	}
}
```

- [ ] **Step 2: Run, expect failure**

Run: `go test ./internal/backend/`
Expected: FAIL (package missing).

- [ ] **Step 3: Implement**

`internal/backend/backend.go`:

```go
// Package backend defines the Mimir/Loki API shapes and client interfaces used by the operator.
package backend

import (
	"context"
	"net/http"
	"time"
)

// Rule is one ruler rule in backend YAML shape.
type Rule struct {
	Record        string            `json:"record,omitempty"`
	Alert         string            `json:"alert,omitempty"`
	Expr          string            `json:"expr"`
	For           string            `json:"for,omitempty"`
	KeepFiringFor string            `json:"keep_firing_for,omitempty"`
	Labels        map[string]string `json:"labels,omitempty"`
	Annotations   map[string]string `json:"annotations,omitempty"`
}

// RuleGroup is one ruler group in backend YAML shape.
type RuleGroup struct {
	Name     string `json:"name"`
	Interval string `json:"interval,omitempty"`
	Rules    []Rule `json:"rules"`
}

// AlertmanagerConfig is the Mimir per-tenant Alertmanager document (POST/GET /api/v1/alerts).
type AlertmanagerConfig struct {
	Config        string            `json:"alertmanager_config"`
	TemplateFiles map[string]string `json:"template_files,omitempty"`
}

// RuleStore manages ruler rule groups for one tenant.
type RuleStore interface {
	// List returns namespace → groups. Empty map when the tenant has no rules.
	List(ctx context.Context) (map[string][]RuleGroup, error)
	SetGroup(ctx context.Context, namespace string, g RuleGroup) error
	DeleteGroup(ctx context.Context, namespace, group string) error
	DeleteNamespace(ctx context.Context, namespace string) error
}

// AlertmanagerStore manages the Alertmanager config for one tenant.
type AlertmanagerStore interface {
	// Get returns nil, nil when no config is stored.
	Get(ctx context.Context) (*AlertmanagerConfig, error)
	Set(ctx context.Context, cfg *AlertmanagerConfig) error
	Delete(ctx context.Context) error
}

type BasicAuth struct {
	Username string
	Password string
}

// Options configure a backend client.
type Options struct {
	Address    string
	TenantID   string
	BasicAuth  *BasicAuth
	Timeout    time.Duration // default 30s
	HTTPClient *http.Client  // default http.DefaultClient with Timeout
}
```

`internal/backend/httpx.go`:

```go
package backend

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"sigs.k8s.io/yaml"
)

// StatusError is returned for HTTP responses with status >= 400.
type StatusError struct {
	Status int
	Body   string
}

func (e *StatusError) Error() string {
	b := strings.TrimSpace(e.Body)
	if len(b) > 512 {
		b = b[:512] + "…"
	}
	if b == "" {
		return fmt.Sprintf("backend returned %d", e.Status)
	}
	return fmt.Sprintf("backend returned %d: %s", e.Status, b)
}

// IsUnavailable reports transport errors and 5xx responses.
func IsUnavailable(err error) bool {
	if err == nil {
		return false
	}
	var se *StatusError
	if errors.As(err, &se) {
		return se.Status >= 500
	}
	return true
}

// IsRejected reports 4xx responses other than 404.
func IsRejected(err error) bool {
	var se *StatusError
	return errors.As(err, &se) && se.Status >= 400 && se.Status < 500 && se.Status != 404
}

func IsNotFound(err error) bool {
	var se *StatusError
	return errors.As(err, &se) && se.Status == 404
}

// EscapePath escapes each segment (including "/") and joins with "/".
func EscapePath(segments ...string) string {
	parts := make([]string, len(segments))
	for i, s := range segments {
		parts[i] = url.PathEscape(s)
	}
	return strings.Join(parts, "/")
}

// HTTP is the shared transport for Mimir and Loki clients.
type HTTP struct {
	base   string
	tenant string
	auth   *BasicAuth
	client *http.Client
}

func NewHTTP(o Options) *HTTP {
	timeout := o.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	c := o.HTTPClient
	if c == nil {
		c = &http.Client{Timeout: timeout}
	}
	return &HTTP{base: strings.TrimRight(o.Address, "/"), tenant: o.TenantID, auth: o.BasicAuth, client: c}
}

// Do performs one request. Status >= 400 yields *StatusError.
func (h *HTTP) Do(ctx context.Context, method, path string, body []byte, contentType string) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, h.base+path, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("X-Scope-OrgID", h.tenant)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if h.auth != nil {
		req.SetBasicAuth(h.auth.Username, h.auth.Password)
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return resp.StatusCode, nil, err
	}
	if resp.StatusCode >= 400 {
		return resp.StatusCode, respBody, &StatusError{Status: resp.StatusCode, Body: string(respBody)}
	}
	return resp.StatusCode, respBody, nil
}

// GetYAML decodes a YAML response into out. A 404 returns found=false, err=nil.
func (h *HTTP) GetYAML(ctx context.Context, path string, out any) (bool, error) {
	_, body, err := h.Do(ctx, http.MethodGet, path, nil, "")
	if IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return true, nil
	}
	if err := yaml.Unmarshal(body, out); err != nil {
		return true, fmt.Errorf("decode %s: %w", path, err)
	}
	return true, nil
}

// PostYAML encodes in as YAML and POSTs it.
func (h *HTTP) PostYAML(ctx context.Context, path string, in any) error {
	body, err := yaml.Marshal(in)
	if err != nil {
		return err
	}
	_, _, err = h.Do(ctx, http.MethodPost, path, body, "application/yaml")
	return err
}

// Delete issues DELETE; 404 is treated as success.
func (h *HTTP) Delete(ctx context.Context, path string) error {
	_, _, err := h.Do(ctx, http.MethodDelete, path, nil, "")
	if IsNotFound(err) {
		return nil
	}
	return err
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/backend/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/backend
git commit -m "feat(backend): types, store interfaces and shared HTTP transport"
```

---

### Task 8: Fake Mimir/Loki server

**Files:**
- Create: `internal/backend/fake/server.go`
- Test: `internal/backend/fake/server_test.go`

**Interfaces:**
- Produces:
```go
package fake
type Server struct { *httptest.Server; ... }
func New() *Server
func (s *Server) Rules(tenant string) map[string][]backend.RuleGroup           // copy of the Mimir ruler state
func (s *Server) LokiRules(tenant string) map[string][]backend.RuleGroup       // copy of the Loki ruler state
func (s *Server) Alertmanager(tenant string) *backend.AlertmanagerConfig      // copy or nil
func (s *Server) SetRules(tenant, ns string, groups []backend.RuleGroup)      // seed Mimir
func (s *Server) SetLokiRules(tenant, ns string, groups []backend.RuleGroup)  // seed Loki
func (s *Server) SetAlertmanager(tenant string, cfg *backend.AlertmanagerConfig)
func (s *Server) Fail(status int)     // every request returns status until Fail(0)
func (s *Server) RejectPost(msg string) // POSTs return 400 msg until RejectPost("")
func (s *Server) Requests() []string   // "METHOD path" log
func (s *Server) ResetRequests()
```
Paths served: Mimir `/prometheus/config/v1/rules[/{ns}[/{group}]]`, `/api/v1/alerts`; Loki `/loki/api/v1/rules[/{ns}[/{group}]]`. Mimir and Loki rules live in **separate** maps so one server can stand in for both backends of a Tenant without the two syncs pruning each other. Loki per-group GET deliberately returns 404 with a non-JSON body to mirror Loki 3.6.7. State keyed by `X-Scope-OrgID`; missing header → 401.

- [ ] **Step 1: Failing test**

`internal/backend/fake/server_test.go`:

```go
package fake

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/antnsn/alerts-operator/internal/backend"
)

func do(t *testing.T, method, url, body string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	req.Header.Set("X-Scope-OrgID", "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestFakeMimirRulesRoundTrip(t *testing.T) {
	s := New()
	defer s.Close()
	code, _ := do(t, "POST", s.URL+"/prometheus/config/v1/rules/p%2Fns%2Fa", "name: g1\nrules:\n- alert: A\n  expr: up == 0\n")
	if code != 202 {
		t.Fatalf("post %d", code)
	}
	code, body := do(t, "GET", s.URL+"/prometheus/config/v1/rules", "")
	if code != 200 || !strings.Contains(body, "p/ns/a:") || !strings.Contains(body, "name: g1") {
		t.Fatalf("get %d %q", code, body)
	}
	if code, _ = do(t, "DELETE", s.URL+"/prometheus/config/v1/rules/p%2Fns%2Fa/g1", ""); code != 202 {
		t.Fatalf("delete group %d", code)
	}
	if code, _ = do(t, "GET", s.URL+"/prometheus/config/v1/rules", ""); code != 404 {
		t.Fatalf("empty list should be 404, got %d", code)
	}
	if len(s.Rules("1")) != 0 {
		t.Fatalf("state not empty")
	}
}

func TestFakeLokiPerGroupGetIsBroken(t *testing.T) {
	s := New()
	defer s.Close()
	s.SetLokiRules("1", "p/ns/a", []backend.RuleGroup{{Name: "g", Rules: []backend.Rule{{Alert: "A", Expr: `{job="x"} |= "err"`}}}})
	if len(s.Rules("1")) != 0 {
		t.Fatal("loki seed must not appear in the mimir map")
	}
	code, body := do(t, "GET", s.URL+"/loki/api/v1/rules/p%2Fns%2Fa/g", "")
	if code != 404 || body == "" {
		t.Fatalf("expected malformed 404, got %d %q", code, body)
	}
	if code, _ = do(t, "GET", s.URL+"/loki/api/v1/rules", ""); code != 200 {
		t.Fatalf("bulk get %d", code)
	}
}

func TestFakeAlertmanagerAndFaults(t *testing.T) {
	s := New()
	defer s.Close()
	if code, _ := do(t, "GET", s.URL+"/api/v1/alerts", ""); code != 404 {
		t.Fatalf("no config should be 404, got %d", code)
	}
	if code, _ := do(t, "POST", s.URL+"/api/v1/alerts", "alertmanager_config: |\n  route:\n    receiver: x\n  receivers:\n  - name: x\n"); code != 201 {
		t.Fatalf("post %d", code)
	}
	if s.Alertmanager("1") == nil || !strings.Contains(s.Alertmanager("1").Config, "receiver: x") {
		t.Fatalf("not stored")
	}
	s.Fail(503)
	if code, _ := do(t, "GET", s.URL+"/api/v1/alerts", ""); code != 503 {
		t.Fatalf("fail %d", code)
	}
	s.Fail(0)
	s.RejectPost("invalid config")
	if code, body := do(t, "POST", s.URL+"/api/v1/alerts", "x: y"); code != 400 || !strings.Contains(body, "invalid config") {
		t.Fatalf("reject %d %q", code, body)
	}
	s.RejectPost("")
	if code, _ := do(t, "DELETE", s.URL+"/api/v1/alerts", ""); code != 200 || s.Alertmanager("1") != nil {
		t.Fatalf("delete")
	}
	if got := s.Requests(); len(got) == 0 || !strings.HasPrefix(got[0], "GET /api/v1/alerts") {
		t.Fatalf("requests %v", got)
	}
}
```

- [ ] **Step 2: Run, expect failure**

Run: `go test ./internal/backend/fake/`
Expected: FAIL.

- [ ] **Step 3: Implement**

`internal/backend/fake/server.go`:

```go
// Package fake is an in-memory Mimir + Loki ruler/Alertmanager API for tests.
package fake

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"

	"sigs.k8s.io/yaml"

	"github.com/antnsn/alerts-operator/internal/backend"
)

type tenantState struct {
	mimir map[string][]backend.RuleGroup // Mimir ruler namespaces
	loki  map[string][]backend.RuleGroup // Loki ruler namespaces
	am    *backend.AlertmanagerConfig
}

type Server struct {
	*httptest.Server
	mu         sync.Mutex
	tenants    map[string]*tenantState
	failStatus int
	rejectMsg  string
	requests   []string
}

func New() *Server {
	s := &Server{tenants: map[string]*tenantState{}}
	s.Server = httptest.NewServer(http.HandlerFunc(s.handle))
	return s
}

func (s *Server) tenant(id string) *tenantState {
	t, ok := s.tenants[id]
	if !ok {
		t = &tenantState{mimir: map[string][]backend.RuleGroup{}, loki: map[string][]backend.RuleGroup{}}
		s.tenants[id] = t
	}
	return t
}

func (s *Server) Rules(tenant string) map[string][]backend.RuleGroup {
	s.mu.Lock()
	defer s.mu.Unlock()
	return copyRules(s.tenant(tenant).mimir)
}

func (s *Server) LokiRules(tenant string) map[string][]backend.RuleGroup {
	s.mu.Lock()
	defer s.mu.Unlock()
	return copyRules(s.tenant(tenant).loki)
}

func copyRules(in map[string][]backend.RuleGroup) map[string][]backend.RuleGroup {
	out := map[string][]backend.RuleGroup{}
	for ns, gs := range in {
		out[ns] = append([]backend.RuleGroup(nil), gs...)
	}
	return out
}

func (s *Server) Alertmanager(tenant string) *backend.AlertmanagerConfig {
	s.mu.Lock()
	defer s.mu.Unlock()
	am := s.tenant(tenant).am
	if am == nil {
		return nil
	}
	c := *am
	return &c
}

func (s *Server) SetRules(tenant, ns string, groups []backend.RuleGroup) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tenant(tenant).mimir[ns] = append([]backend.RuleGroup(nil), groups...)
}

func (s *Server) SetLokiRules(tenant, ns string, groups []backend.RuleGroup) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tenant(tenant).loki[ns] = append([]backend.RuleGroup(nil), groups...)
}

func (s *Server) SetAlertmanager(tenant string, cfg *backend.AlertmanagerConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tenant(tenant).am = cfg
}

func (s *Server) Fail(status int)        { s.mu.Lock(); s.failStatus = status; s.mu.Unlock() }
func (s *Server) RejectPost(msg string)  { s.mu.Lock(); s.rejectMsg = msg; s.mu.Unlock() }
func (s *Server) ResetRequests()         { s.mu.Lock(); s.requests = nil; s.mu.Unlock() }
func (s *Server) Requests() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.requests...)
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = append(s.requests, r.Method+" "+r.URL.EscapedPath())
	if s.failStatus != 0 {
		http.Error(w, "injected failure", s.failStatus)
		return
	}
	if r.Method == http.MethodPost && s.rejectMsg != "" {
		http.Error(w, s.rejectMsg, http.StatusBadRequest)
		return
	}
	tenant := r.Header.Get("X-Scope-OrgID")
	if tenant == "" {
		http.Error(w, "no org id", http.StatusUnauthorized)
		return
	}
	st := s.tenant(tenant)
	path := r.URL.EscapedPath()

	switch {
	case path == "/api/v1/alerts":
		s.handleAM(w, r, st)
	case strings.HasPrefix(path, "/prometheus/config/v1/rules"):
		s.handleRules(w, r, st.mimir, strings.TrimPrefix(path, "/prometheus/config/v1/rules"), false)
	case strings.HasPrefix(path, "/loki/api/v1/rules"):
		s.handleRules(w, r, st.loki, strings.TrimPrefix(path, "/loki/api/v1/rules"), true)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) handleAM(w http.ResponseWriter, r *http.Request, st *tenantState) {
	switch r.Method {
	case http.MethodGet:
		if st.am == nil {
			http.Error(w, "alertmanager storage object not found", http.StatusNotFound)
			return
		}
		b, _ := yaml.Marshal(st.am)
		w.Header().Set("Content-Type", "application/yaml")
		_, _ = w.Write(b)
	case http.MethodPost:
		body, _ := io.ReadAll(r.Body)
		var cfg backend.AlertmanagerConfig
		if err := yaml.Unmarshal(body, &cfg); err != nil || cfg.Config == "" {
			http.Error(w, "error validating Alertmanager config: "+fmt.Sprint(err), http.StatusBadRequest)
			return
		}
		st.am = &cfg
		w.WriteHeader(http.StatusCreated)
	case http.MethodDelete:
		st.am = nil
		w.WriteHeader(http.StatusOK)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// segments splits "/ns/group" (escaped) into decoded parts.
func segments(rest string) []string {
	rest = strings.Trim(rest, "/")
	if rest == "" {
		return nil
	}
	parts := strings.Split(rest, "/")
	for i, p := range parts {
		if u, err := unescape(p); err == nil {
			parts[i] = u
		}
	}
	return parts
}

func unescape(s string) (string, error) {
	return url.PathUnescape(s)
}

func (s *Server) handleRules(w http.ResponseWriter, r *http.Request, rules map[string][]backend.RuleGroup, rest string, loki bool) {
	seg := segments(rest)
	switch {
	case r.Method == http.MethodGet && len(seg) == 0:
		if len(rules) == 0 {
			http.Error(w, "no rule groups found", http.StatusNotFound)
			return
		}
		b, _ := yaml.Marshal(rules)
		w.Header().Set("Content-Type", "application/yaml")
		_, _ = w.Write(b)
	case r.Method == http.MethodGet && len(seg) == 1:
		gs, ok := rules[seg[0]]
		if !ok {
			http.Error(w, "namespace not found", http.StatusNotFound)
			return
		}
		b, _ := yaml.Marshal(map[string][]backend.RuleGroup{seg[0]: gs})
		_, _ = w.Write(b)
	case r.Method == http.MethodGet && len(seg) == 2:
		if loki {
			// Mirrors Loki 3.6.7: per-group GET is broken and returns a malformed 404.
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte("<html>not found</html>"))
			return
		}
		for _, g := range rules[seg[0]] {
			if g.Name == seg[1] {
				b, _ := yaml.Marshal(g)
				_, _ = w.Write(b)
				return
			}
		}
		http.Error(w, "group not found", http.StatusNotFound)
	case r.Method == http.MethodPost && len(seg) == 1:
		body, _ := io.ReadAll(r.Body)
		var g backend.RuleGroup
		if err := yaml.Unmarshal(body, &g); err != nil || g.Name == "" {
			http.Error(w, "invalid rule group", http.StatusBadRequest)
			return
		}
		ns := seg[0]
		replaced := false
		for i := range rules[ns] {
			if rules[ns][i].Name == g.Name {
				rules[ns][i] = g
				replaced = true
			}
		}
		if !replaced {
			rules[ns] = append(rules[ns], g)
		}
		w.WriteHeader(http.StatusAccepted)
	case r.Method == http.MethodDelete && len(seg) == 1:
		delete(rules, seg[0])
		w.WriteHeader(http.StatusAccepted)
	case r.Method == http.MethodDelete && len(seg) == 2:
		ns := seg[0]
		var kept []backend.RuleGroup
		for _, g := range rules[ns] {
			if g.Name != seg[1] {
				kept = append(kept, g)
			}
		}
		if len(kept) == 0 {
			delete(rules, ns)
		} else {
			rules[ns] = kept
		}
		w.WriteHeader(http.StatusAccepted)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}
```
- [ ] **Step 4: Run tests**

Run: `go test ./internal/backend/... -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/backend/fake
git commit -m "test(backend): in-memory fake Mimir/Loki server with fault injection"
```

---

### Task 9: Mimir client

**Files:**
- Create: `internal/backend/mimir/client.go`
- Test: `internal/backend/mimir/client_test.go`

**Interfaces:**
- Produces: `package mimir; type Client struct{...}; func New(o backend.Options) *Client` implementing `backend.RuleStore` and `backend.AlertmanagerStore`. Compile-time assertions `var _ backend.RuleStore = (*Client)(nil)` etc.

- [ ] **Step 1: Failing test**

`internal/backend/mimir/client_test.go`:

```go
package mimir

import (
	"context"
	"testing"

	"github.com/antnsn/alerts-operator/internal/backend"
	"github.com/antnsn/alerts-operator/internal/backend/fake"
)

func TestClientRulesAndAlertmanager(t *testing.T) {
	s := fake.New()
	defer s.Close()
	c := New(backend.Options{Address: s.URL, TenantID: "1"})
	ctx := context.Background()

	got, err := c.List(ctx)
	if err != nil || len(got) != 0 {
		t.Fatalf("empty list: %v %v", got, err)
	}
	g := backend.RuleGroup{Name: "g", Interval: "1m", Rules: []backend.Rule{{Alert: "A", Expr: "up == 0", For: "5m", Labels: map[string]string{"severity": "critical"}}}}
	if err := c.SetGroup(ctx, "alerts-operator/ns/x", g); err != nil {
		t.Fatal(err)
	}
	got, err = c.List(ctx)
	if err != nil || len(got["alerts-operator/ns/x"]) != 1 || got["alerts-operator/ns/x"][0].Rules[0].Labels["severity"] != "critical" {
		t.Fatalf("list after set: %+v %v", got, err)
	}
	if err := c.DeleteGroup(ctx, "alerts-operator/ns/x", "g"); err != nil {
		t.Fatal(err)
	}
	if err := c.DeleteNamespace(ctx, "alerts-operator/ns/x"); err != nil {
		t.Fatal(err)
	}
	if reqs := s.Requests(); reqs[1] != "POST /prometheus/config/v1/rules/alerts-operator%2Fns%2Fx" {
		t.Fatalf("path escaping: %v", reqs)
	}

	am, err := c.Get(ctx)
	if err != nil || am != nil {
		t.Fatalf("no config: %v %v", am, err)
	}
	cfg := &backend.AlertmanagerConfig{Config: "route:\n  receiver: x\nreceivers:\n- name: x\n", TemplateFiles: map[string]string{"a.tmpl": "{{ define \"a\" }}x{{ end }}"}}
	if err := c.Set(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	am, err = c.Get(ctx)
	if err != nil || am == nil || am.Config != cfg.Config || am.TemplateFiles["a.tmpl"] != cfg.TemplateFiles["a.tmpl"] {
		t.Fatalf("round trip: %+v %v", am, err)
	}
	if err := c.Delete(ctx); err != nil {
		t.Fatal(err)
	}
	s.Fail(502)
	if _, err := c.List(ctx); !backend.IsUnavailable(err) {
		t.Fatalf("expected unavailable, got %v", err)
	}
}
```

- [ ] **Step 2: Run, expect failure**

Run: `go test ./internal/backend/mimir/`
Expected: FAIL.

- [ ] **Step 3: Implement**

`internal/backend/mimir/client.go`:

```go
// Package mimir talks to the Grafana Mimir ruler and Alertmanager config APIs.
package mimir

import (
	"context"

	"github.com/antnsn/alerts-operator/internal/backend"
)

const (
	rulesPath = "/prometheus/config/v1/rules"
	amPath    = "/api/v1/alerts"
)

type Client struct{ h *backend.HTTP }

var (
	_ backend.RuleStore         = (*Client)(nil)
	_ backend.AlertmanagerStore = (*Client)(nil)
)

func New(o backend.Options) *Client { return &Client{h: backend.NewHTTP(o)} }

func (c *Client) List(ctx context.Context) (map[string][]backend.RuleGroup, error) {
	out := map[string][]backend.RuleGroup{}
	if _, err := c.h.GetYAML(ctx, rulesPath, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Client) SetGroup(ctx context.Context, namespace string, g backend.RuleGroup) error {
	return c.h.PostYAML(ctx, rulesPath+"/"+backend.EscapePath(namespace), g)
}

func (c *Client) DeleteGroup(ctx context.Context, namespace, group string) error {
	return c.h.Delete(ctx, rulesPath+"/"+backend.EscapePath(namespace, group))
}

func (c *Client) DeleteNamespace(ctx context.Context, namespace string) error {
	return c.h.Delete(ctx, rulesPath+"/"+backend.EscapePath(namespace))
}

func (c *Client) Get(ctx context.Context) (*backend.AlertmanagerConfig, error) {
	var cfg backend.AlertmanagerConfig
	found, err := c.h.GetYAML(ctx, amPath, &cfg)
	if err != nil || !found {
		return nil, err
	}
	return &cfg, nil
}

func (c *Client) Set(ctx context.Context, cfg *backend.AlertmanagerConfig) error {
	return c.h.PostYAML(ctx, amPath, cfg)
}

func (c *Client) Delete(ctx context.Context) error { return c.h.Delete(ctx, amPath) }
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/backend/... -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/backend/mimir
git commit -m "feat(backend): Mimir ruler and Alertmanager config client"
```

---

### Task 10: Loki client

**Files:**
- Create: `internal/backend/loki/client.go`
- Test: `internal/backend/loki/client_test.go`

**Interfaces:**
- Produces: `package loki; type Client struct{...}; func New(o backend.Options) *Client` implementing `backend.RuleStore` only.

- [ ] **Step 1: Failing test**

`internal/backend/loki/client_test.go`:

```go
package loki

import (
	"context"
	"strings"
	"testing"

	"github.com/antnsn/alerts-operator/internal/backend"
	"github.com/antnsn/alerts-operator/internal/backend/fake"
)

func TestClientNeverUsesPerGroupGet(t *testing.T) {
	s := fake.New()
	defer s.Close()
	c := New(backend.Options{Address: s.URL, TenantID: "1"})
	ctx := context.Background()
	g := backend.RuleGroup{Name: "g", Rules: []backend.Rule{{Alert: "E", Expr: `sum(rate({job="x"} |= "error" [5m])) > 0`}}}
	if err := c.SetGroup(ctx, "alerts-operator/ns/l", g); err != nil {
		t.Fatal(err)
	}
	got, err := c.List(ctx)
	if err != nil || got["alerts-operator/ns/l"][0].Name != "g" {
		t.Fatalf("list: %+v %v", got, err)
	}
	if err := c.DeleteGroup(ctx, "alerts-operator/ns/l", "g"); err != nil {
		t.Fatal(err)
	}
	if err := c.DeleteNamespace(ctx, "alerts-operator/ns/l"); err != nil {
		t.Fatal(err)
	}
	for _, r := range s.Requests() {
		if strings.HasPrefix(r, "GET /loki/api/v1/rules/") {
			t.Fatalf("per-group/namespace GET is forbidden: %s", r)
		}
		if !strings.Contains(r, "/loki/api/v1/rules") {
			t.Fatalf("wrong base path: %s", r)
		}
	}
	got, err = c.List(ctx)
	if err != nil || len(got) != 0 {
		t.Fatalf("empty after delete: %+v %v", got, err)
	}
}
```

- [ ] **Step 2: Run, expect failure**

Run: `go test ./internal/backend/loki/`
Expected: FAIL.

- [ ] **Step 3: Implement**

`internal/backend/loki/client.go`:

```go
// Package loki talks to the Grafana Loki ruler API.
//
// Only the bulk GET /loki/api/v1/rules is used for reads: on Loki 3.6.7 the
// per-group GET returns a malformed 404.
package loki

import (
	"context"

	"github.com/antnsn/alerts-operator/internal/backend"
)

const rulesPath = "/loki/api/v1/rules"

type Client struct{ h *backend.HTTP }

var _ backend.RuleStore = (*Client)(nil)

func New(o backend.Options) *Client { return &Client{h: backend.NewHTTP(o)} }

func (c *Client) List(ctx context.Context) (map[string][]backend.RuleGroup, error) {
	out := map[string][]backend.RuleGroup{}
	if _, err := c.h.GetYAML(ctx, rulesPath, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Client) SetGroup(ctx context.Context, namespace string, g backend.RuleGroup) error {
	return c.h.PostYAML(ctx, rulesPath+"/"+backend.EscapePath(namespace), g)
}

func (c *Client) DeleteGroup(ctx context.Context, namespace, group string) error {
	return c.h.Delete(ctx, rulesPath+"/"+backend.EscapePath(namespace, group))
}

func (c *Client) DeleteNamespace(ctx context.Context, namespace string) error {
	return c.h.Delete(ctx, rulesPath+"/"+backend.EscapePath(namespace))
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/backend/... -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/backend/loki
git commit -m "feat(backend): Loki ruler client using bulk GET only"
```

---

### Task 11: compile.Rules

**Files:**
- Create: `internal/compile/rules.go`
- Test: `internal/compile/rules_test.go`, `internal/compile/testdata/rules_basic.golden.yaml`

**Interfaces:**
- Produces:
```go
package compile
func BackendNamespace(prefix, k8sNamespace, name string) string          // prefix + "/" + ns + "/" + name
func Rules(prefix string, groups []v1alpha1.AlertRuleGroup) map[string][]backend.RuleGroup
func RulesEqual(a, b []backend.RuleGroup) bool                           // order-insensitive by group name, deep-equal content
```

- [ ] **Step 1: Failing tests**

`internal/compile/rules_test.go`:

```go
package compile

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	"github.com/antnsn/alerts-operator/api/v1alpha1"
	"github.com/antnsn/alerts-operator/internal/backend"
)

var update = flag.Bool("update", false, "rewrite golden files")

func golden(t *testing.T, name string, got []byte) {
	t.Helper()
	p := filepath.Join("testdata", name)
	if *update {
		if err := os.WriteFile(p, got, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read golden (run with -update first): %v", err)
	}
	if string(want) != string(got) {
		t.Fatalf("golden mismatch %s\n--- want\n%s\n--- got\n%s", name, want, got)
	}
}

func TestRulesGolden(t *testing.T) {
	in := []v1alpha1.AlertRuleGroup{
		{ObjectMeta: metav1.ObjectMeta{Name: "homelab", Namespace: "monitoring"}, Spec: v1alpha1.AlertRuleGroupSpec{TenantRef: "t", Backend: "mimir", Groups: []v1alpha1.RuleGroup{
			{Name: "node.health", Interval: "1m", Rules: []v1alpha1.Rule{
				{Alert: "NodeDown", Expr: `up{job="node"} == 0`, For: "5m", Labels: map[string]string{"severity": "critical"}, Annotations: map[string]string{"summary": "{{ $labels.instance }} down"}},
				{Record: "job:up:ratio", Expr: "avg by (job) (up)"},
			}},
		}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "udm", Namespace: "loki"}, Spec: v1alpha1.AlertRuleGroupSpec{TenantRef: "t", Backend: "loki", Groups: []v1alpha1.RuleGroup{
			{Name: "udm", Rules: []v1alpha1.Rule{{Alert: "UDMErrors", Expr: `sum(rate({host="udm"} |= "error" [5m])) > 1`, KeepFiringFor: "10m"}}},
		}}},
	}
	got := Rules("alerts-operator", in)
	if len(got) != 2 {
		t.Fatalf("namespaces: %v", got)
	}
	if _, ok := got["alerts-operator/monitoring/homelab"]; !ok {
		t.Fatalf("missing namespace: %v", got)
	}
	b, _ := yaml.Marshal(got)
	golden(t, "rules_basic.golden.yaml", b)
}

func TestRulesEqualIgnoresOrder(t *testing.T) {
	a := []backend.RuleGroup{{Name: "a", Rules: []backend.Rule{{Alert: "x", Expr: "1"}}}, {Name: "b", Rules: []backend.Rule{{Alert: "y", Expr: "2"}}}}
	b := []backend.RuleGroup{{Name: "b", Rules: []backend.Rule{{Alert: "y", Expr: "2"}}}, {Name: "a", Rules: []backend.Rule{{Alert: "x", Expr: "1"}}}}
	if !RulesEqual(a, b) {
		t.Fatal("order should not matter")
	}
	b[0].Rules[0].Expr = "3"
	if RulesEqual(a, b) {
		t.Fatal("content change must be detected")
	}
	// Comparing must not mutate the inputs (normalize deep-copies).
	c := []backend.RuleGroup{{Name: "c", Rules: []backend.Rule{{Alert: "z", Expr: "1", Labels: map[string]string{}}}}}
	_ = RulesEqual(c, c)
	if c[0].Rules[0].Labels == nil {
		t.Fatal("RulesEqual mutated its input")
	}
}
```

- [ ] **Step 2: Run, expect failure**

Run: `go test ./internal/compile/`
Expected: FAIL.

- [ ] **Step 3: Implement**

`internal/compile/rules.go`:

```go
// Package compile turns CRs into backend documents. All functions are pure.
package compile

import (
	"reflect"
	"sort"

	"github.com/antnsn/alerts-operator/api/v1alpha1"
	"github.com/antnsn/alerts-operator/internal/backend"
)

// BackendNamespace is the ruler namespace owned by one AlertRuleGroup.
func BackendNamespace(prefix, k8sNamespace, name string) string {
	return prefix + "/" + k8sNamespace + "/" + name
}

// Rules maps AlertRuleGroups to backend namespaces. The caller filters by backend first.
func Rules(prefix string, groups []v1alpha1.AlertRuleGroup) map[string][]backend.RuleGroup {
	out := map[string][]backend.RuleGroup{}
	for _, arg := range groups {
		ns := BackendNamespace(prefix, arg.Namespace, arg.Name)
		var gs []backend.RuleGroup
		for _, g := range arg.Spec.Groups {
			bg := backend.RuleGroup{Name: g.Name, Interval: g.Interval}
			for _, r := range g.Rules {
				bg.Rules = append(bg.Rules, backend.Rule{
					Record: r.Record, Alert: r.Alert, Expr: r.Expr, For: r.For, KeepFiringFor: r.KeepFiringFor,
					Labels: copyMap(r.Labels), Annotations: copyMap(r.Annotations),
				})
			}
			gs = append(gs, bg)
		}
		out[ns] = gs
	}
	return out
}

// RulesEqual compares two group lists ignoring group order.
func RulesEqual(a, b []backend.RuleGroup) bool {
	if len(a) != len(b) {
		return false
	}
	return reflect.DeepEqual(normalize(a), normalize(b))
}

// normalize returns a deep copy sorted by group name with empty maps nil-ed; inputs are never mutated.
func normalize(in []backend.RuleGroup) []backend.RuleGroup {
	out := make([]backend.RuleGroup, len(in))
	for i, g := range in {
		g.Rules = make([]backend.Rule, len(in[i].Rules))
		for j, r := range in[i].Rules {
			if len(r.Labels) == 0 {
				r.Labels = nil
			}
			if len(r.Annotations) == 0 {
				r.Annotations = nil
			}
			g.Rules[j] = r
		}
		out[i] = g
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func copyMap(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
```

- [ ] **Step 4: Generate golden and run**

Run: `mkdir -p internal/compile/testdata && go test ./internal/compile/ -update && go test ./internal/compile/ -v`
Expected: PASS. Inspect `testdata/rules_basic.golden.yaml`: two top-level keys, `keep_firing_for: 10m` present, no empty `labels: {}`.

- [ ] **Step 5: Commit**

```bash
git add internal/compile
git commit -m "feat(compile): AlertRuleGroup to backend rule namespaces with order-insensitive diff"
```

---

### Task 12: compile.Alertmanager + local validation

**Files:**
- Create: `internal/compile/alertmanager.go`, `internal/compile/validate.go`
- Test: `internal/compile/alertmanager_test.go`, `internal/compile/testdata/am_full.golden.yaml`

**Interfaces:**
- Produces:
```go
type SecretResolver func(namespace, name, key string) (string, error)
type AttributedError struct { Kind, Namespace, Name string; Err error }  // Kind: "ContactPoint" | "NotificationPolicy"
func (e *AttributedError) Error() string; func (e *AttributedError) Unwrap() error
type AlertmanagerInput struct { Policy *v1alpha1.NotificationPolicy; ContactPoints []v1alpha1.ContactPoint; Secrets SecretResolver; Templates map[string]string }
func Alertmanager(in AlertmanagerInput) (*backend.AlertmanagerConfig, error)  // compiles + validates; error is *AttributedError when attributable
func ReceiverName(namespace, name string) string   // namespace + "/" + name
func ValidateAlertmanager(cfg *backend.AlertmanagerConfig) error   // alertmanager/config.Load on cfg.Config with templates stubbed
func ValidatePromQL(expr string) error
func HashAlertmanager(cfg *backend.AlertmanagerConfig) string      // "sha256:<hex>" over canonical YAML
```

Compiled YAML uses Alertmanager's native keys. Receivers are emitted for **every** ContactPoint (sorted by receiver name), whether or not the route references them. `templates:` lists every key in `Templates` (Mimir requires names to match `template_files`). Route receiver names are rewritten from ContactPoint name to `ReceiverName(policy.Namespace, name)`.

- [ ] **Step 1: Failing tests**

`internal/compile/alertmanager_test.go`:

```go
package compile

import (
	"errors"
	"strings"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	"github.com/antnsn/alerts-operator/api/v1alpha1"
)

func ptr[T any](v T) *T { return &v }

func secrets(m map[string]string) SecretResolver {
	return func(ns, name, key string) (string, error) {
		v, ok := m[ns+"/"+name+"/"+key]
		if !ok {
			return "", errors.New("missing " + ns + "/" + name + "/" + key)
		}
		return v, nil
	}
}

func fullInput() AlertmanagerInput {
	return AlertmanagerInput{
		Policy: &v1alpha1.NotificationPolicy{ObjectMeta: metav1.ObjectMeta{Name: "homelab", Namespace: "monitoring"}, Spec: v1alpha1.NotificationPolicySpec{
			TenantRef: "t",
			Route: v1alpha1.Route{Receiver: "keep", GroupBy: []string{"alertname", "namespace"}, GroupWait: "30s", GroupInterval: "5m", RepeatInterval: "4h",
				Routes: []apiextensionsv1.JSON{{Raw: []byte(`{"receiver":"pushover","matchers":["severity=\"critical\""],"continue":true}`)}}},
			InhibitRules: []v1alpha1.InhibitRule{{SourceMatchers: []string{`severity="critical"`}, TargetMatchers: []string{`severity="warning"`}, Equal: []string{"alertname"}}},
		}},
		ContactPoints: []v1alpha1.ContactPoint{
			{ObjectMeta: metav1.ObjectMeta{Name: "pushover", Namespace: "monitoring"}, Spec: v1alpha1.ContactPointSpec{TenantRef: "t", Pushover: []v1alpha1.PushoverConfig{{UserKeySecretRef: v1alpha1.SecretKeyRef{Name: "po", Key: "user"}, TokenSecretRef: v1alpha1.SecretKeyRef{Name: "po", Key: "token"}, Priority: "1", SendResolved: ptr(true)}}}},
			{ObjectMeta: metav1.ObjectMeta{Name: "keep", Namespace: "monitoring"}, Spec: v1alpha1.ContactPointSpec{TenantRef: "t", Webhook: []v1alpha1.WebhookConfig{{URL: "http://keep-backend.keep:8080/alerts/event/prometheus", HTTPConfig: &v1alpha1.HTTPConfig{BearerTokenSecretRef: &v1alpha1.SecretKeyRef{Name: "keep", Key: "api-key"}}, MaxAlerts: ptr(int32(0))}}}},
			{ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "monitoring"}, Spec: v1alpha1.ContactPointSpec{TenantRef: "t",
				Slack:    []v1alpha1.SlackConfig{{APIURLSecretRef: v1alpha1.SecretKeyRef{Name: "slack", Key: "url"}, Channel: "#alerts", Title: "t", Text: "x"}},
				Discord:  []v1alpha1.DiscordConfig{{WebhookURLSecretRef: v1alpha1.SecretKeyRef{Name: "discord", Key: "url"}}},
				Telegram: []v1alpha1.TelegramConfig{{BotTokenSecretRef: v1alpha1.SecretKeyRef{Name: "tg", Key: "token"}, ChatID: 42, ParseMode: "HTML"}},
				Email:    []v1alpha1.EmailConfig{{To: "a@b.c", From: "x@b.c", Smarthost: "smtp:587", AuthUsername: "x", AuthPasswordSecretRef: &v1alpha1.SecretKeyRef{Name: "smtp", Key: "pw"}, RequireTLS: ptr(true)}},
			}},
			{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "team-b"}, Spec: v1alpha1.ContactPointSpec{TenantRef: "t", Webhook: []v1alpha1.WebhookConfig{{URLSecretRef: &v1alpha1.SecretKeyRef{Name: "hook", Key: "url"}}}}},
		},
		Secrets: secrets(map[string]string{
			"monitoring/po/user": "U", "monitoring/po/token": "T", "monitoring/keep/api-key": "K",
			"monitoring/slack/url": "https://hooks.slack.com/x", "monitoring/discord/url": "https://discord.com/api/webhooks/x",
			"monitoring/tg/token": "TG", "monitoring/smtp/pw": "PW", "team-b/hook/url": "http://hook",
		}),
		Templates: map[string]string{"default.tmpl": `{{ define "pushover.default.title" }}[{{ .Status }}] {{ .CommonLabels.alertname }}{{ end }}`},
	}
}

func TestAlertmanagerGolden(t *testing.T) {
	cfg, err := Alertmanager(fullInput())
	if err != nil {
		t.Fatal(err)
	}
	b, _ := yaml.Marshal(cfg)
	golden(t, "am_full.golden.yaml", b)
	if !strings.Contains(cfg.Config, "receiver: monitoring/keep") || !strings.Contains(cfg.Config, "- name: team-b/other") {
		t.Fatalf("receiver naming:\n%s", cfg.Config)
	}
	if !strings.Contains(cfg.Config, "templates:\n- default.tmpl") {
		t.Fatalf("templates list missing:\n%s", cfg.Config)
	}
	if HashAlertmanager(cfg) != HashAlertmanager(cfg) || !strings.HasPrefix(HashAlertmanager(cfg), "sha256:") {
		t.Fatal("hash")
	}
}

func TestAlertmanagerAttributesErrors(t *testing.T) {
	in := fullInput()
	in.Secrets = secrets(map[string]string{})
	_, err := Alertmanager(in)
	var ae *AttributedError
	if !errors.As(err, &ae) || ae.Kind != "ContactPoint" {
		t.Fatalf("expected ContactPoint attribution, got %v", err)
	}

	in = fullInput()
	in.Policy.Spec.Route.Routes[0] = apiextensionsv1.JSON{Raw: []byte(`{"receiver":"nope"}`)}
	_, err = Alertmanager(in)
	if !errors.As(err, &ae) || ae.Kind != "NotificationPolicy" || !strings.Contains(err.Error(), "nope") {
		t.Fatalf("expected policy attribution, got %v", err)
	}

	in = fullInput()
	in.Policy.Spec.Route.GroupWait = "not-a-duration"
	_, err = Alertmanager(in)
	if !errors.As(err, &ae) || ae.Kind != "NotificationPolicy" {
		t.Fatalf("expected validation attributed to policy, got %v", err)
	}

	in = fullInput()
	in.Policy.Spec.Route.Routes[0] = apiextensionsv1.JSON{Raw: []byte("[]")}
	_, err = Alertmanager(in)
	if !errors.As(err, &ae) || ae.Kind != "NotificationPolicy" {
		t.Fatalf("expected decode-error attribution, got %v", err)
	}
}

func TestAlertmanagerRejectsBrokenTemplate(t *testing.T) {
	in := fullInput()
	in.Templates = map[string]string{"bad.tmpl": `{{ define "x" }}{{ .Unclosed `}
	_, err := Alertmanager(in)
	if err == nil || !strings.Contains(err.Error(), "templates") {
		t.Fatalf("expected template parse error, got %v", err)
	}
}

func TestValidatePromQL(t *testing.T) {
	if err := ValidatePromQL(`up{job="x"} == 0`); err != nil {
		t.Fatal(err)
	}
	if err := ValidatePromQL(`up{job=`); err == nil {
		t.Fatal("expected parse error")
	}
}
```

- [ ] **Step 2: Run, expect failure**

Run: `go test ./internal/compile/`
Expected: FAIL.

- [ ] **Step 3: Implement compile**

`internal/compile/alertmanager.go`:

```go
package compile

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"

	"sigs.k8s.io/yaml"

	"github.com/antnsn/alerts-operator/api/v1alpha1"
	"github.com/antnsn/alerts-operator/internal/backend"
)

// SecretResolver returns the value of one Secret key.
type SecretResolver func(namespace, name, key string) (string, error)

// AttributedError names the CR responsible for a compile/validation failure.
type AttributedError struct {
	Kind      string
	Namespace string
	Name      string
	Err       error
}

func (e *AttributedError) Error() string {
	return fmt.Sprintf("%s %s/%s: %v", e.Kind, e.Namespace, e.Name, e.Err)
}
func (e *AttributedError) Unwrap() error { return e.Err }

// secretRecorder wraps a SecretResolver, recording every non-empty value it resolves over the
// lifetime of one Alertmanager() call. Alertmanager's own config/template errors can embed raw
// field values verbatim (e.g. a url.Parse failure quotes the whole URL it was given), so every
// error this package returns is passed through redact() first to keep secret values out of
// status conditions and events.
type secretRecorder struct {
	inner  SecretResolver
	values map[string]struct{}
}

func newSecretRecorder(inner SecretResolver) *secretRecorder {
	return &secretRecorder{inner: inner, values: map[string]struct{}{}}
}

// resolve satisfies SecretResolver, recording the value before returning it.
func (r *secretRecorder) resolve(namespace, name, key string) (string, error) {
	v, err := r.inner(namespace, name, key)
	if v != "" {
		r.values[v] = struct{}{}
	}
	return v, err
}

// redact replaces every recorded secret value in msg with "[REDACTED]". Longest values are
// replaced first so a value that is a substring of another isn't left partially redacted.
func (r *secretRecorder) redact(msg string) string {
	vals := make([]string, 0, len(r.values))
	for v := range r.values {
		vals = append(vals, v)
	}
	sort.Slice(vals, func(i, j int) bool { return len(vals[i]) > len(vals[j]) })
	for _, v := range vals {
		msg = strings.ReplaceAll(msg, v, "[REDACTED]")
	}
	return msg
}

// attribute wraps a non-nil err as an *AttributedError with its message redacted of any secret
// value resolved so far. Returns nil when err is nil.
func (r *secretRecorder) attribute(kind, namespace, name string, err error) error {
	if err == nil {
		return nil
	}
	return &AttributedError{Kind: kind, Namespace: namespace, Name: name, Err: errors.New(r.redact(err.Error()))}
}

// AlertmanagerInput is everything needed to compile one tenant's Alertmanager document.
type AlertmanagerInput struct {
	Policy        *v1alpha1.NotificationPolicy
	ContactPoints []v1alpha1.ContactPoint
	Secrets       SecretResolver
	Templates     map[string]string
}

// ReceiverName is the Alertmanager receiver name for a ContactPoint.
func ReceiverName(namespace, name string) string { return namespace + "/" + name }

// --- Alertmanager-native YAML shapes (json tags = AM yaml keys) ---

type amConfig struct {
	Route        *amRoute        `json:"route"`
	Receivers    []amReceiver    `json:"receivers"`
	InhibitRules []amInhibitRule `json:"inhibit_rules,omitempty"`
	Templates    []string        `json:"templates,omitempty"`
}

type amRoute struct {
	Receiver       string    `json:"receiver"`
	GroupBy        []string  `json:"group_by,omitempty"`
	GroupWait      string    `json:"group_wait,omitempty"`
	GroupInterval  string    `json:"group_interval,omitempty"`
	RepeatInterval string    `json:"repeat_interval,omitempty"`
	Matchers       []string  `json:"matchers,omitempty"`
	Continue       bool      `json:"continue,omitempty"`
	Routes         []amRoute `json:"routes,omitempty"`
}

type amInhibitRule struct {
	SourceMatchers []string `json:"source_matchers,omitempty"`
	TargetMatchers []string `json:"target_matchers,omitempty"`
	Equal          []string `json:"equal,omitempty"`
}

type amReceiver struct {
	Name            string       `json:"name"`
	WebhookConfigs  []amWebhook  `json:"webhook_configs,omitempty"`
	PushoverConfigs []amPushover `json:"pushover_configs,omitempty"`
	SlackConfigs    []amSlack    `json:"slack_configs,omitempty"`
	DiscordConfigs  []amDiscord  `json:"discord_configs,omitempty"`
	TelegramConfigs []amTelegram `json:"telegram_configs,omitempty"`
	EmailConfigs    []amEmail    `json:"email_configs,omitempty"`
}

type amHTTPConfig struct {
	Authorization *amAuthorization `json:"authorization,omitempty"`
	BasicAuth     *amBasicAuth     `json:"basic_auth,omitempty"`
}
type amAuthorization struct {
	Credentials string `json:"credentials"`
}
type amBasicAuth struct {
	Username string `json:"username"`
	Password string `json:"password"`
}
type amWebhook struct {
	SendResolved *bool         `json:"send_resolved,omitempty"`
	URL          string        `json:"url"`
	HTTPConfig   *amHTTPConfig `json:"http_config,omitempty"`
	MaxAlerts    *int32        `json:"max_alerts,omitempty"`
}
type amPushover struct {
	SendResolved *bool  `json:"send_resolved,omitempty"`
	UserKey      string `json:"user_key"`
	Token        string `json:"token"`
	Title        string `json:"title,omitempty"`
	Message      string `json:"message,omitempty"`
	URL          string `json:"url,omitempty"`
	URLTitle     string `json:"url_title,omitempty"`
	Priority     string `json:"priority,omitempty"`
	Sound        string `json:"sound,omitempty"`
}
type amSlack struct {
	SendResolved *bool  `json:"send_resolved,omitempty"`
	APIURL       string `json:"api_url"`
	Channel      string `json:"channel,omitempty"`
	Username     string `json:"username,omitempty"`
	Title        string `json:"title,omitempty"`
	Text         string `json:"text,omitempty"`
	IconEmoji    string `json:"icon_emoji,omitempty"`
}
type amDiscord struct {
	SendResolved *bool  `json:"send_resolved,omitempty"`
	WebhookURL   string `json:"webhook_url"`
	Title        string `json:"title,omitempty"`
	Message      string `json:"message,omitempty"`
}
type amTelegram struct {
	SendResolved *bool  `json:"send_resolved,omitempty"`
	BotToken     string `json:"bot_token"`
	ChatID       int64  `json:"chat_id"`
	ParseMode    string `json:"parse_mode,omitempty"`
	Message      string `json:"message,omitempty"`
}
type amEmail struct {
	SendResolved *bool  `json:"send_resolved,omitempty"`
	To           string `json:"to"`
	From         string `json:"from,omitempty"`
	Smarthost    string `json:"smarthost,omitempty"`
	Hello        string `json:"hello,omitempty"`
	AuthUsername string `json:"auth_username,omitempty"`
	AuthPassword string `json:"auth_password,omitempty"`
	RequireTLS   *bool  `json:"require_tls,omitempty"`
}

// Alertmanager compiles and validates the per-tenant Alertmanager document.
func Alertmanager(in AlertmanagerInput) (*backend.AlertmanagerConfig, error) {
	if in.Policy == nil {
		return nil, fmt.Errorf("no NotificationPolicy")
	}
	rec := newSecretRecorder(in.Secrets)
	cfg := amConfig{}

	// Receivers from every ContactPoint, deterministic order. Each receiver is also validated in
	// isolation so a receiver-specific failure (malformed webhook/Slack URL, bad email settings,
	// ...) is attributed to the ContactPoint that produced it, not folded into the whole-document
	// validation below (which is attributed to the policy).
	byName := map[string]bool{}
	for i := range in.ContactPoints {
		cp := &in.ContactPoints[i]
		rcv, err := compileReceiver(cp, rec.resolve)
		if err != nil {
			return nil, rec.attribute("ContactPoint", cp.Namespace, cp.Name, err)
		}
		if err := validateReceiver(rcv); err != nil {
			return nil, rec.attribute("ContactPoint", cp.Namespace, cp.Name, err)
		}
		cfg.Receivers = append(cfg.Receivers, rcv)
		byName[rcv.Name] = true
	}
	sort.Slice(cfg.Receivers, func(i, j int) bool { return cfg.Receivers[i].Name < cfg.Receivers[j].Name })

	// Route tree, rewriting receiver names to <policy-ns>/<name>.
	pol := in.Policy
	route, err := compileRoute(&pol.Spec.Route, pol.Namespace, byName)
	if err != nil {
		return nil, rec.attribute("NotificationPolicy", pol.Namespace, pol.Name, err)
	}
	cfg.Route = route
	for _, ir := range pol.Spec.InhibitRules {
		cfg.InhibitRules = append(cfg.InhibitRules, amInhibitRule{SourceMatchers: ir.SourceMatchers, TargetMatchers: ir.TargetMatchers, Equal: ir.Equal})
	}

	// Templates: names must match template_files keys.
	for name := range in.Templates {
		cfg.Templates = append(cfg.Templates, name)
	}
	sort.Strings(cfg.Templates)

	raw, err := yaml.Marshal(cfg)
	if err != nil {
		return nil, errors.New(rec.redact(err.Error()))
	}
	out := &backend.AlertmanagerConfig{Config: string(raw), TemplateFiles: in.Templates}
	if err := ValidateAlertmanager(out); err != nil {
		// Whole-document validation covers route/matchers/durations/inhibit/templates: attribute
		// to the policy. Receiver-specific failures are caught earlier, per-receiver, above.
		return nil, rec.attribute("NotificationPolicy", pol.Namespace, pol.Name, err)
	}
	return out, nil
}

func compileRoute(r *v1alpha1.Route, ns string, known map[string]bool) (*amRoute, error) {
	full := ReceiverName(ns, r.Receiver)
	if !known[full] {
		return nil, fmt.Errorf("receiver %q not found as ContactPoint %s", r.Receiver, full)
	}
	out := &amRoute{Receiver: full, GroupBy: r.GroupBy, GroupWait: r.GroupWait, GroupInterval: r.GroupInterval,
		RepeatInterval: r.RepeatInterval, Matchers: r.Matchers, Continue: r.Continue}
	children, err := r.ChildRoutes()
	if err != nil {
		return nil, fmt.Errorf("route %q: %w", r.Receiver, err)
	}
	for i := range children {
		child, err := compileRoute(&children[i], ns, known)
		if err != nil {
			return nil, err
		}
		out.Routes = append(out.Routes, *child)
	}
	return out, nil
}

func compileReceiver(cp *v1alpha1.ContactPoint, secrets SecretResolver) (amReceiver, error) {
	get := func(ref *v1alpha1.SecretKeyRef) (string, error) {
		if ref == nil {
			return "", nil
		}
		v, err := secrets(cp.Namespace, ref.Name, ref.Key)
		if err != nil {
			return "", fmt.Errorf("secret %s/%s key %s: %w", cp.Namespace, ref.Name, ref.Key, err)
		}
		return v, nil
	}
	rcv := amReceiver{Name: ReceiverName(cp.Namespace, cp.Name)}
	for _, w := range cp.Spec.Webhook {
		url := w.URL
		if w.URLSecretRef != nil {
			v, err := get(w.URLSecretRef)
			if err != nil {
				return rcv, err
			}
			url = v
		}
		hook := amWebhook{URL: url, SendResolved: w.SendResolved, MaxAlerts: w.MaxAlerts}
		if w.HTTPConfig != nil {
			hc := &amHTTPConfig{}
			if w.HTTPConfig.BearerTokenSecretRef != nil {
				tok, err := get(w.HTTPConfig.BearerTokenSecretRef)
				if err != nil {
					return rcv, err
				}
				hc.Authorization = &amAuthorization{Credentials: tok}
			}
			if w.HTTPConfig.BasicAuth != nil {
				u, err := get(&w.HTTPConfig.BasicAuth.UsernameSecretRef)
				if err != nil {
					return rcv, err
				}
				p, err := get(&w.HTTPConfig.BasicAuth.PasswordSecretRef)
				if err != nil {
					return rcv, err
				}
				hc.BasicAuth = &amBasicAuth{Username: u, Password: p}
			}
			hook.HTTPConfig = hc
		}
		rcv.WebhookConfigs = append(rcv.WebhookConfigs, hook)
	}
	for _, p := range cp.Spec.Pushover {
		user, err := get(&p.UserKeySecretRef)
		if err != nil {
			return rcv, err
		}
		tok, err := get(&p.TokenSecretRef)
		if err != nil {
			return rcv, err
		}
		rcv.PushoverConfigs = append(rcv.PushoverConfigs, amPushover{SendResolved: p.SendResolved, UserKey: user, Token: tok,
			Title: p.Title, Message: p.Message, URL: p.URL, URLTitle: p.URLTitle, Priority: p.Priority, Sound: p.Sound})
	}
	for _, s := range cp.Spec.Slack {
		u, err := get(&s.APIURLSecretRef)
		if err != nil {
			return rcv, err
		}
		rcv.SlackConfigs = append(rcv.SlackConfigs, amSlack{SendResolved: s.SendResolved, APIURL: u, Channel: s.Channel, Username: s.Username, Title: s.Title, Text: s.Text, IconEmoji: s.IconEmoji})
	}
	for _, d := range cp.Spec.Discord {
		u, err := get(&d.WebhookURLSecretRef)
		if err != nil {
			return rcv, err
		}
		rcv.DiscordConfigs = append(rcv.DiscordConfigs, amDiscord{SendResolved: d.SendResolved, WebhookURL: u, Title: d.Title, Message: d.Message})
	}
	for _, tg := range cp.Spec.Telegram {
		tok, err := get(&tg.BotTokenSecretRef)
		if err != nil {
			return rcv, err
		}
		rcv.TelegramConfigs = append(rcv.TelegramConfigs, amTelegram{SendResolved: tg.SendResolved, BotToken: tok, ChatID: tg.ChatID, ParseMode: tg.ParseMode, Message: tg.Message})
	}
	for _, e := range cp.Spec.Email {
		pw, err := get(e.AuthPasswordSecretRef)
		if err != nil {
			return rcv, err
		}
		rcv.EmailConfigs = append(rcv.EmailConfigs, amEmail{SendResolved: e.SendResolved, To: e.To, From: e.From, Smarthost: e.Smarthost, Hello: e.Hello, AuthUsername: e.AuthUsername, AuthPassword: pw, RequireTLS: e.RequireTLS})
	}
	return rcv, nil
}

// HashAlertmanager returns "sha256:<hex>" over the canonical YAML of cfg.
func HashAlertmanager(cfg *backend.AlertmanagerConfig) string {
	b, _ := yaml.Marshal(cfg)
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}
```

`internal/compile/validate.go`:

```go
package compile

import (
	"fmt"
	"os"
	"path/filepath"

	amconfig "github.com/prometheus/alertmanager/config"
	amtemplate "github.com/prometheus/alertmanager/template"
	"github.com/prometheus/prometheus/promql/parser"
	"sigs.k8s.io/yaml"

	"github.com/antnsn/alerts-operator/internal/backend"
)

// ValidateAlertmanager runs Alertmanager's own config loader on the compiled document and
// parses the template files the same way Alertmanager does at startup (config.Load alone
// does not touch templates). Template names are rewritten to real temp files first, scoped
// strictly to the document's "templates" key: the document is unmarshaled into a generic map
// and only that key is replaced, so a matcher, group_by label, or receiver name that happens to
// equal a template name is never touched.
func ValidateAlertmanager(cfg *backend.AlertmanagerConfig) error {
	text := cfg.Config
	var paths []string
	if len(cfg.TemplateFiles) > 0 {
		dir, err := os.MkdirTemp("", "am-templates-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(dir) //nolint:errcheck // best-effort cleanup of a temp dir

		var doc map[string]any
		if err := yaml.Unmarshal([]byte(text), &doc); err != nil {
			return fmt.Errorf("alertmanager config: %w", err)
		}
		for name, body := range cfg.TemplateFiles {
			p := filepath.Join(dir, name)
			if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
				return err
			}
			paths = append(paths, p)
		}
		doc["templates"] = paths
		raw, err := yaml.Marshal(doc)
		if err != nil {
			return fmt.Errorf("alertmanager config: %w", err)
		}
		text = string(raw)
	}
	if _, err := amconfig.Load(text); err != nil {
		return fmt.Errorf("alertmanager config: %w", err)
	}
	if len(paths) > 0 {
		if _, err := amtemplate.FromGlobs(paths); err != nil {
			return fmt.Errorf("alertmanager templates: %w", err)
		}
	}
	return nil
}

// validateReceiver runs Alertmanager's config loader against a single receiver in isolation
// (wrapped in the smallest valid document: a root route that points at it and nothing else), so
// receiver-specific failures -- a malformed webhook/Slack URL, invalid email settings, and so on
// -- surface here and can be attributed to the ContactPoint that produced them, rather than
// being folded into the whole-document validation in ValidateAlertmanager.
func validateReceiver(rcv amReceiver) error {
	doc := amConfig{Route: &amRoute{Receiver: rcv.Name}, Receivers: []amReceiver{rcv}}
	raw, err := yaml.Marshal(doc)
	if err != nil {
		return err
	}
	if _, err := amconfig.Load(string(raw)); err != nil {
		return fmt.Errorf("receiver: %w", err)
	}
	return nil
}

// promqlParser is stateless (holds only Options) and safe for concurrent use; shared across calls.
var promqlParser = parser.NewParser(parser.Options{})

// ValidatePromQL parses a Mimir rule expression.
func ValidatePromQL(expr string) error {
	_, err := promqlParser.ParseExpr(expr)
	return err
}
```

Run `go get github.com/prometheus/alertmanager@v0.34.1 github.com/prometheus/prometheus@v0.314.0 && go mod tidy`. If `go mod tidy` reports replace-directive conflicts from the prometheus module, copy the `replace` lines it names from prometheus's own `go.mod` into ours and re-run.

Note: `github.com/prometheus/prometheus/promql/parser` at the pinned `v0.314.0` no longer exports a package-level `ParseExpr`; it moved to a method on a `Parser` interface obtained via `parser.NewParser(parser.Options{})`. `ValidatePromQL`'s exported signature is unaffected.

Fix round 1 (post-Step-5, same task): a Codex review flagged that folding every `ValidateAlertmanager` failure into a `NotificationPolicy`-attributed error mis-attributes receiver-specific failures (a malformed webhook/Slack URL, say) that actually belong to a `ContactPoint`. `Alertmanager()` now validates each compiled receiver in isolation (`validateReceiver`, minimal `{route: {receiver: <name>}, receivers: [<rcv>]}` doc) immediately after compiling it, before it's added to the document; only failures from the remaining whole-document validation (route/matchers/durations/inhibit/templates) are attributed to the policy. The same round also closed two other findings: (1) `amconfig.Load` (and `url.Parse` in particular) can embed raw secret values verbatim in its error text, so every error `Alertmanager()` returns is now passed through a `secretRecorder` that redacts every value resolved during that call before the error is returned; (2) the old whole-document `strings.ReplaceAll(text, "- "+name+"\n", ...)` template-path rewrite could corrupt an unrelated matcher/group_by/receiver value that happened to equal a template name, so the rewrite is now scoped to the document's `templates` key via an unmarshal/replace-key/remarshal round trip.

- [ ] **Step 4: Golden + run**

Run: `go test ./internal/compile/ -update && go test ./internal/compile/ -v`
Expected: PASS. Inspect `testdata/am_full.golden.yaml`: `alertmanager_config` string contains `receivers:` sorted `monitoring/chat`, `monitoring/keep`, `monitoring/pushover`, `team-b/other`; webhook has `authorization: {credentials: K}`; `template_files` has `default.tmpl`.

- [ ] **Step 5: Commit**

```bash
git add internal/compile go.mod go.sum
git commit -m "feat(compile): Alertmanager document from policy and contact points with local validation"
```

---

---

### Task 13: Field indexers

**Files:**
- Create: `internal/index/index.go`
- Modify: `internal/controller/suite_test.go` (register indexers before manager start), `cmd/main.go` (same)
- Test: `internal/controller/index_test.go`

**Interfaces:**
- Consumes: `v1alpha1.ContactPoint.SecretRefs()` (Task 5), `Spec.TenantRef` on the three namespaced kinds (Tasks 4–6).
- Produces:
```go
package index
const IndexTenantRef  = "spec.tenantRef"   // ContactPoint, NotificationPolicy, AlertRuleGroup → []string{spec.tenantRef}
const IndexSecretRefs = "spec.secretRefs"  // ContactPoint → unique Secret names referenced by SecretRefs()
func Register(ctx context.Context, mgr ctrl.Manager) error
```

- [ ] **Step 1: Failing envtest test**

`internal/controller/index_test.go`:

```go
package controller

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	observabilityv1alpha1 "github.com/antnsn/alerts-operator/api/v1alpha1"
	"github.com/antnsn/alerts-operator/internal/index"
)

func TestIndexers(t *testing.T) {
	cpA := &observabilityv1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "idx-a", Namespace: "default"}, Spec: observabilityv1alpha1.ContactPointSpec{TenantRef: "idx-tenant-1",
		Pushover: []observabilityv1alpha1.PushoverConfig{{UserKeySecretRef: observabilityv1alpha1.SecretKeyRef{Name: "po", Key: "user"}, TokenSecretRef: observabilityv1alpha1.SecretKeyRef{Name: "po", Key: "token"}}}}}
	cpB := &observabilityv1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "idx-b", Namespace: "default"}, Spec: observabilityv1alpha1.ContactPointSpec{TenantRef: "idx-tenant-2",
		Webhook: []observabilityv1alpha1.WebhookConfig{{URLSecretRef: &observabilityv1alpha1.SecretKeyRef{Name: "hook", Key: "url"}}}}}
	arg := &observabilityv1alpha1.AlertRuleGroup{ObjectMeta: metav1.ObjectMeta{Name: "idx-arg", Namespace: "default"}, Spec: observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: "idx-tenant-1", Backend: "mimir",
		Groups: []observabilityv1alpha1.RuleGroup{{Name: "g", Rules: []observabilityv1alpha1.Rule{{Alert: "A", Expr: "up == 0"}}}}}}
	pol := &observabilityv1alpha1.NotificationPolicy{ObjectMeta: metav1.ObjectMeta{Name: "idx-pol", Namespace: "default"}, Spec: observabilityv1alpha1.NotificationPolicySpec{TenantRef: "idx-tenant-1", Route: observabilityv1alpha1.Route{Receiver: "idx-a"}}}
	for _, o := range []client.Object{cpA, cpB, arg, pol} {
		if err := testClient.Create(testCtx, o); err != nil {
			t.Fatal(err)
		}
		defer func(o client.Object) { _ = testClient.Delete(testCtx, o) }(o)
	}

	waitFor(t, func() bool {
		var cps observabilityv1alpha1.ContactPointList
		if err := testClient.List(testCtx, &cps, client.MatchingFields{index.IndexTenantRef: "idx-tenant-1"}); err != nil {
			return false
		}
		return len(cps.Items) == 1 && cps.Items[0].Name == "idx-a"
	})
	waitFor(t, func() bool {
		var cps observabilityv1alpha1.ContactPointList
		if err := testClient.List(testCtx, &cps, client.InNamespace("default"), client.MatchingFields{index.IndexSecretRefs: "po"}); err != nil {
			return false
		}
		return len(cps.Items) == 1 && cps.Items[0].Name == "idx-a"
	})
	waitFor(t, func() bool {
		var cps observabilityv1alpha1.ContactPointList
		if err := testClient.List(testCtx, &cps, client.MatchingFields{index.IndexSecretRefs: "hook"}); err != nil {
			return false
		}
		return len(cps.Items) == 1 && cps.Items[0].Name == "idx-b"
	})
	waitFor(t, func() bool {
		var args observabilityv1alpha1.AlertRuleGroupList
		var pols observabilityv1alpha1.NotificationPolicyList
		if err := testClient.List(testCtx, &args, client.MatchingFields{index.IndexTenantRef: "idx-tenant-1"}); err != nil {
			return false
		}
		if err := testClient.List(testCtx, &pols, client.MatchingFields{index.IndexTenantRef: "idx-tenant-1"}); err != nil {
			return false
		}
		return len(args.Items) == 1 && len(pols.Items) == 1
	})
}
```

- [ ] **Step 2: Run, expect failure**

Run: `make test`
Expected: FAIL (package `index` missing).

- [ ] **Step 3: Implement**

`internal/index/index.go`:

```go
// Package index registers the cache field indexers used by the controllers.
package index

import (
	"context"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/antnsn/alerts-operator/api/v1alpha1"
)

const (
	// IndexTenantRef indexes ContactPoint, NotificationPolicy and AlertRuleGroup by spec.tenantRef.
	IndexTenantRef = "spec.tenantRef"
	// IndexSecretRefs indexes ContactPoint by every Secret name it references.
	IndexSecretRefs = "spec.secretRefs"
)

// Register installs all indexers. Must run before the manager starts.
func Register(ctx context.Context, mgr ctrl.Manager) error {
	idx := mgr.GetFieldIndexer()
	if err := idx.IndexField(ctx, &v1alpha1.ContactPoint{}, IndexTenantRef, func(o client.Object) []string {
		return []string{o.(*v1alpha1.ContactPoint).Spec.TenantRef}
	}); err != nil {
		return err
	}
	if err := idx.IndexField(ctx, &v1alpha1.NotificationPolicy{}, IndexTenantRef, func(o client.Object) []string {
		return []string{o.(*v1alpha1.NotificationPolicy).Spec.TenantRef}
	}); err != nil {
		return err
	}
	if err := idx.IndexField(ctx, &v1alpha1.AlertRuleGroup{}, IndexTenantRef, func(o client.Object) []string {
		return []string{o.(*v1alpha1.AlertRuleGroup).Spec.TenantRef}
	}); err != nil {
		return err
	}
	return idx.IndexField(ctx, &v1alpha1.ContactPoint{}, IndexSecretRefs, func(o client.Object) []string {
		seen := map[string]bool{}
		var out []string
		for _, r := range o.(*v1alpha1.ContactPoint).SecretRefs() {
			if !seen[r.Name] {
				seen[r.Name] = true
				out = append(out, r.Name)
			}
		}
		return out
	})
}
```

In `internal/controller/suite_test.go`, after `testClient = mgr.GetClient()` and before `setupReconcilers(mgr)`, add:

```go
	if err := index.Register(testCtx, mgr); err != nil {
		panic(err)
	}
```
with import `"github.com/antnsn/alerts-operator/internal/index"`.

In `cmd/main.go`, right after `mgr, err := ctrl.NewManager(...)` succeeds and before the reconcilers are set up, add:

```go
	if err := index.Register(context.Background(), mgr); err != nil {
		setupLog.Error(err, "unable to register field indexers")
		os.Exit(1)
	}
```
with imports `"context"` and `"github.com/antnsn/alerts-operator/internal/index"`.

- [ ] **Step 4: Run tests**

Run: `make test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/index internal/controller/suite_test.go internal/controller/index_test.go cmd/main.go
git commit -m "feat: field indexers for tenantRef and referenced secrets"
```

---

### Task 14: Status/condition helpers and Tenant mapping

**Files:**
- Create: `api/v1alpha1/conditioned.go`, `internal/controller/conditions.go`
- Test: `api/v1alpha1/conditioned_test.go`, `internal/controller/conditions_test.go`

**Interfaces:**
- Consumes: condition constants (Task 2).
- Produces:
```go
// api/v1alpha1
type Conditioned interface { client.Object; GetConditions() []metav1.Condition; SetConditions([]metav1.Condition) }
// implemented by *Tenant, *ContactPoint, *NotificationPolicy, *AlertRuleGroup

// internal/controller
func setCondition(conds *[]metav1.Condition, typ string, status metav1.ConditionStatus, reason, msg string, gen int64) bool
func patchStatus(ctx context.Context, c client.Client, obj client.Object, mutate func(), changed *bool) error  // merge patch on status subresource with optimistic lock; skipped when *changed is false after mutate (nil = always patch)
func tenantRequest(name string) reconcile.Request
func tenantRefOf(o client.Object) string                                     // "" for unknown kinds
func mapToTenant(ctx context.Context, o client.Object) []reconcile.Request   // for handler.EnqueueRequestsFromMapFunc
func syncedFromErr(err error) (metav1.ConditionStatus, string, string)      // nil→True/Synced; IsUnavailable→False/BackendUnavailable; IsRejected→False/Rejected; else False/Invalid
```

- [ ] **Step 1: Failing tests**

`api/v1alpha1/conditioned_test.go`:

```go
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
```

`internal/controller/conditions_test.go`:

```go
package controller

import (
	"context"
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	observabilityv1alpha1 "github.com/antnsn/alerts-operator/api/v1alpha1"
	"github.com/antnsn/alerts-operator/internal/backend"
)

func TestSetConditionReportsChange(t *testing.T) {
	var conds []metav1.Condition
	if !setCondition(&conds, observabilityv1alpha1.ConditionAccepted, metav1.ConditionTrue, observabilityv1alpha1.ReasonAccepted, "", 1) {
		t.Fatal("first set should report change")
	}
	if setCondition(&conds, observabilityv1alpha1.ConditionAccepted, metav1.ConditionTrue, observabilityv1alpha1.ReasonAccepted, "", 1) {
		t.Fatal("identical set should not report change")
	}
	if !setCondition(&conds, observabilityv1alpha1.ConditionAccepted, metav1.ConditionFalse, observabilityv1alpha1.ReasonTenantNotFound, "x", 2) {
		t.Fatal("status change should report change")
	}
	if conds[0].ObservedGeneration != 2 || conds[0].Message != "x" {
		t.Fatalf("condition not updated: %+v", conds[0])
	}
}

func TestMapToTenant(t *testing.T) {
	cp := &observabilityv1alpha1.ContactPoint{Spec: observabilityv1alpha1.ContactPointSpec{TenantRef: "t1"}}
	pol := &observabilityv1alpha1.NotificationPolicy{Spec: observabilityv1alpha1.NotificationPolicySpec{TenantRef: "t2"}}
	arg := &observabilityv1alpha1.AlertRuleGroup{Spec: observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: "t3"}}
	if got := mapToTenant(context.Background(), cp); len(got) != 1 || got[0].Name != "t1" || got[0].Namespace != "" {
		t.Fatalf("cp: %v", got)
	}
	if got := mapToTenant(context.Background(), pol); len(got) != 1 || got[0].Name != "t2" {
		t.Fatalf("pol: %v", got)
	}
	if got := mapToTenant(context.Background(), arg); len(got) != 1 || got[0].Name != "t3" {
		t.Fatalf("arg: %v", got)
	}
	if got := mapToTenant(context.Background(), &observabilityv1alpha1.Tenant{}); got != nil {
		t.Fatalf("tenant should not map: %v", got)
	}
}

func TestPatchStatusSkipsWhenUnchanged(t *testing.T) {
	obj := &observabilityv1alpha1.Tenant{}
	obj.Name = "never-created"
	unchanged := false
	// Would fail with NotFound if a patch were sent; a skipped patch returns nil.
	if err := patchStatus(testCtx, testClient, obj, func() {}, &unchanged); err != nil {
		t.Fatalf("unchanged patch should be skipped: %v", err)
	}
	changed := true
	if err := patchStatus(testCtx, testClient, obj, func() {}, &changed); err == nil {
		t.Fatal("changed patch must be sent (and fail NotFound here)")
	}
}

func TestSyncedFromErr(t *testing.T) {
	cases := []struct {
		err    error
		status metav1.ConditionStatus
		reason string
	}{
		{nil, metav1.ConditionTrue, observabilityv1alpha1.ReasonSynced},
		{&backend.StatusError{Status: 503}, metav1.ConditionFalse, observabilityv1alpha1.ReasonBackendUnavailable},
		{errors.New("dial tcp: refused"), metav1.ConditionFalse, observabilityv1alpha1.ReasonBackendUnavailable},
		{&backend.StatusError{Status: 400, Body: "bad"}, metav1.ConditionFalse, observabilityv1alpha1.ReasonRejected},
	}
	for _, c := range cases {
		s, r, _ := syncedFromErr(c.err)
		if s != c.status || r != c.reason {
			t.Fatalf("%v → %s/%s", c.err, s, r)
		}
	}
}
```
- [ ] **Step 2: Run, expect failure**

Run: `go test ./api/... ./internal/controller/ -run 'TestConditioned|TestSetCondition|TestMapToTenant|TestSyncedFromErr'`
Expected: FAIL (undefined symbols).

- [ ] **Step 3: Implement**

`api/v1alpha1/conditioned.go`:

```go
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Conditioned is implemented by every kind that carries status.conditions.
type Conditioned interface {
	client.Object
	GetConditions() []metav1.Condition
	SetConditions([]metav1.Condition)
}

func (t *Tenant) GetConditions() []metav1.Condition              { return t.Status.Conditions }
func (t *Tenant) SetConditions(c []metav1.Condition)             { t.Status.Conditions = c }
func (c *ContactPoint) GetConditions() []metav1.Condition        { return c.Status.Conditions }
func (c *ContactPoint) SetConditions(v []metav1.Condition)       { c.Status.Conditions = v }
func (n *NotificationPolicy) GetConditions() []metav1.Condition  { return n.Status.Conditions }
func (n *NotificationPolicy) SetConditions(v []metav1.Condition) { n.Status.Conditions = v }
func (a *AlertRuleGroup) GetConditions() []metav1.Condition      { return a.Status.Conditions }
func (a *AlertRuleGroup) SetConditions(v []metav1.Condition)     { a.Status.Conditions = v }

var (
	_ Conditioned = (*Tenant)(nil)
	_ Conditioned = (*ContactPoint)(nil)
	_ Conditioned = (*NotificationPolicy)(nil)
	_ Conditioned = (*AlertRuleGroup)(nil)
)
```

`internal/controller/conditions.go`:

```go
package controller

import (
	"context"
	"unicode/utf8"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/antnsn/alerts-operator/api/v1alpha1"
	"github.com/antnsn/alerts-operator/internal/backend"
)

// maxConditionMessage matches metav1.Condition's Message MaxLength (32768), enforced by the
// generated CRD schema, minus headroom for the truncation marker. Spec fields such as
// tenantRef or a rule's expr/interval/for/keep_firing_for have no MaxLength of their own, and
// some backend/parser errors echo the offending input back verbatim, so an oversized value
// could otherwise produce a status patch the API server rejects -- leaving the condition never
// recorded and the object stuck retrying the same failing patch forever. Bounding it centrally
// in setCondition protects every reconciler, not just the ones that remember to do it locally.
const maxConditionMessage = 32768 - 256

// truncateMessage bounds msg to fit metav1.Condition's Message field, truncating on a UTF-8
// rune boundary so the result is always valid.
func truncateMessage(msg string) string {
	if len(msg) <= maxConditionMessage {
		return msg
	}
	const suffix = "… [truncated]"
	cut := maxConditionMessage - len(suffix)
	for cut > 0 && !utf8.RuneStart(msg[cut]) {
		cut--
	}
	return msg[:cut] + suffix
}

// setCondition upserts a condition and reports whether anything changed. msg is truncated to
// fit metav1.Condition's Message MaxLength before being recorded.
func setCondition(conds *[]metav1.Condition, typ string, status metav1.ConditionStatus, reason, msg string, gen int64) bool {
	return meta.SetStatusCondition(conds, metav1.Condition{
		Type: typ, Status: status, Reason: reason, Message: truncateMessage(msg), ObservedGeneration: gen,
	})
}

// patchStatus applies mutate to obj and merge-patches the status subresource.
// The optimistic lock makes a concurrent status writer (e.g. the Tenant reconciler
// setting Synced while a child sets Accepted) surface as a conflict → requeue,
// instead of one side silently overwriting the other's conditions.
//
// mutate reports through *changed whether anything differs; when it says false the
// patch is skipped. This matters: an optimistic-lock patch always carries
// metadata.resourceVersion, so an unconditional patch bumps the object, fires a
// watch event and re-enqueues every controller watching it — a hot loop.
func patchStatus(ctx context.Context, c client.Client, obj client.Object, mutate func(), changed *bool) error {
	base := obj.DeepCopyObject().(client.Object)
	mutate()
	if changed != nil && !*changed {
		return nil
	}
	return c.Status().Patch(ctx, obj, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
}

func tenantRequest(name string) reconcile.Request {
	return reconcile.Request{NamespacedName: types.NamespacedName{Name: name}}
}

// tenantRefOf returns spec.tenantRef for the namespaced kinds, "" otherwise.
func tenantRefOf(o client.Object) string {
	switch t := o.(type) {
	case *v1alpha1.ContactPoint:
		return t.Spec.TenantRef
	case *v1alpha1.NotificationPolicy:
		return t.Spec.TenantRef
	case *v1alpha1.AlertRuleGroup:
		return t.Spec.TenantRef
	}
	return ""
}

// mapToTenant enqueues the Tenant a child references. Works for delete events too,
// since the event object still carries spec.
func mapToTenant(_ context.Context, o client.Object) []reconcile.Request {
	if name := tenantRefOf(o); name != "" {
		return []reconcile.Request{tenantRequest(name)}
	}
	return nil
}

// syncedFromErr maps a backend error to a Synced-style condition.
func syncedFromErr(err error) (metav1.ConditionStatus, string, string) {
	switch {
	case err == nil:
		return metav1.ConditionTrue, v1alpha1.ReasonSynced, ""
	case backend.IsRejected(err):
		return metav1.ConditionFalse, v1alpha1.ReasonRejected, err.Error()
	case backend.IsUnavailable(err):
		return metav1.ConditionFalse, v1alpha1.ReasonBackendUnavailable, err.Error()
	default:
		return metav1.ConditionFalse, v1alpha1.ReasonInvalid, err.Error()
	}
}
```

- [ ] **Step 4: Run tests**

Run: `make generate && go test ./api/... ./internal/controller/ -run 'TestConditioned|TestSetCondition|TestMapToTenant|TestSyncedFromErr' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add api/v1alpha1/conditioned.go api/v1alpha1/conditioned_test.go api/v1alpha1/zz_generated.deepcopy.go internal/controller/conditions.go internal/controller/conditions_test.go
git commit -m "feat(controller): condition helpers, status patching and tenant mapping"
```

---

### Task 15: AlertRuleGroup controller

**Files:**
- Modify: `internal/controller/alertrulegroup_controller.go` (scaffolded), `internal/controller/suite_test.go` (`setupReconcilers`)
- Create: `internal/controller/helpers_test.go`
- Test: `internal/controller/alertrulegroup_validate_test.go`, `internal/controller/alertrulegroup_controller_test.go`

**Interfaces:**
- Consumes: `compile.ValidatePromQL`, `compile.BackendNamespace` (Tasks 11–12); `index.IndexTenantRef` (Task 13); `setCondition`, `patchStatus` (Task 14); `tenant.Prefix()` (Task 3).
- Produces:
```go
func validateRuleGroups(be v1alpha1.Backend, groups []v1alpha1.RuleGroup) error   // pure; nil when valid; message "group <name> rule <i>: <cause>"
type AlertRuleGroupReconciler struct { client.Client; Scheme *runtime.Scheme }
func (r *AlertRuleGroupReconciler) Reconcile(ctx, req) (ctrl.Result, error)
func (r *AlertRuleGroupReconciler) SetupWithManager(mgr ctrl.Manager) error
// helpers_test.go
func newFakeTenant(t *testing.T, name string, withMimir, withLoki bool) (*v1alpha1.Tenant, *fake.Server)  // creates Tenant with the selected backends pointing at a fake server; cleanup deletes Tenant, waits for it to vanish, closes server
func waitCondition(t *testing.T, obj v1alpha1.Conditioned, typ string, status metav1.ConditionStatus, reason string)
```

- [ ] **Step 1: Failing unit test for pure validation**

`internal/controller/alertrulegroup_validate_test.go`:

```go
package controller

import (
	"strings"
	"testing"

	observabilityv1alpha1 "github.com/antnsn/alerts-operator/api/v1alpha1"
)

func TestValidateRuleGroups(t *testing.T) {
	ok := []observabilityv1alpha1.RuleGroup{{Name: "g", Interval: "1m", Rules: []observabilityv1alpha1.Rule{{Alert: "A", Expr: `up{job="x"} == 0`, For: "5m", KeepFiringFor: "1h"}}}}
	if err := validateRuleGroups(observabilityv1alpha1.BackendMimir, ok); err != nil {
		t.Fatal(err)
	}
	bad := []observabilityv1alpha1.RuleGroup{{Name: "g", Rules: []observabilityv1alpha1.Rule{{Alert: "A", Expr: "up == 0"}, {Alert: "B", Expr: `up{job=`}}}}
	err := validateRuleGroups(observabilityv1alpha1.BackendMimir, bad)
	if err == nil || !strings.HasPrefix(err.Error(), "group g rule 1:") {
		t.Fatalf("expected rule index in message, got %v", err)
	}
	// LogQL is not parsed locally: a Loki group with the same expr passes.
	if err := validateRuleGroups(observabilityv1alpha1.BackendLoki, bad); err != nil {
		t.Fatalf("loki should skip expr parsing: %v", err)
	}
	badFor := []observabilityv1alpha1.RuleGroup{{Name: "g", Rules: []observabilityv1alpha1.Rule{{Alert: "A", Expr: "up", For: "soon"}}}}
	if err := validateRuleGroups(observabilityv1alpha1.BackendLoki, badFor); err == nil || !strings.Contains(err.Error(), "for") {
		t.Fatalf("bad duration: %v", err)
	}
	badInterval := []observabilityv1alpha1.RuleGroup{{Name: "g", Interval: "x", Rules: []observabilityv1alpha1.Rule{{Alert: "A", Expr: "up"}}}}
	if err := validateRuleGroups(observabilityv1alpha1.BackendMimir, badInterval); err == nil || !strings.HasPrefix(err.Error(), "group g: interval") {
		t.Fatalf("bad interval: %v", err)
	}
}
```

- [ ] **Step 2: Failing envtest tests + helpers**

`internal/controller/helpers_test.go`:

```go
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
```

`internal/controller/alertrulegroup_controller_test.go`:

```go
package controller

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	observabilityv1alpha1 "github.com/antnsn/alerts-operator/api/v1alpha1"
)

func ruleGroups(expr string) []observabilityv1alpha1.RuleGroup {
	return []observabilityv1alpha1.RuleGroup{{Name: "g", Rules: []observabilityv1alpha1.Rule{{Alert: "A", Expr: expr}}}}
}

func TestAlertRuleGroupAccepted(t *testing.T) {
	newFakeTenant(t, "arg-tenant", true, false) // mimir only

	missing := &observabilityv1alpha1.AlertRuleGroup{ObjectMeta: metav1.ObjectMeta{Name: "arg-missing", Namespace: "default"},
		Spec: observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: "arg-nope", Backend: "mimir", Groups: ruleGroups("up == 0")}}
	createAndCleanup(t, missing)
	waitCondition(t, missing, observabilityv1alpha1.ConditionAccepted, metav1.ConditionFalse, observabilityv1alpha1.ReasonTenantNotFound)

	loki := &observabilityv1alpha1.AlertRuleGroup{ObjectMeta: metav1.ObjectMeta{Name: "arg-loki", Namespace: "default"},
		Spec: observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: "arg-tenant", Backend: "loki", Groups: ruleGroups(`{job="x"} |= "e"`)}}
	createAndCleanup(t, loki)
	waitCondition(t, loki, observabilityv1alpha1.ConditionAccepted, metav1.ConditionFalse, observabilityv1alpha1.ReasonBackendNotConfigured)

	bad := &observabilityv1alpha1.AlertRuleGroup{ObjectMeta: metav1.ObjectMeta{Name: "arg-bad", Namespace: "default"},
		Spec: observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: "arg-tenant", Backend: "mimir", Groups: ruleGroups(`up{job=`)}}
	createAndCleanup(t, bad)
	waitCondition(t, bad, observabilityv1alpha1.ConditionAccepted, metav1.ConditionFalse, observabilityv1alpha1.ReasonInvalidRule)
	if c := findCond(bad, observabilityv1alpha1.ConditionAccepted); !strings.Contains(c.Message, "group g rule 0") {
		t.Fatalf("message should name the rule: %q", c.Message)
	}

	good := &observabilityv1alpha1.AlertRuleGroup{ObjectMeta: metav1.ObjectMeta{Name: "arg-good", Namespace: "default"},
		Spec: observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: "arg-tenant", Backend: "mimir", Groups: ruleGroups("up == 0")}}
	createAndCleanup(t, good)
	waitCondition(t, good, observabilityv1alpha1.ConditionAccepted, metav1.ConditionTrue, observabilityv1alpha1.ReasonAccepted)
	if good.Status.BackendNamespace != "alerts-operator/default/arg-good" || good.Status.ObservedGeneration != good.Generation {
		t.Fatalf("status %+v", good.Status)
	}

	// Tenant appearing later flips TenantNotFound → Accepted.
	newFakeTenant(t, "arg-nope", true, false)
	waitCondition(t, missing, observabilityv1alpha1.ConditionAccepted, metav1.ConditionTrue, observabilityv1alpha1.ReasonAccepted)
}

func findCond(obj observabilityv1alpha1.Conditioned, typ string) metav1.Condition {
	for _, c := range obj.GetConditions() {
		if c.Type == typ {
			return c
		}
	}
	return metav1.Condition{}
}
```

- [ ] **Step 3: Run, expect failure**

Run: `make test`
Expected: FAIL (`validateRuleGroups` undefined; conditions never set).

- [ ] **Step 4: Implement controller**

Replace `internal/controller/alertrulegroup_controller.go`:

```go
package controller

import (
	"context"
	"fmt"

	"github.com/prometheus/common/model"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/antnsn/alerts-operator/api/v1alpha1"
	"github.com/antnsn/alerts-operator/internal/compile"
	"github.com/antnsn/alerts-operator/internal/index"
)

// AlertRuleGroupReconciler validates AlertRuleGroups and sets Accepted. It never writes to a backend.
type AlertRuleGroupReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=observability.antnsn.dev,resources=alertrulegroups,verbs=get;list;watch
// +kubebuilder:rbac:groups=observability.antnsn.dev,resources=alertrulegroups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=observability.antnsn.dev,resources=tenants,verbs=get;list;watch

func (r *AlertRuleGroupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var arg v1alpha1.AlertRuleGroup
	if err := r.Get(ctx, req.NamespacedName, &arg); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	status, reason, msg := metav1.ConditionTrue, v1alpha1.ReasonAccepted, ""
	backendNS := ""

	var tenant v1alpha1.Tenant
	switch err := r.Get(ctx, types.NamespacedName{Name: arg.Spec.TenantRef}, &tenant); {
	case errors.IsNotFound(err):
		status, reason, msg = metav1.ConditionFalse, v1alpha1.ReasonTenantNotFound, fmt.Sprintf("Tenant %q not found", arg.Spec.TenantRef)
	case err != nil:
		return ctrl.Result{}, err
	default:
		backendNS = compile.BackendNamespace(tenant.Prefix(), arg.Namespace, arg.Name)
		switch {
		case arg.Spec.Backend == v1alpha1.BackendMimir && tenant.Spec.Mimir == nil,
			arg.Spec.Backend == v1alpha1.BackendLoki && tenant.Spec.Loki == nil:
			status, reason, msg = metav1.ConditionFalse, v1alpha1.ReasonBackendNotConfigured, fmt.Sprintf("Tenant %q has no %s backend", tenant.Name, arg.Spec.Backend)
		default:
			if verr := validateRuleGroups(arg.Spec.Backend, arg.Spec.Groups); verr != nil {
				status, reason, msg = metav1.ConditionFalse, v1alpha1.ReasonInvalidRule, verr.Error()
			}
		}
	}

	changed := false
	err := patchStatus(ctx, r.Client, &arg, func() {
		changed = setCondition(&arg.Status.Conditions, v1alpha1.ConditionAccepted, status, reason, msg, arg.Generation)
		changed = changed || arg.Status.ObservedGeneration != arg.Generation || arg.Status.BackendNamespace != backendNS
		arg.Status.ObservedGeneration = arg.Generation
		arg.Status.BackendNamespace = backendNS
	}, &changed)
	return ctrl.Result{}, err
}

// validateRuleGroups checks durations for all backends and PromQL syntax for Mimir.
// LogQL is validated by Loki itself at sync time.
func validateRuleGroups(be v1alpha1.Backend, groups []v1alpha1.RuleGroup) error {
	for _, g := range groups {
		if g.Interval != "" {
			if _, err := model.ParseDuration(g.Interval); err != nil {
				return fmt.Errorf("group %s: interval: %w", g.Name, err)
			}
		}
		for i, rule := range g.Rules {
			if rule.For != "" {
				if _, err := model.ParseDuration(rule.For); err != nil {
					return fmt.Errorf("group %s rule %d: for: %w", g.Name, i, err)
				}
			}
			if rule.KeepFiringFor != "" {
				if _, err := model.ParseDuration(rule.KeepFiringFor); err != nil {
					return fmt.Errorf("group %s rule %d: keep_firing_for: %w", g.Name, i, err)
				}
			}
			if be == v1alpha1.BackendMimir {
				if err := compile.ValidatePromQL(rule.Expr); err != nil {
					return fmt.Errorf("group %s rule %d: expr: %w", g.Name, i, err)
				}
			}
		}
	}
	return nil
}

// tenantToRuleGroups re-enqueues every AlertRuleGroup that references a changed Tenant.
func (r *AlertRuleGroupReconciler) tenantToRuleGroups(ctx context.Context, o client.Object) []reconcile.Request {
	var list v1alpha1.AlertRuleGroupList
	if err := r.List(ctx, &list, client.MatchingFields{index.IndexTenantRef: o.GetName()}); err != nil {
		return nil
	}
	out := make([]reconcile.Request, 0, len(list.Items))
	for _, item := range list.Items {
		out = append(out, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&item)})
	}
	return out
}

func (r *AlertRuleGroupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.AlertRuleGroup{}).
		Watches(&v1alpha1.Tenant{}, handler.EnqueueRequestsFromMapFunc(r.tenantToRuleGroups)).
		Named("alertrulegroup").
		Complete(r)
}
```

`github.com/prometheus/common` is already an indirect dependency via prometheus; run `go get github.com/prometheus/common@latest && go mod tidy` to make it direct.

In `suite_test.go` `setupReconcilers`:

```go
func setupReconcilers(mgr ctrl.Manager) error {
	if err := (&AlertRuleGroupReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}).SetupWithManager(mgr); err != nil {
		return err
	}
	return nil
}
```

- [ ] **Step 5: Run tests**

Run: `make manifests && make test`
Expected: PASS. `config/rbac/role.yaml` now lists `alertrulegroups`, `alertrulegroups/status`, `tenants`.

- [ ] **Step 6: Commit**

```bash
git add internal/controller config/rbac go.mod go.sum
git commit -m "feat(controller): AlertRuleGroup validation and Accepted condition"
```

---

### Task 16: ContactPoint controller

**Files:**
- Modify: `internal/controller/contactpoint_controller.go`, `internal/controller/alertrulegroup_controller.go` (switch `tenantToRuleGroups` to `requestsFor`), `internal/controller/suite_test.go`
- Test: `internal/controller/contactpoint_controller_test.go`

**Interfaces:**
- Consumes: `ContactPoint.SecretRefs()` (Task 5), `index.IndexSecretRefs`, `index.IndexTenantRef` (Task 13), helpers (Task 14–15).
- Produces:
```go
type ContactPointReconciler struct { client.Client; Scheme *runtime.Scheme }
func (r *ContactPointReconciler) Reconcile(ctx, req) (ctrl.Result, error)
func (r *ContactPointReconciler) SetupWithManager(mgr ctrl.Manager) error
func missingSecretRef(ctx context.Context, c client.Client, namespace string, refs []v1alpha1.SecretKeyRef) (string, error)  // "" when all resolve; else "secret <ns>/<name> key <key> not found"
```

- [ ] **Step 1: Failing envtest test**

`internal/controller/contactpoint_controller_test.go`:

```go
package controller

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	observabilityv1alpha1 "github.com/antnsn/alerts-operator/api/v1alpha1"
)

func TestContactPointAccepted(t *testing.T) {
	newFakeTenant(t, "cp-tenant", true, false)

	missingTenant := &observabilityv1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "cp-missing-tenant", Namespace: "default"},
		Spec: observabilityv1alpha1.ContactPointSpec{TenantRef: "cp-nope", Webhook: []observabilityv1alpha1.WebhookConfig{{URL: "http://x"}}}}
	createAndCleanup(t, missingTenant)
	waitCondition(t, missingTenant, observabilityv1alpha1.ConditionAccepted, metav1.ConditionFalse, observabilityv1alpha1.ReasonTenantNotFound)

	inline := &observabilityv1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "cp-inline", Namespace: "default"},
		Spec: observabilityv1alpha1.ContactPointSpec{TenantRef: "cp-tenant", Webhook: []observabilityv1alpha1.WebhookConfig{{URL: "http://x"}}}}
	createAndCleanup(t, inline)
	waitCondition(t, inline, observabilityv1alpha1.ConditionAccepted, metav1.ConditionTrue, observabilityv1alpha1.ReasonAccepted)

	po := &observabilityv1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "cp-po", Namespace: "default"},
		Spec: observabilityv1alpha1.ContactPointSpec{TenantRef: "cp-tenant", Pushover: []observabilityv1alpha1.PushoverConfig{{
			UserKeySecretRef: observabilityv1alpha1.SecretKeyRef{Name: "cp-po-secret", Key: "user"},
			TokenSecretRef:   observabilityv1alpha1.SecretKeyRef{Name: "cp-po-secret", Key: "token"}}}}}
	createAndCleanup(t, po)
	waitCondition(t, po, observabilityv1alpha1.ConditionAccepted, metav1.ConditionFalse, observabilityv1alpha1.ReasonSecretNotFound)

	// Secret with only one key → still SecretNotFound, message names the key.
	sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "cp-po-secret", Namespace: "default"}, StringData: map[string]string{"user": "U"}}
	createAndCleanup(t, sec)
	waitFor(t, func() bool {
		_ = testClient.Get(testCtx, clientKey(po), po)
		c := findCond(po, observabilityv1alpha1.ConditionAccepted)
		return c.Reason == observabilityv1alpha1.ReasonSecretNotFound && c.Message == "secret default/cp-po-secret key token not found"
	})

	// Adding the missing key flips Accepted to True via the Secret watch.
	if err := testClient.Get(testCtx, clientKey(sec), sec); err != nil {
		t.Fatal(err)
	}
	sec.Data["token"] = []byte("T")
	if err := testClient.Update(testCtx, sec); err != nil {
		t.Fatal(err)
	}
	waitCondition(t, po, observabilityv1alpha1.ConditionAccepted, metav1.ConditionTrue, observabilityv1alpha1.ReasonAccepted)
}
```

Add to `helpers_test.go`:

```go
func clientKey(obj client.Object) client.ObjectKey { return client.ObjectKeyFromObject(obj) }
```

- [ ] **Step 2: Run, expect failure**

Run: `make test`
Expected: FAIL (Accepted never set on ContactPoints).

- [ ] **Step 3: Implement**

Replace `internal/controller/contactpoint_controller.go`:

```go
package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/antnsn/alerts-operator/api/v1alpha1"
	"github.com/antnsn/alerts-operator/internal/index"
)

// ContactPointReconciler validates ContactPoints (tenant + secret refs) and sets Accepted.
type ContactPointReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=observability.antnsn.dev,resources=contactpoints,verbs=get;list;watch
// +kubebuilder:rbac:groups=observability.antnsn.dev,resources=contactpoints/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=observability.antnsn.dev,resources=tenants,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

func (r *ContactPointReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var cp v1alpha1.ContactPoint
	if err := r.Get(ctx, req.NamespacedName, &cp); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	status, reason, msg := metav1.ConditionTrue, v1alpha1.ReasonAccepted, ""
	var tenant v1alpha1.Tenant
	if err := r.Get(ctx, types.NamespacedName{Name: cp.Spec.TenantRef}, &tenant); errors.IsNotFound(err) {
		status, reason, msg = metav1.ConditionFalse, v1alpha1.ReasonTenantNotFound, fmt.Sprintf("Tenant %q not found", cp.Spec.TenantRef)
	} else if err != nil {
		return ctrl.Result{}, err
	} else {
		missing, err := missingSecretRef(ctx, r.Client, cp.Namespace, cp.SecretRefs())
		if err != nil {
			return ctrl.Result{}, err
		}
		if missing != "" {
			status, reason, msg = metav1.ConditionFalse, v1alpha1.ReasonSecretNotFound, missing
		}
	}

	changed := false
	err := patchStatus(ctx, r.Client, &cp, func() {
		changed = setCondition(&cp.Status.Conditions, v1alpha1.ConditionAccepted, status, reason, msg, cp.Generation)
		changed = changed || cp.Status.ObservedGeneration != cp.Generation
		cp.Status.ObservedGeneration = cp.Generation
	}, &changed)
	return ctrl.Result{}, err
}

// missingSecretRef returns a message for the first unresolvable ref, "" if all resolve.
func missingSecretRef(ctx context.Context, c client.Client, namespace string, refs []v1alpha1.SecretKeyRef) (string, error) {
	cache := map[string]*corev1.Secret{}
	for _, ref := range refs {
		sec, ok := cache[ref.Name]
		if !ok {
			sec = &corev1.Secret{}
			err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: ref.Name}, sec)
			if errors.IsNotFound(err) {
				sec = nil
			} else if err != nil {
				return "", err
			}
			cache[ref.Name] = sec
		}
		if sec == nil {
			return fmt.Sprintf("secret %s/%s not found", namespace, ref.Name), nil
		}
		if _, ok := sec.Data[ref.Key]; !ok {
			return fmt.Sprintf("secret %s/%s key %s not found", namespace, ref.Name, ref.Key), nil
		}
	}
	return "", nil
}

func (r *ContactPointReconciler) secretToContactPoints(ctx context.Context, o client.Object) []reconcile.Request {
	var list v1alpha1.ContactPointList
	if err := r.List(ctx, &list, client.InNamespace(o.GetNamespace()), client.MatchingFields{index.IndexSecretRefs: o.GetName()}); err != nil {
		return nil
	}
	return requestsFor(list.Items)
}

func (r *ContactPointReconciler) tenantToContactPoints(ctx context.Context, o client.Object) []reconcile.Request {
	var list v1alpha1.ContactPointList
	if err := r.List(ctx, &list, client.MatchingFields{index.IndexTenantRef: o.GetName()}); err != nil {
		return nil
	}
	return requestsFor(list.Items)
}

func requestsFor[T any, PT interface {
	*T
	client.Object
}](items []T) []reconcile.Request {
	out := make([]reconcile.Request, 0, len(items))
	for i := range items {
		out = append(out, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(PT(&items[i]))})
	}
	return out
}

func (r *ContactPointReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.ContactPoint{}).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.secretToContactPoints)).
		Watches(&v1alpha1.Tenant{}, handler.EnqueueRequestsFromMapFunc(r.tenantToContactPoints)).
		Named("contactpoint").
		Complete(r)
}
```

Refactor Task 15's `tenantToRuleGroups` to use `requestsFor(list.Items)` too (delete its manual loop).

Add to `setupReconcilers` in `suite_test.go`:

```go
	if err := (&ContactPointReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}).SetupWithManager(mgr); err != nil {
		return err
	}
```

- [ ] **Step 4: Run tests**

Run: `make manifests && make test`
Expected: PASS. `config/rbac/role.yaml` contains `secrets` with get/list/watch.

- [ ] **Step 5: Commit**

```bash
git add internal/controller config/rbac
git commit -m "feat(controller): ContactPoint validation with secret watch"
```

---

### Task 17: NotificationPolicy controller

**Files:**
- Modify: `internal/controller/notificationpolicy_controller.go`, `internal/controller/suite_test.go`
- Test: `internal/controller/notificationpolicy_controller_test.go`

**Interfaces:**
- Consumes: `Route.Receivers()` and `Route.ChildRoutes()` (Task 6), `index.IndexTenantRef` (Task 13), helpers (Tasks 14–16).
- Produces:
```go
const maxRouteDepth = 10
type NotificationPolicyReconciler struct { client.Client; Scheme *runtime.Scheme }
func routeDepth(r *v1alpha1.Route) (int, error)                                     // root = 1; propagates ChildRoutes() decode errors
func policyWinner(items []v1alpha1.NotificationPolicy) *v1alpha1.NotificationPolicy // oldest creationTimestamp, tie → lexicographically smallest "<ns>/<name>"; nil for empty
func (r *NotificationPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error
```
`policyWinner` is exported-in-package so the Tenant reconciler (Task 18) uses the same rule. A `Route.Receivers()` or `routeDepth()` decode error (malformed JSON under `spec.route.routes`) sets `Accepted=False reason=Invalid` with the error message.

- [ ] **Step 1: Failing tests**

`internal/controller/notificationpolicy_controller_test.go`:

```go
package controller

import (
	"strings"
	"testing"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	observabilityv1alpha1 "github.com/antnsn/alerts-operator/api/v1alpha1"
)

func TestRouteDepthAndWinner(t *testing.T) {
	r := observabilityv1alpha1.Route{Receiver: "a", Routes: []apiextensionsv1.JSON{{Raw: []byte(`{"receiver":"b","routes":[{"receiver":"c"}]}`)}}}
	d, err := routeDepth(&r)
	if err != nil {
		t.Fatal(err)
	}
	if d != 3 {
		t.Fatalf("depth %d", d)
	}
	now := metav1.Now()
	earlier := metav1.NewTime(now.Add(-time.Hour))
	items := []observabilityv1alpha1.NotificationPolicy{
		{ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "x", CreationTimestamp: now}},
		{ObjectMeta: metav1.ObjectMeta{Name: "z", Namespace: "a", CreationTimestamp: earlier}},
		{ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "a", CreationTimestamp: earlier}},
	}
	if w := policyWinner(items); w == nil || w.Namespace != "a" || w.Name != "c" {
		t.Fatalf("winner %+v", w)
	}
	if policyWinner(nil) != nil {
		t.Fatal("empty → nil")
	}
}

func TestNotificationPolicyAccepted(t *testing.T) {
	newFakeTenant(t, "np-tenant", true, false)
	keep := &observabilityv1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "np-keep", Namespace: "default"},
		Spec: observabilityv1alpha1.ContactPointSpec{TenantRef: "np-tenant", Webhook: []observabilityv1alpha1.WebhookConfig{{URL: "http://keep"}}}}
	createAndCleanup(t, keep)

	pol := &observabilityv1alpha1.NotificationPolicy{ObjectMeta: metav1.ObjectMeta{Name: "np-a", Namespace: "default"},
		Spec: observabilityv1alpha1.NotificationPolicySpec{TenantRef: "np-tenant", Route: observabilityv1alpha1.Route{Receiver: "np-keep",
			Routes: []apiextensionsv1.JSON{{Raw: []byte(`{"receiver":"np-later"}`)}}}}}
	createAndCleanup(t, pol)
	waitCondition(t, pol, observabilityv1alpha1.ConditionAccepted, metav1.ConditionFalse, observabilityv1alpha1.ReasonContactPointNotFound)
	if !strings.Contains(findCond(pol, observabilityv1alpha1.ConditionAccepted).Message, "np-later") {
		t.Fatalf("message: %q", findCond(pol, observabilityv1alpha1.ConditionAccepted).Message)
	}

	// ContactPoint appears → policy flips to Accepted via the ContactPoint watch.
	later := &observabilityv1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "np-later", Namespace: "default"},
		Spec: observabilityv1alpha1.ContactPointSpec{TenantRef: "np-tenant", Webhook: []observabilityv1alpha1.WebhookConfig{{URL: "http://later"}}}}
	createAndCleanup(t, later)
	waitCondition(t, pol, observabilityv1alpha1.ConditionAccepted, metav1.ConditionTrue, observabilityv1alpha1.ReasonAccepted)

	// ContactPoint bound to a different tenant does not count.
	otherTenantCP := &observabilityv1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "np-other", Namespace: "default"},
		Spec: observabilityv1alpha1.ContactPointSpec{TenantRef: "np-tenant-2", Webhook: []observabilityv1alpha1.WebhookConfig{{URL: "http://o"}}}}
	createAndCleanup(t, otherTenantCP)
	wrong := &observabilityv1alpha1.NotificationPolicy{ObjectMeta: metav1.ObjectMeta{Name: "np-wrong", Namespace: "default"},
		Spec: observabilityv1alpha1.NotificationPolicySpec{TenantRef: "np-tenant", Route: observabilityv1alpha1.Route{Receiver: "np-other"}}}
	createAndCleanup(t, wrong)
	waitFor(t, func() bool {
		_ = testClient.Get(testCtx, clientKey(wrong), wrong)
		c := findCond(wrong, observabilityv1alpha1.ConditionAccepted)
		return c.Status == metav1.ConditionFalse && (c.Reason == observabilityv1alpha1.ReasonContactPointNotFound || c.Reason == observabilityv1alpha1.ReasonConflict)
	})

	// Second valid policy for the same tenant → Conflict naming the winner.
	second := &observabilityv1alpha1.NotificationPolicy{ObjectMeta: metav1.ObjectMeta{Name: "np-b", Namespace: "default"},
		Spec: observabilityv1alpha1.NotificationPolicySpec{TenantRef: "np-tenant", Route: observabilityv1alpha1.Route{Receiver: "np-keep"}}}
	createAndCleanup(t, second)
	waitCondition(t, second, observabilityv1alpha1.ConditionAccepted, metav1.ConditionFalse, observabilityv1alpha1.ReasonConflict)
	if !strings.Contains(findCond(second, observabilityv1alpha1.ConditionAccepted).Message, "default/np-a") {
		t.Fatalf("conflict message should name winner: %q", findCond(second, observabilityv1alpha1.ConditionAccepted).Message)
	}
	// Winner stays accepted.
	waitCondition(t, pol, observabilityv1alpha1.ConditionAccepted, metav1.ConditionTrue, observabilityv1alpha1.ReasonAccepted)

	// Deleting the winner promotes the second.
	if err := testClient.Delete(testCtx, pol); err != nil {
		t.Fatal(err)
	}
	waitCondition(t, second, observabilityv1alpha1.ConditionAccepted, metav1.ConditionTrue, observabilityv1alpha1.ReasonAccepted)
}

func TestNotificationPolicyInvalidRouteJSON(t *testing.T) {
	newFakeTenant(t, "np-tenant-3", true, false)
	keep := &observabilityv1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "np-keep3", Namespace: "default"},
		Spec: observabilityv1alpha1.ContactPointSpec{TenantRef: "np-tenant-3", Webhook: []observabilityv1alpha1.WebhookConfig{{URL: "http://keep3"}}}}
	createAndCleanup(t, keep)

	// A malformed child route (routes: [42]) fails ChildRoutes() decoding at reconcile time.
	pol := &observabilityv1alpha1.NotificationPolicy{ObjectMeta: metav1.ObjectMeta{Name: "np-badjson", Namespace: "default"},
		Spec: observabilityv1alpha1.NotificationPolicySpec{TenantRef: "np-tenant-3", Route: observabilityv1alpha1.Route{Receiver: "np-keep3",
			Routes: []apiextensionsv1.JSON{{Raw: []byte("42")}}}}}
	createAndCleanup(t, pol)
	waitCondition(t, pol, observabilityv1alpha1.ConditionAccepted, metav1.ConditionFalse, observabilityv1alpha1.ReasonInvalid)
}
```

- [ ] **Step 2: Run, expect failure**

Run: `make test`
Expected: FAIL.

- [ ] **Step 3: Implement**

Replace `internal/controller/notificationpolicy_controller.go`:

```go
package controller

import (
	"context"
	"fmt"
	"sort"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/antnsn/alerts-operator/api/v1alpha1"
	"github.com/antnsn/alerts-operator/internal/index"
)

const maxRouteDepth = 10

// NotificationPolicyReconciler validates the route tree, receiver references and per-tenant uniqueness.
type NotificationPolicyReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=observability.antnsn.dev,resources=notificationpolicies,verbs=get;list;watch
// +kubebuilder:rbac:groups=observability.antnsn.dev,resources=notificationpolicies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=observability.antnsn.dev,resources=contactpoints,verbs=get;list;watch
// +kubebuilder:rbac:groups=observability.antnsn.dev,resources=tenants,verbs=get;list;watch

func (r *NotificationPolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var pol v1alpha1.NotificationPolicy
	if err := r.Get(ctx, req.NamespacedName, &pol); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	status, reason, msg, err := r.validate(ctx, &pol)
	if err != nil {
		return ctrl.Result{}, err
	}
	changed := false
	err = patchStatus(ctx, r.Client, &pol, func() {
		changed = setCondition(&pol.Status.Conditions, v1alpha1.ConditionAccepted, status, reason, msg, pol.Generation)
		changed = changed || pol.Status.ObservedGeneration != pol.Generation
		pol.Status.ObservedGeneration = pol.Generation
	}, &changed)
	return ctrl.Result{}, err
}

func (r *NotificationPolicyReconciler) validate(ctx context.Context, pol *v1alpha1.NotificationPolicy) (metav1.ConditionStatus, string, string, error) {
	var tenant v1alpha1.Tenant
	if err := r.Get(ctx, types.NamespacedName{Name: pol.Spec.TenantRef}, &tenant); errors.IsNotFound(err) {
		return metav1.ConditionFalse, v1alpha1.ReasonTenantNotFound, fmt.Sprintf("Tenant %q not found", pol.Spec.TenantRef), nil
	} else if err != nil {
		return "", "", "", err
	}
	depth, err := routeDepth(&pol.Spec.Route)
	if err != nil {
		return metav1.ConditionFalse, v1alpha1.ReasonInvalid, err.Error(), nil
	}
	if depth > maxRouteDepth {
		return metav1.ConditionFalse, v1alpha1.ReasonInvalid, fmt.Sprintf("route tree depth %d exceeds %d", depth, maxRouteDepth), nil
	}
	receivers, err := pol.Spec.Route.Receivers()
	if err != nil {
		return metav1.ConditionFalse, v1alpha1.ReasonInvalid, err.Error(), nil
	}
	for _, name := range receivers {
		var cp v1alpha1.ContactPoint
		err := r.Get(ctx, types.NamespacedName{Namespace: pol.Namespace, Name: name}, &cp)
		if errors.IsNotFound(err) || (err == nil && cp.Spec.TenantRef != pol.Spec.TenantRef) {
			return metav1.ConditionFalse, v1alpha1.ReasonContactPointNotFound,
				fmt.Sprintf("receiver %q: no ContactPoint %s/%s for tenant %q", name, pol.Namespace, name, pol.Spec.TenantRef), nil
		}
		if err != nil {
			return "", "", "", err
		}
	}
	var all v1alpha1.NotificationPolicyList
	if err := r.List(ctx, &all, client.MatchingFields{index.IndexTenantRef: pol.Spec.TenantRef}); err != nil {
		return "", "", "", err
	}
	if w := policyWinner(all.Items); w != nil && (w.Namespace != pol.Namespace || w.Name != pol.Name) {
		return metav1.ConditionFalse, v1alpha1.ReasonConflict,
			fmt.Sprintf("Tenant %q already has NotificationPolicy %s/%s (oldest wins)", pol.Spec.TenantRef, w.Namespace, w.Name), nil
	}
	return metav1.ConditionTrue, v1alpha1.ReasonAccepted, "", nil
}

// routeDepth returns the depth of the route tree; a single root is 1. It decodes children via
// ChildRoutes() and returns the first decode error, which the caller treats as Invalid.
func routeDepth(r *v1alpha1.Route) (int, error) {
	children, err := r.ChildRoutes()
	if err != nil {
		return 0, err
	}
	max := 0
	for i := range children {
		d, err := routeDepth(&children[i])
		if err != nil {
			return 0, err
		}
		if d > max {
			max = d
		}
	}
	return max + 1, nil
}

// policyWinner picks the one policy that counts for a tenant: oldest first, then "<ns>/<name>".
func policyWinner(items []v1alpha1.NotificationPolicy) *v1alpha1.NotificationPolicy {
	if len(items) == 0 {
		return nil
	}
	sorted := make([]v1alpha1.NotificationPolicy, len(items))
	copy(sorted, items)
	sort.SliceStable(sorted, func(i, j int) bool {
		ti, tj := sorted[i].CreationTimestamp.Time, sorted[j].CreationTimestamp.Time
		if !ti.Equal(tj) {
			return ti.Before(tj)
		}
		return sorted[i].Namespace+"/"+sorted[i].Name < sorted[j].Namespace+"/"+sorted[j].Name
	})
	return &sorted[0]
}

// policiesOfTenant enqueues all policies sharing a tenant (used for both Tenant and NotificationPolicy events).
func (r *NotificationPolicyReconciler) policiesOfTenant(ctx context.Context, tenantName string) []reconcile.Request {
	var list v1alpha1.NotificationPolicyList
	if err := r.List(ctx, &list, client.MatchingFields{index.IndexTenantRef: tenantName}); err != nil {
		return nil
	}
	return requestsFor(list.Items)
}

func (r *NotificationPolicyReconciler) contactPointToPolicies(ctx context.Context, o client.Object) []reconcile.Request {
	var list v1alpha1.NotificationPolicyList
	if err := r.List(ctx, &list, client.InNamespace(o.GetNamespace())); err != nil {
		return nil
	}
	return requestsFor(list.Items)
}

func (r *NotificationPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.NotificationPolicy{}).
		// Siblings of the same tenant must re-evaluate the winner on any policy change (incl. delete).
		Watches(&v1alpha1.NotificationPolicy{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
			return r.policiesOfTenant(ctx, tenantRefOf(o))
		})).
		Watches(&v1alpha1.ContactPoint{}, handler.EnqueueRequestsFromMapFunc(r.contactPointToPolicies)).
		Watches(&v1alpha1.Tenant{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
			return r.policiesOfTenant(ctx, o.GetName())
		})).
		Named("notificationpolicy").
		Complete(r)
}
```

Add to `setupReconcilers`:

```go
	if err := (&NotificationPolicyReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}).SetupWithManager(mgr); err != nil {
		return err
	}
```

- [ ] **Step 4: Run tests**

Run: `make manifests && make test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/controller config/rbac
git commit -m "feat(controller): NotificationPolicy validation, receiver refs and per-tenant uniqueness"
```

---

### Task 18: Tenant controller — skeleton, children listing, watches, finalizer registration

**Files:**
- Modify: `internal/controller/tenant_controller.go` (scaffolded), `internal/controller/suite_test.go`, `cmd/main.go`
- Test: `internal/controller/tenant_controller_test.go`

**Interfaces:**
- Consumes: `backend.Options`, `backend.RuleStore`, `backend.AlertmanagerStore`, `backend.BasicAuth` (Task 7); `mimir.New` (Task 9); `loki.New` (Task 10); `compile.BackendNamespace` (Task 11); `index.IndexTenantRef` (Task 13); `policyWinner` (Task 17); `setCondition`, `patchStatus`, `mapToTenant` (Task 14); `tenant.Prefix()`, `tenant.Resync()` (Task 3).
- Produces:
```go
const tenantFinalizer = "observability.antnsn.dev/tenant"
type MimirClient interface { backend.RuleStore; backend.AlertmanagerStore }
type TenantReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	NewMimir func(backend.Options) MimirClient        // nil → mimir.New
	NewLoki  func(backend.Options) backend.RuleStore  // nil → loki.New
	mu         sync.Mutex
	lastAMSync map[string]time.Time                    // tenant name → last successful Alertmanager sync
}
type children struct {
	ContactPoints  []v1alpha1.ContactPoint
	Policy         *v1alpha1.NotificationPolicy
	MimirGroups    []v1alpha1.AlertRuleGroup
	LokiGroups     []v1alpha1.AlertRuleGroup
	KeepNamespaces map[string]bool // backend namespaces of stale-generation AlertRuleGroups: not written, not pruned, this pass
	PendingAM      bool            // an accepted-for-this-tenant ContactPoint or NotificationPolicy is stale-generation
}
// listChildren splits accepted-current children by kind/backend. A child (any of the three kinds)
// whose Accepted condition is missing or stale (ObservedGeneration < Generation) is neither desired
// nor excluded: its AlertRuleGroup namespace lands in KeepNamespaces and, for a ContactPoint or
// NotificationPolicy, PendingAM is set. Accepted=False at the current generation is a validated
// rejection and stays excluded/prunable, same as before.
func (r *TenantReconciler) listChildren(ctx context.Context, tenant *v1alpha1.Tenant) (*children, error)
func acceptedCurrent(obj v1alpha1.Conditioned) bool
func staleGeneration(obj v1alpha1.Conditioned) bool  // Accepted condition missing, or ObservedGeneration < Generation
func (r *TenantReconciler) backendOptions(ctx context.Context, tenant *v1alpha1.Tenant, spec *v1alpha1.BackendSpec) (backend.Options, error)
func (r *TenantReconciler) mimirClient(ctx, tenant) (MimirClient, error)
func (r *TenantReconciler) lokiClient(ctx, tenant) (backend.RuleStore, error)
// Contract for the sync functions (bodies in Task 19; stubs here):
//   they set the relevant tenant.Status condition in memory and return a non-nil error ONLY
//   for backend.IsUnavailable errors (caller returns it to controller-runtime → backoff).
func (r *TenantReconciler) syncRules(ctx context.Context, tenant *v1alpha1.Tenant, store backend.RuleStore, be v1alpha1.Backend, groups []v1alpha1.AlertRuleGroup, keepNamespaces map[string]bool) (int32, error)
func (r *TenantReconciler) syncAlertmanager(ctx context.Context, tenant *v1alpha1.Tenant, store backend.AlertmanagerStore, ch *children) error
func (r *TenantReconciler) finalize(ctx context.Context, tenant *v1alpha1.Tenant) error  // body in Task 20
```

- [ ] **Step 1: Failing envtest test**

`internal/controller/tenant_controller_test.go`:

```go
package controller

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	observabilityv1alpha1 "github.com/antnsn/alerts-operator/api/v1alpha1"
)

func TestTenantReadyWithoutChildren(t *testing.T) {
	// Loki-only: no Alertmanager, so Ready does not depend on a NotificationPolicy.
	tn, _ := newFakeTenant(t, "tn-loki-only", false, true)
	waitCondition(t, tn, observabilityv1alpha1.ConditionReady, metav1.ConditionTrue, "")
	if !controllerutil.ContainsFinalizer(tn, tenantFinalizer) {
		t.Fatalf("finalizer missing: %v", tn.Finalizers)
	}
	if tn.Status.ObservedGeneration != tn.Generation {
		t.Fatalf("observedGeneration %d != %d", tn.Status.ObservedGeneration, tn.Generation)
	}
	mimirCond := findCond(tn, observabilityv1alpha1.ConditionMimirRulesSynced)
	if mimirCond.Status != metav1.ConditionUnknown || mimirCond.Reason != observabilityv1alpha1.ReasonNotConfigured {
		t.Fatalf("unconfigured backend should be Unknown/NotConfigured: %+v", mimirCond)
	}
	amCond := findCond(tn, observabilityv1alpha1.ConditionAlertmanagerSynced)
	if amCond.Status != metav1.ConditionUnknown || amCond.Reason != observabilityv1alpha1.ReasonNotConfigured {
		t.Fatalf("unconfigured alertmanager should be Unknown/NotConfigured: %+v", amCond)
	}
}

func TestTenantListChildrenFiltersAccepted(t *testing.T) {
	tn, _ := newFakeTenant(t, "tn-children", true, true)
	good := &observabilityv1alpha1.AlertRuleGroup{ObjectMeta: metav1.ObjectMeta{Name: "tn-good", Namespace: "default"},
		Spec: observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: tn.Name, Backend: "mimir", Groups: ruleGroups("up == 0")}}
	bad := &observabilityv1alpha1.AlertRuleGroup{ObjectMeta: metav1.ObjectMeta{Name: "tn-bad", Namespace: "default"},
		Spec: observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: tn.Name, Backend: "mimir", Groups: ruleGroups("up{")}}
	lokiG := &observabilityv1alpha1.AlertRuleGroup{ObjectMeta: metav1.ObjectMeta{Name: "tn-loki", Namespace: "default"},
		Spec: observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: tn.Name, Backend: "loki", Groups: ruleGroups(`{a="b"}`)}}
	cp := &observabilityv1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "tn-cp", Namespace: "default"},
		Spec: observabilityv1alpha1.ContactPointSpec{TenantRef: tn.Name, Webhook: []observabilityv1alpha1.WebhookConfig{{URL: "http://x"}}}}
	pol := &observabilityv1alpha1.NotificationPolicy{ObjectMeta: metav1.ObjectMeta{Name: "tn-pol", Namespace: "default"},
		Spec: observabilityv1alpha1.NotificationPolicySpec{TenantRef: tn.Name, Route: observabilityv1alpha1.Route{Receiver: "tn-cp"}}}
	for _, o := range []observabilityv1alpha1.Conditioned{good, bad, lokiG, cp, pol} {
		createAndCleanup(t, o)
	}
	waitCondition(t, good, observabilityv1alpha1.ConditionAccepted, metav1.ConditionTrue, "")
	waitCondition(t, bad, observabilityv1alpha1.ConditionAccepted, metav1.ConditionFalse, "")
	waitCondition(t, lokiG, observabilityv1alpha1.ConditionAccepted, metav1.ConditionTrue, "")
	waitCondition(t, pol, observabilityv1alpha1.ConditionAccepted, metav1.ConditionTrue, "")

	r := &TenantReconciler{Client: testClient}
	ch, err := r.listChildren(testCtx, tn)
	if err != nil {
		t.Fatal(err)
	}
	if len(ch.MimirGroups) != 1 || ch.MimirGroups[0].Name != "tn-good" {
		t.Fatalf("mimir groups: %+v", ch.MimirGroups)
	}
	if len(ch.LokiGroups) != 1 || len(ch.ContactPoints) != 1 || ch.Policy == nil || ch.Policy.Name != "tn-pol" {
		t.Fatalf("children: %+v", ch)
	}
}
```

- [ ] **Step 2: Run, expect failure**

Run: `make test`
Expected: FAIL (`tenantFinalizer`, `listChildren` undefined; Ready never set).

- [ ] **Step 3: Implement**

Replace `internal/controller/tenant_controller.go`:

```go
package controller

import (
	"context"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/antnsn/alerts-operator/api/v1alpha1"
	"github.com/antnsn/alerts-operator/internal/backend"
	"github.com/antnsn/alerts-operator/internal/backend/loki"
	"github.com/antnsn/alerts-operator/internal/backend/mimir"
	"github.com/antnsn/alerts-operator/internal/compile"
	"github.com/antnsn/alerts-operator/internal/index"
)

const tenantFinalizer = "observability.antnsn.dev/tenant"

// MimirClient is what the Tenant reconciler needs from Mimir.
type MimirClient interface {
	backend.RuleStore
	backend.AlertmanagerStore
}

// TenantReconciler is the single writer to Mimir and Loki.
type TenantReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	NewMimir func(backend.Options) MimirClient
	NewLoki  func(backend.Options) backend.RuleStore

	mu         sync.Mutex
	lastAMSync map[string]time.Time
}

// children are the Accepted CRs referencing one Tenant, plus what's known about children that are
// accepted for a stale generation (see staleGeneration): their backend state must survive this pass
// untouched rather than being treated as no-longer-desired and pruned.
type children struct {
	ContactPoints []v1alpha1.ContactPoint
	Policy        *v1alpha1.NotificationPolicy
	MimirGroups   []v1alpha1.AlertRuleGroup
	LokiGroups    []v1alpha1.AlertRuleGroup

	KeepNamespaces map[string]bool // backend namespaces of stale-generation AlertRuleGroups
	PendingAM      bool            // a ContactPoint or NotificationPolicy referencing this tenant is stale-generation
}

// +kubebuilder:rbac:groups=observability.antnsn.dev,resources=tenants,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=observability.antnsn.dev,resources=tenants/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=observability.antnsn.dev,resources=tenants/finalizers,verbs=update
// +kubebuilder:rbac:groups=observability.antnsn.dev,resources=contactpoints;notificationpolicies;alertrulegroups,verbs=get;list;watch
// +kubebuilder:rbac:groups=observability.antnsn.dev,resources=contactpoints/status;notificationpolicies/status;alertrulegroups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=secrets;configmaps,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

func (r *TenantReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var tenant v1alpha1.Tenant
	if err := r.Get(ctx, req.NamespacedName, &tenant); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !tenant.DeletionTimestamp.IsZero() {
		if !controllerutil.ContainsFinalizer(&tenant, tenantFinalizer) {
			return ctrl.Result{}, nil
		}
		if err := r.finalize(ctx, &tenant); err != nil {
			deletingChanged := false
			_ = patchStatus(ctx, r.Client, &tenant, func() {
				deletingChanged = setCondition(&tenant.Status.Conditions, v1alpha1.ConditionReady, metav1.ConditionFalse, v1alpha1.ReasonDeleting, err.Error(), tenant.Generation)
			}, &deletingChanged)
			r.Recorder.Eventf(&tenant, corev1.EventTypeWarning, "FinalizeFailed", "%v", err)
			return ctrl.Result{}, err
		}
		controllerutil.RemoveFinalizer(&tenant, tenantFinalizer)
		return ctrl.Result{}, r.Update(ctx, &tenant)
	}

	if controllerutil.AddFinalizer(&tenant, tenantFinalizer) {
		return ctrl.Result{}, r.Update(ctx, &tenant) // Update triggers a new reconcile.
	}

	// Snapshot before the sync functions mutate tenant.Status in memory; the final
	// status patch is the diff against this snapshot.
	base := tenant.DeepCopy()

	ch, err := r.listChildren(ctx, &tenant)
	if err != nil {
		return ctrl.Result{}, err
	}

	var firstErr error
	keep := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	gen := tenant.Generation

	if tenant.Spec.Mimir != nil {
		mc, err := r.mimirClient(ctx, &tenant)
		if err != nil {
			setCondition(&tenant.Status.Conditions, v1alpha1.ConditionMimirRulesSynced, metav1.ConditionFalse, v1alpha1.ReasonInvalid, err.Error(), gen)
			setCondition(&tenant.Status.Conditions, v1alpha1.ConditionAlertmanagerSynced, metav1.ConditionFalse, v1alpha1.ReasonInvalid, err.Error(), gen)
		} else {
			n, err := r.syncRules(ctx, &tenant, mc, v1alpha1.BackendMimir, ch.MimirGroups, ch.KeepNamespaces)
			keep(err)
			tenant.Status.RuleGroups.Mimir = n
			keep(r.syncAlertmanager(ctx, &tenant, mc, ch))
		}
	} else {
		setCondition(&tenant.Status.Conditions, v1alpha1.ConditionMimirRulesSynced, metav1.ConditionUnknown, v1alpha1.ReasonNotConfigured, "spec.mimir not set", gen)
		setCondition(&tenant.Status.Conditions, v1alpha1.ConditionAlertmanagerSynced, metav1.ConditionUnknown, v1alpha1.ReasonNotConfigured, "spec.mimir not set", gen)
		tenant.Status.RuleGroups.Mimir = 0
	}

	if tenant.Spec.Loki != nil {
		lc, err := r.lokiClient(ctx, &tenant)
		if err != nil {
			setCondition(&tenant.Status.Conditions, v1alpha1.ConditionLokiRulesSynced, metav1.ConditionFalse, v1alpha1.ReasonInvalid, err.Error(), gen)
		} else {
			n, err := r.syncRules(ctx, &tenant, lc, v1alpha1.BackendLoki, ch.LokiGroups, ch.KeepNamespaces)
			keep(err)
			tenant.Status.RuleGroups.Loki = n
		}
	} else {
		setCondition(&tenant.Status.Conditions, v1alpha1.ConditionLokiRulesSynced, metav1.ConditionUnknown, v1alpha1.ReasonNotConfigured, "spec.loki not set", gen)
		tenant.Status.RuleGroups.Loki = 0
	}

	// Ready = every configured target condition is True. A False condition always wins. An Unknown
	// condition with ReasonNotConfigured is a backend the tenant doesn't use and never affects
	// Ready. An Unknown condition with ReasonPending (syncAlertmanager, when ch.PendingAM: a
	// ContactPoint/NotificationPolicy edit that hasn't been re-validated yet) means this pass has no
	// new information, so Ready keeps whatever value it already had rather than jumping to True.
	readyStatus, readyReason, readyMsg := metav1.ConditionTrue, v1alpha1.ReasonSynced, ""
	pending := false
	for _, typ := range []string{v1alpha1.ConditionAlertmanagerSynced, v1alpha1.ConditionMimirRulesSynced, v1alpha1.ConditionLokiRulesSynced} {
		c := meta.FindStatusCondition(tenant.Status.Conditions, typ)
		if c == nil || c.Status == metav1.ConditionUnknown {
			if c != nil && c.Reason == v1alpha1.ReasonPending {
				pending = true
			}
			continue
		}
		if c.Status == metav1.ConditionFalse {
			readyStatus, readyReason, readyMsg = metav1.ConditionFalse, c.Reason, typ+": "+c.Message
			pending = false
			break
		}
	}
	if pending {
		if prev := meta.FindStatusCondition(base.Status.Conditions, v1alpha1.ConditionReady); prev != nil {
			readyStatus, readyReason, readyMsg = prev.Status, prev.Reason, prev.Message
		}
	}
	setCondition(&tenant.Status.Conditions, v1alpha1.ConditionReady, readyStatus, readyReason, readyMsg, gen)

	tenant.Status.ObservedGeneration = gen
	// Only patch when status really changed (see patchStatus for why an unconditional
	// optimistic-lock patch would loop).
	if !equality.Semantic.DeepEqual(base.Status, tenant.Status) {
		if err := r.Status().Patch(ctx, &tenant, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
			return ctrl.Result{}, err
		}
	}
	if firstErr != nil {
		return ctrl.Result{}, firstErr
	}
	// A pending child or a kept-but-not-yet-revalidated namespace means this Tenant is sitting on
	// stale exclusions; the child's own status patch also re-enqueues via the watch, but this is a
	// safety net so we don't wait a full resync interval to pick the fix up.
	requeue := tenant.Resync()
	if ch.PendingAM || len(ch.KeepNamespaces) > 0 {
		requeue = 5 * time.Second
	}
	return ctrl.Result{RequeueAfter: requeue}, nil
}

// listChildren returns the Accepted children of a tenant, split by kind and backend, plus what's
// pending: children (any of the three kinds) whose Accepted condition is stale — missing, or
// ObservedGeneration < Generation — because a spec edit bumped Generation and this child's own
// reconciler hasn't run yet. Those are neither desired (they're not in the returned slices) nor
// prunable (an AlertRuleGroup's namespace lands in KeepNamespaces; a stale ContactPoint or
// NotificationPolicy sets PendingAM). An object with Accepted=False at its current generation is a
// validated rejection, not pending: it's simply excluded, same as before.
func (r *TenantReconciler) listChildren(ctx context.Context, tenant *v1alpha1.Tenant) (*children, error) {
	sel := client.MatchingFields{index.IndexTenantRef: tenant.Name}
	ch := &children{KeepNamespaces: map[string]bool{}}

	var cps v1alpha1.ContactPointList
	if err := r.List(ctx, &cps, sel); err != nil {
		return nil, err
	}
	for _, cp := range cps.Items {
		switch {
		case acceptedCurrent(&cp):
			ch.ContactPoints = append(ch.ContactPoints, cp)
		case staleGeneration(&cp):
			ch.PendingAM = true
		}
	}

	var pols v1alpha1.NotificationPolicyList
	if err := r.List(ctx, &pols, sel); err != nil {
		return nil, err
	}
	var accepted []v1alpha1.NotificationPolicy
	for _, p := range pols.Items {
		switch {
		case acceptedCurrent(&p):
			accepted = append(accepted, p)
		case staleGeneration(&p):
			ch.PendingAM = true
		}
	}
	ch.Policy = policyWinner(accepted)

	var args v1alpha1.AlertRuleGroupList
	if err := r.List(ctx, &args, sel); err != nil {
		return nil, err
	}
	for _, a := range args.Items {
		switch {
		case acceptedCurrent(&a):
			switch a.Spec.Backend {
			case v1alpha1.BackendMimir:
				ch.MimirGroups = append(ch.MimirGroups, a)
			case v1alpha1.BackendLoki:
				ch.LokiGroups = append(ch.LokiGroups, a)
			}
		case staleGeneration(&a):
			ch.KeepNamespaces[compile.BackendNamespace(tenant.Prefix(), a.Namespace, a.Name)] = true
		}
	}
	return ch, nil
}

// acceptedCurrent is true only when Accepted=True was set for the object's current generation.
// A freshly edited spec still carries the previous generation's Accepted=True until its own
// reconciler runs; without this check the Tenant could push an unvalidated spec to the backend.
func acceptedCurrent(obj v1alpha1.Conditioned) bool {
	c := meta.FindStatusCondition(obj.GetConditions(), v1alpha1.ConditionAccepted)
	return c != nil && c.Status == metav1.ConditionTrue && c.ObservedGeneration == obj.GetGeneration()
}

// staleGeneration is true when obj's Accepted condition has not yet been evaluated for its current
// generation: missing, or ObservedGeneration < Generation. That's the window right after a spec
// edit, before the child's own reconciler runs — not the same as Accepted=False at the current
// generation, which is a validated rejection and must stay excluded/prunable.
func staleGeneration(obj v1alpha1.Conditioned) bool {
	c := meta.FindStatusCondition(obj.GetConditions(), v1alpha1.ConditionAccepted)
	return c == nil || c.ObservedGeneration < obj.GetGeneration()
}

// backendOptions builds client options, resolving basic auth from a Secret with keys username/password.
func (r *TenantReconciler) backendOptions(ctx context.Context, tenant *v1alpha1.Tenant, spec *v1alpha1.BackendSpec) (backend.Options, error) {
	o := backend.Options{Address: spec.Address, TenantID: tenant.Spec.TenantID}
	if spec.Auth != nil && spec.Auth.BasicAuthSecretRef != nil {
		ref := spec.Auth.BasicAuthSecretRef
		var sec corev1.Secret
		if err := r.Get(ctx, types.NamespacedName{Namespace: ref.Namespace, Name: ref.Name}, &sec); err != nil {
			if errors.IsNotFound(err) {
				return o, fmt.Errorf("basic auth secret %s not found", ref)
			}
			return o, err
		}
		u, uok := sec.Data["username"]
		p, pok := sec.Data["password"]
		if !uok || !pok {
			return o, fmt.Errorf("basic auth secret %s must have keys username and password", ref)
		}
		o.BasicAuth = &backend.BasicAuth{Username: string(u), Password: string(p)}
	}
	return o, nil
}

func (r *TenantReconciler) mimirClient(ctx context.Context, tenant *v1alpha1.Tenant) (MimirClient, error) {
	o, err := r.backendOptions(ctx, tenant, tenant.Spec.Mimir)
	if err != nil {
		return nil, err
	}
	if r.NewMimir != nil {
		return r.NewMimir(o), nil
	}
	return mimir.New(o), nil
}

func (r *TenantReconciler) lokiClient(ctx context.Context, tenant *v1alpha1.Tenant) (backend.RuleStore, error) {
	o, err := r.backendOptions(ctx, tenant, tenant.Spec.Loki)
	if err != nil {
		return nil, err
	}
	if r.NewLoki != nil {
		return r.NewLoki(o), nil
	}
	return loki.New(o), nil
}

// --- stubs replaced in Tasks 19 and 20 ---

func (r *TenantReconciler) syncRules(_ context.Context, tenant *v1alpha1.Tenant, _ backend.RuleStore, be v1alpha1.Backend, groups []v1alpha1.AlertRuleGroup, _ map[string]bool) (int32, error) {
	typ := v1alpha1.ConditionMimirRulesSynced
	if be == v1alpha1.BackendLoki {
		typ = v1alpha1.ConditionLokiRulesSynced
	}
	setCondition(&tenant.Status.Conditions, typ, metav1.ConditionTrue, v1alpha1.ReasonSynced, "", tenant.Generation)
	return int32(len(groups)), nil
}

func (r *TenantReconciler) syncAlertmanager(_ context.Context, tenant *v1alpha1.Tenant, _ backend.AlertmanagerStore, _ *children) error {
	setCondition(&tenant.Status.Conditions, v1alpha1.ConditionAlertmanagerSynced, metav1.ConditionTrue, v1alpha1.ReasonSynced, "", tenant.Generation)
	return nil
}

func (r *TenantReconciler) finalize(_ context.Context, _ *v1alpha1.Tenant) error { return nil }

// --- watches ---

// secretToTenants maps a Secret to tenants using it for basic auth and to tenants of ContactPoints referencing it.
func (r *TenantReconciler) secretToTenants(ctx context.Context, o client.Object) []reconcile.Request {
	seen := map[string]bool{}
	var out []reconcile.Request
	add := func(name string) {
		if name != "" && !seen[name] {
			seen[name] = true
			out = append(out, tenantRequest(name))
		}
	}
	var tenants v1alpha1.TenantList
	if err := r.List(ctx, &tenants); err == nil {
		for _, t := range tenants.Items {
			for _, b := range []*v1alpha1.BackendSpec{t.Spec.Mimir, t.Spec.Loki} {
				if b != nil && b.Auth != nil && b.Auth.BasicAuthSecretRef != nil &&
					b.Auth.BasicAuthSecretRef.Namespace == o.GetNamespace() && b.Auth.BasicAuthSecretRef.Name == o.GetName() {
					add(t.Name)
				}
			}
		}
	}
	var cps v1alpha1.ContactPointList
	if err := r.List(ctx, &cps, client.InNamespace(o.GetNamespace()), client.MatchingFields{index.IndexSecretRefs: o.GetName()}); err == nil {
		for _, cp := range cps.Items {
			add(cp.Spec.TenantRef)
		}
	}
	return out
}

// configMapToTenants maps a ConfigMap to tenants whose alertmanager.templatesRef points at it.
func (r *TenantReconciler) configMapToTenants(ctx context.Context, o client.Object) []reconcile.Request {
	var tenants v1alpha1.TenantList
	if err := r.List(ctx, &tenants); err != nil {
		return nil
	}
	var out []reconcile.Request
	for _, t := range tenants.Items {
		ref := t.Spec.Alertmanager
		if ref != nil && ref.TemplatesRef != nil && ref.TemplatesRef.Namespace == o.GetNamespace() && ref.TemplatesRef.Name == o.GetName() {
			out = append(out, tenantRequest(t.Name))
		}
	}
	return out
}

func (r *TenantReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorderFor("tenant-controller")
	}
	r.lastAMSync = map[string]time.Time{}
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.Tenant{}).
		Watches(&v1alpha1.ContactPoint{}, handler.EnqueueRequestsFromMapFunc(mapToTenant)).
		Watches(&v1alpha1.NotificationPolicy{}, handler.EnqueueRequestsFromMapFunc(mapToTenant)).
		Watches(&v1alpha1.AlertRuleGroup{}, handler.EnqueueRequestsFromMapFunc(mapToTenant)).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.secretToTenants)).
		Watches(&corev1.ConfigMap{}, handler.EnqueueRequestsFromMapFunc(r.configMapToTenants)).
		Named("tenant").
		Complete(r)
}
```

`suite_test.go` `setupReconcilers`, add:

```go
	if err := (&TenantReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}).SetupWithManager(mgr); err != nil {
		return err
	}
```

`cmd/main.go`: the scaffolded `TenantReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}` block stays; `SetupWithManager` fills `Recorder` and the client constructors default to the real ones.

Now that a Tenant gets a finalizer, the Task 3 CEL test must not leave Tenants pointing at dead URLs. Edit `internal/controller/tenant_api_test.go` `TestTenantCEL`: replace the two literal addresses with a fake server and wait for deletion:

```go
	srv := fake.New()
	defer srv.Close()
	// in the cases table use srv.URL instead of "http://m" and "http://l"
	...
			if err == nil {
				_ = testClient.Delete(testCtx, obj)
				waitFor(t, func() bool {
					return errors.IsNotFound(testClient.Get(testCtx, client.ObjectKeyFromObject(obj), &observabilityv1alpha1.Tenant{}))
				})
			}
```
with imports `"k8s.io/apimachinery/pkg/api/errors"`, `"sigs.k8s.io/controller-runtime/pkg/client"`, `"github.com/antnsn/alerts-operator/internal/backend/fake"`. Build the `cases` slice after `srv` exists.

- [ ] **Step 4: Run tests**

Run: `make manifests && make test`
Expected: PASS. `config/rbac/role.yaml` includes `tenants/finalizers`, `configmaps`, `events`.

- [ ] **Step 5: Commit**

```bash
git add internal/controller config/rbac cmd/main.go
git commit -m "feat(controller): Tenant reconciler skeleton with children listing, watches and finalizer"
```

---

### Task 19: Tenant sync — rules and Alertmanager

**Files:**
- Create: `internal/controller/tenant_rules.go`, `internal/controller/tenant_alertmanager.go`
- Modify: `internal/controller/tenant_controller.go` (delete the `syncRules`/`syncAlertmanager` stubs)
- Test: `internal/controller/tenant_sync_test.go`

**Interfaces:**
- Consumes: `compile.Rules`, `compile.RulesEqual`, `compile.BackendNamespace` (Task 11); `compile.Alertmanager`, `compile.AlertmanagerInput`, `compile.SecretResolver`, `compile.AttributedError`, `compile.HashAlertmanager` (Task 12); `backend.RuleStore`, `backend.AlertmanagerStore`, `backend.IsUnavailable`, `backend.IsRejected` (Task 7); `syncedFromErr`, `patchStatus`, `setCondition` (Task 14); `children`, `TenantReconciler.lastAMSync` (Task 18).
- Produces (same signatures as the Task 18 stubs):
```go
func (r *TenantReconciler) syncRules(ctx, tenant *v1alpha1.Tenant, store backend.RuleStore, be v1alpha1.Backend, groups []v1alpha1.AlertRuleGroup, keepNamespaces map[string]bool) (int32, error)  // keepNamespaces: skip in the diff loop, exclude from the prune loop
func (r *TenantReconciler) syncAlertmanager(ctx, tenant *v1alpha1.Tenant, store backend.AlertmanagerStore, ch *children) error  // if ch.PendingAM: AlertmanagerSynced=Unknown/Pending, no compile, no POST
func (r *TenantReconciler) secretResolver(ctx context.Context) compile.SecretResolver
func (r *TenantReconciler) loadTemplates(ctx context.Context, tenant *v1alpha1.Tenant) (map[string]string, error)
func (r *TenantReconciler) setChildSynced(ctx context.Context, obj v1alpha1.Conditioned, err error)  // patches Synced on a child; logs but does not fail on patch error
func worstErr(errs []error) error   // first IsUnavailable error, else first error, else nil
```

- [ ] **Step 1: Failing envtest tests**

`internal/controller/tenant_sync_test.go`:

```go
package controller

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"

	observabilityv1alpha1 "github.com/antnsn/alerts-operator/api/v1alpha1"
	"github.com/antnsn/alerts-operator/internal/backend"
)

func countPrefix(reqs []string, prefix string) int {
	n := 0
	for _, r := range reqs {
		if strings.HasPrefix(r, prefix) {
			n++
		}
	}
	return n
}

func TestTenantSyncsRulesAndAlertmanager(t *testing.T) {
	tn, srv := newFakeTenant(t, "sync-tenant", true, true)
	// Foreign state that must survive: a namespace outside our prefix.
	srv.SetRules("1", "other/x", []backend.RuleGroup{{Name: "keep-me", Rules: []backend.Rule{{Alert: "K", Expr: "up"}}}})

	mimirG := &observabilityv1alpha1.AlertRuleGroup{ObjectMeta: metav1.ObjectMeta{Name: "sync-m", Namespace: "default"},
		Spec: observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: tn.Name, Backend: "mimir", Groups: []observabilityv1alpha1.RuleGroup{
			{Name: "g1", Rules: []observabilityv1alpha1.Rule{{Alert: "A", Expr: "up == 0"}}},
			{Name: "g2", Rules: []observabilityv1alpha1.Rule{{Record: "r", Expr: "avg(up)"}}},
		}}}
	lokiG := &observabilityv1alpha1.AlertRuleGroup{ObjectMeta: metav1.ObjectMeta{Name: "sync-l", Namespace: "default"},
		Spec: observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: tn.Name, Backend: "loki", Groups: ruleGroups(`sum(rate({job="x"} |= "e" [5m])) > 0`)}}
	sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "sync-po", Namespace: "default"}, StringData: map[string]string{"user": "U", "token": "T"}}
	keep := &observabilityv1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "keep", Namespace: "default"},
		Spec: observabilityv1alpha1.ContactPointSpec{TenantRef: tn.Name, Webhook: []observabilityv1alpha1.WebhookConfig{{URL: "http://keep"}}}}
	po := &observabilityv1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "pushover", Namespace: "default"},
		Spec: observabilityv1alpha1.ContactPointSpec{TenantRef: tn.Name, Pushover: []observabilityv1alpha1.PushoverConfig{{
			UserKeySecretRef: observabilityv1alpha1.SecretKeyRef{Name: "sync-po", Key: "user"},
			TokenSecretRef:   observabilityv1alpha1.SecretKeyRef{Name: "sync-po", Key: "token"}}}}}
	pol := &observabilityv1alpha1.NotificationPolicy{ObjectMeta: metav1.ObjectMeta{Name: "sync-pol", Namespace: "default"},
		Spec: observabilityv1alpha1.NotificationPolicySpec{TenantRef: tn.Name, Route: observabilityv1alpha1.Route{Receiver: "keep",
			Routes: []observabilityv1alpha1.Route{{Receiver: "pushover", Matchers: []string{`severity="critical"`}}}}}}
	createAndCleanup(t, sec)
	for _, o := range []observabilityv1alpha1.Conditioned{mimirG, lokiG, keep, po, pol} {
		createAndCleanup(t, o)
	}

	waitCondition(t, tn, observabilityv1alpha1.ConditionReady, metav1.ConditionTrue, "")
	rules, lokiRules := srv.Rules("1"), srv.LokiRules("1")
	if len(rules["alerts-operator/default/sync-m"]) != 2 || len(lokiRules["alerts-operator/default/sync-l"]) != 1 || len(rules["other/x"]) != 1 {
		t.Fatalf("rules in backend: mimir=%+v loki=%+v", rules, lokiRules)
	}
	if _, leaked := rules["alerts-operator/default/sync-l"]; leaked {
		t.Fatal("loki group must not be written to mimir")
	}
	am := srv.Alertmanager("1")
	if am == nil || !strings.Contains(am.Config, "receiver: default/keep") || !strings.Contains(am.Config, "- name: default/pushover") || !strings.Contains(am.Config, "user_key: U") {
		t.Fatalf("alertmanager config: %+v", am)
	}
	if tn.Status.RuleGroups.Mimir != 2 || tn.Status.RuleGroups.Loki != 1 || !strings.HasPrefix(tn.Status.AlertmanagerConfigHash, "sha256:") {
		t.Fatalf("status: %+v", tn.Status)
	}
	for _, o := range []observabilityv1alpha1.Conditioned{mimirG, lokiG, keep, po, pol} {
		waitCondition(t, o, observabilityv1alpha1.ConditionSynced, metav1.ConditionTrue, observabilityv1alpha1.ReasonSynced)
	}

	// No-op reconcile: metadata-only change on a child must not POST anything.
	srv.ResetRequests()
	if err := testClient.Get(testCtx, clientKey(mimirG), mimirG); err != nil {
		t.Fatal(err)
	}
	mimirG.Labels = map[string]string{"touch": "1"}
	if err := testClient.Update(testCtx, mimirG); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return countPrefix(srv.Requests(), "GET ") >= 2 })
	if n := countPrefix(srv.Requests(), "POST "); n != 0 {
		t.Fatalf("expected no POST on no-op, got %d: %v", n, srv.Requests())
	}

	// Change one group → exactly one POST, to that namespace.
	srv.ResetRequests()
	if err := testClient.Get(testCtx, clientKey(mimirG), mimirG); err != nil {
		t.Fatal(err)
	}
	mimirG.Spec.Groups[0].Rules[0].Expr = "up == 1"
	if err := testClient.Update(testCtx, mimirG); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		return srv.Rules("1")["alerts-operator/default/sync-m"][0].Rules[0].Expr == "up == 1"
	})
	if n := countPrefix(srv.Requests(), "POST /prometheus/config/v1/rules/alerts-operator%2Fdefault%2Fsync-m"); n != 1 {
		t.Fatalf("expected exactly one rules POST, got %d: %v", n, srv.Requests())
	}
	if n := countPrefix(srv.Requests(), "POST /api/v1/alerts"); n != 0 {
		t.Fatalf("alertmanager must not be re-posted: %v", srv.Requests())
	}

	// Remove a rule group → its namespace is pruned; foreign namespace untouched.
	if err := testClient.Delete(testCtx, lokiG); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { _, ok := srv.LokiRules("1")["alerts-operator/default/sync-l"]; return !ok })
	if _, ok := srv.Rules("1")["other/x"]; !ok {
		t.Fatal("foreign namespace was pruned")
	}

	// Backend down → BackendUnavailable, Ready False; recovery → Ready True.
	srv.Fail(503)
	if err := testClient.Get(testCtx, clientKey(mimirG), mimirG); err != nil {
		t.Fatal(err)
	}
	mimirG.Spec.Groups[1].Rules[0].Expr = "max(up)"
	if err := testClient.Update(testCtx, mimirG); err != nil {
		t.Fatal(err)
	}
	waitCondition(t, tn, observabilityv1alpha1.ConditionMimirRulesSynced, metav1.ConditionFalse, observabilityv1alpha1.ReasonBackendUnavailable)
	waitCondition(t, tn, observabilityv1alpha1.ConditionReady, metav1.ConditionFalse, observabilityv1alpha1.ReasonBackendUnavailable)
	srv.Fail(0)
	waitCondition(t, tn, observabilityv1alpha1.ConditionReady, metav1.ConditionTrue, "")
	if srv.Rules("1")["alerts-operator/default/sync-m"][1].Rules[0].Expr != "max(up)" {
		t.Fatal("change lost after recovery")
	}
}

// TestTenantKeepsStaleGenerationNamespace covers the race the controller must not lose: a spec edit
// bumps an AlertRuleGroup's Generation and enqueues both its own reconciler and the Tenant
// reconciler. If the Tenant runs first, acceptedCurrent is false for that group — without
// KeepNamespaces, syncRules would read that as "no longer desired" and prune its backend namespace,
// producing a transient alerting gap until the child re-validates. We force exactly that window by
// patching the Accepted condition's ObservedGeneration back to the pre-edit generation and driving
// the Tenant reconciler directly (bypassing the manager, so nothing else can race our read).
func TestTenantKeepsStaleGenerationNamespace(t *testing.T) {
	tn, srv := newFakeTenant(t, "stale-tenant", true, false) // mimir only

	mimirG := &observabilityv1alpha1.AlertRuleGroup{ObjectMeta: metav1.ObjectMeta{Name: "stale-m", Namespace: "default"},
		Spec: observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: tn.Name, Backend: "mimir", Groups: ruleGroups("up == 0")}}
	createAndCleanup(t, mimirG)
	waitCondition(t, mimirG, observabilityv1alpha1.ConditionAccepted, metav1.ConditionTrue, observabilityv1alpha1.ReasonAccepted)
	waitFor(t, func() bool { return len(srv.Rules("1")["alerts-operator/default/stale-m"]) == 1 })
	oldGen := mimirG.Generation

	// Edit the spec: Generation moves to oldGen+1.
	mimirG.Spec.Groups[0].Rules[0].Expr = "up == 1"
	if err := testClient.Update(testCtx, mimirG); err != nil {
		t.Fatal(err)
	}

	// Simulate the child not having run yet: force Accepted back to the pre-edit generation.
	if err := testClient.Get(testCtx, clientKey(mimirG), mimirG); err != nil {
		t.Fatal(err)
	}
	setCondition(&mimirG.Status.Conditions, observabilityv1alpha1.ConditionAccepted, metav1.ConditionTrue, observabilityv1alpha1.ReasonAccepted, "", oldGen)
	if err := testClient.Status().Update(testCtx, mimirG); err != nil {
		t.Fatal(err)
	}

	srv.ResetRequests()
	r := &TenantReconciler{Client: testClient, Recorder: record.NewFakeRecorder(20)}
	if _, err := r.Reconcile(testCtx, ctrl.Request{NamespacedName: types.NamespacedName{Name: tn.Name}}); err != nil {
		t.Fatal(err)
	}

	if len(srv.Rules("1")["alerts-operator/default/stale-m"]) != 1 {
		t.Fatalf("stale-generation group's namespace must not be deleted: %+v", srv.Rules("1"))
	}
	if n := countPrefix(srv.Requests(), "DELETE"); n != 0 {
		t.Fatalf("no DELETE expected for a stale-generation namespace: %v", srv.Requests())
	}

	// Let the child reconcile normally, then the Tenant picks up the new content.
	waitFor(t, func() bool {
		if err := testClient.Get(testCtx, clientKey(mimirG), mimirG); err != nil {
			return false
		}
		return mimirG.Status.ObservedGeneration == mimirG.Generation
	})
	waitFor(t, func() bool {
		rs := srv.Rules("1")["alerts-operator/default/stale-m"]
		return len(rs) == 1 && rs[0].Rules[0].Expr == "up == 1"
	})
}

func TestTenantWithoutPolicy(t *testing.T) {
	tn, srv := newFakeTenant(t, "nopol-tenant", true, false)
	waitCondition(t, tn, observabilityv1alpha1.ConditionAlertmanagerSynced, metav1.ConditionFalse, observabilityv1alpha1.ReasonNoNotificationPolicy)
	waitCondition(t, tn, observabilityv1alpha1.ConditionReady, metav1.ConditionFalse, observabilityv1alpha1.ReasonNoNotificationPolicy)
	if srv.Alertmanager("1") != nil || countPrefix(srv.Requests(), "POST /api/v1/alerts") != 0 || countPrefix(srv.Requests(), "DELETE /api/v1/alerts") != 0 {
		t.Fatalf("backend AM must be untouched: %v", srv.Requests())
	}
}

func TestTenantAlertmanagerRejected(t *testing.T) {
	tn, srv := newFakeTenant(t, "rej-tenant", true, false)
	keep := &observabilityv1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "rej-keep", Namespace: "default"},
		Spec: observabilityv1alpha1.ContactPointSpec{TenantRef: tn.Name, Webhook: []observabilityv1alpha1.WebhookConfig{{URL: "http://keep"}}}}
	pol := &observabilityv1alpha1.NotificationPolicy{ObjectMeta: metav1.ObjectMeta{Name: "rej-pol", Namespace: "default"},
		Spec: observabilityv1alpha1.NotificationPolicySpec{TenantRef: tn.Name, Route: observabilityv1alpha1.Route{Receiver: "rej-keep"}}}
	srv.RejectPost("tenant quota exceeded")
	createAndCleanup(t, keep)
	createAndCleanup(t, pol)
	waitCondition(t, tn, observabilityv1alpha1.ConditionAlertmanagerSynced, metav1.ConditionFalse, observabilityv1alpha1.ReasonRejected)
	if !strings.Contains(findCond(tn, observabilityv1alpha1.ConditionAlertmanagerSynced).Message, "tenant quota exceeded") {
		t.Fatalf("message should carry backend body: %+v", findCond(tn, observabilityv1alpha1.ConditionAlertmanagerSynced))
	}
	waitCondition(t, pol, observabilityv1alpha1.ConditionSynced, metav1.ConditionFalse, observabilityv1alpha1.ReasonRejected)
	srv.RejectPost("")
	// Trigger a re-sync by touching the policy.
	if err := testClient.Get(testCtx, clientKey(pol), pol); err != nil {
		t.Fatal(err)
	}
	pol.Spec.Route.GroupWait = "10s"
	if err := testClient.Update(testCtx, pol); err != nil {
		t.Fatal(err)
	}
	waitCondition(t, tn, observabilityv1alpha1.ConditionAlertmanagerSynced, metav1.ConditionTrue, observabilityv1alpha1.ReasonSynced)
}
```

- [ ] **Step 2: Run, expect failure**

Run: `make test`
Expected: FAIL (stubs never write to the fake; `Rules("1")` empty).

- [ ] **Step 3: Implement rules sync**

Delete the `syncRules`, `syncAlertmanager` stubs from `tenant_controller.go` (keep `finalize`).

`internal/controller/tenant_rules.go`:

```go
package controller

import (
	"context"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/antnsn/alerts-operator/api/v1alpha1"
	"github.com/antnsn/alerts-operator/internal/backend"
	"github.com/antnsn/alerts-operator/internal/compile"
)

// syncRules makes the backend's rule namespaces under the tenant prefix match the accepted groups.
// Namespaces in keep — backend namespaces of AlertRuleGroups whose Accepted condition is stale for
// the current generation (see staleGeneration) — are left alone this pass: they're skipped in the
// diff loop (in practice desired never contains them, since groups only has accepted-current
// AlertRuleGroups) and, critically, excluded from the prune loop, so a spec edit racing ahead of its
// own child reconciler can never make the Tenant reconciler prune a namespace whose new content just
// hasn't been validated yet. It sets Mimir/LokiRulesSynced on the tenant and Synced on every group.
// The returned error is non-nil only when the backend was unavailable (caller backs off).
func (r *TenantReconciler) syncRules(ctx context.Context, tenant *v1alpha1.Tenant, store backend.RuleStore, be v1alpha1.Backend, groups []v1alpha1.AlertRuleGroup, keep map[string]bool) (int32, error) {
	condType := v1alpha1.ConditionMimirRulesSynced
	if be == v1alpha1.BackendLoki {
		condType = v1alpha1.ConditionLokiRulesSynced
	}
	prefix := tenant.Prefix() + "/"
	desired := compile.Rules(tenant.Prefix(), groups)

	actual, err := store.List(ctx)
	if err != nil {
		status, reason, msg := syncedFromErr(err)
		setCondition(&tenant.Status.Conditions, condType, status, reason, msg, tenant.Generation)
		r.Recorder.Eventf(tenant, corev1.EventTypeWarning, "RulesListFailed", "%s: %v", be, err)
		for i := range groups {
			r.setChildSynced(ctx, &groups[i], err)
		}
		if backend.IsUnavailable(err) {
			return 0, err
		}
		return 0, nil // 4xx: wait for the next change or resync, no backoff loop
	}

	nsErr := map[string]error{}
	var count int32
	for ns, want := range desired {
		if keep[ns] {
			continue
		}
		count += int32(len(want))
		have := actual[ns]
		if compile.RulesEqual(have, want) {
			continue
		}
		haveByName := map[string]backend.RuleGroup{}
		for _, g := range have {
			haveByName[g.Name] = g
		}
		wantByName := map[string]bool{}
		for _, g := range want {
			wantByName[g.Name] = true
			if h, ok := haveByName[g.Name]; ok && compile.RulesEqual([]backend.RuleGroup{h}, []backend.RuleGroup{g}) {
				continue
			}
			if err := store.SetGroup(ctx, ns, g); err != nil {
				nsErr[ns] = err
				break
			}
		}
		if nsErr[ns] != nil {
			continue
		}
		for name := range haveByName {
			if !wantByName[name] {
				if err := store.DeleteGroup(ctx, ns, name); err != nil {
					nsErr[ns] = err
					break
				}
			}
		}
	}
	var pruneErrs []error
	for ns := range actual {
		if keep[ns] {
			continue
		}
		if _, wanted := desired[ns]; strings.HasPrefix(ns, prefix) && !wanted {
			if err := store.DeleteNamespace(ctx, ns); err != nil {
				pruneErrs = append(pruneErrs, err)
			}
		}
	}

	var all []error
	for i := range groups {
		ns := compile.BackendNamespace(tenant.Prefix(), groups[i].Namespace, groups[i].Name)
		r.setChildSynced(ctx, &groups[i], nsErr[ns])
		if nsErr[ns] != nil {
			all = append(all, nsErr[ns])
			r.Recorder.Eventf(&groups[i], corev1.EventTypeWarning, "SyncFailed", "%s: %v", be, nsErr[ns])
		}
	}
	all = append(all, pruneErrs...)
	worst := worstErr(all)
	status, reason, msg := syncedFromErr(worst)
	setCondition(&tenant.Status.Conditions, condType, status, reason, msg, tenant.Generation)
	if worst != nil && backend.IsUnavailable(worst) {
		return count, worst
	}
	return count, nil
}

// worstErr prefers an unavailable error (retryable) over a rejection.
func worstErr(errs []error) error {
	var first error
	for _, e := range errs {
		if e == nil {
			continue
		}
		if backend.IsUnavailable(e) {
			return e
		}
		if first == nil {
			first = e
		}
	}
	return first
}

// setChildSynced patches Synced on a child. Patch failures are logged, not returned: the next
// Tenant reconcile repeats the patch.
func (r *TenantReconciler) setChildSynced(ctx context.Context, obj v1alpha1.Conditioned, syncErr error) {
	status, reason, msg := syncedFromErr(syncErr)
	conds := obj.GetConditions()
	if c := findCondition(conds, v1alpha1.ConditionSynced); c != nil && c.Status == status && c.Reason == reason && c.Message == msg {
		return
	}
	err := patchStatus(ctx, r.Client, obj, func() {
		conds := obj.GetConditions()
		setCondition(&conds, v1alpha1.ConditionSynced, status, reason, msg, obj.GetGeneration())
		obj.SetConditions(conds)
	}, nil) // the early return above already guarantees a real change
	if err != nil {
		log.FromContext(ctx).Info("failed to patch Synced on child", "object", obj.GetNamespace()+"/"+obj.GetName(), "err", err.Error())
	}
}

func findCondition(conds []metav1.Condition, typ string) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == typ {
			return &conds[i]
		}
	}
	return nil
}
```

- [ ] **Step 4: Implement Alertmanager sync**

`internal/controller/tenant_alertmanager.go`:

```go
package controller

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/antnsn/alerts-operator/api/v1alpha1"
	"github.com/antnsn/alerts-operator/internal/backend"
	"github.com/antnsn/alerts-operator/internal/compile"
)

// syncAlertmanager compiles and pushes the tenant's Alertmanager document. It sets
// AlertmanagerSynced on the tenant and Synced on the policy and contact points. The returned
// error is non-nil only when the backend was unavailable.
func (r *TenantReconciler) syncAlertmanager(ctx context.Context, tenant *v1alpha1.Tenant, store backend.AlertmanagerStore, ch *children) error {
	gen := tenant.Generation
	setTenant := func(status metav1.ConditionStatus, reason, msg string) {
		setCondition(&tenant.Status.Conditions, v1alpha1.ConditionAlertmanagerSynced, status, reason, msg, gen)
	}
	setChildren := func(err error) {
		if ch.Policy != nil {
			r.setChildSynced(ctx, ch.Policy, err)
		}
		for i := range ch.ContactPoints {
			r.setChildSynced(ctx, &ch.ContactPoints[i], err)
		}
	}

	// A ContactPoint or the NotificationPolicy was just edited and hasn't been re-validated by its
	// own reconciler yet (see listChildren/staleGeneration). Compiling now would risk either
	// pushing a stale config or, if the edit dropped the last accepted policy, misreporting
	// NoNotificationPolicy for what is really a transient gap. Wait: don't compile, don't POST.
	if ch.PendingAM {
		setTenant(metav1.ConditionUnknown, v1alpha1.ReasonPending, "waiting for child validation")
		return nil
	}

	if ch.Policy == nil {
		setTenant(metav1.ConditionFalse, v1alpha1.ReasonNoNotificationPolicy, "no accepted NotificationPolicy references this Tenant")
		return nil
	}

	templates, err := r.loadTemplates(ctx, tenant)
	if err != nil {
		setTenant(metav1.ConditionFalse, v1alpha1.ReasonInvalid, err.Error())
		r.Recorder.Eventf(tenant, corev1.EventTypeWarning, "TemplatesInvalid", "%v", err)
		return nil
	}

	cfg, err := compile.Alertmanager(compile.AlertmanagerInput{
		Policy: ch.Policy, ContactPoints: ch.ContactPoints, Secrets: r.secretResolver(ctx), Templates: templates,
	})
	if err != nil {
		var ae *compile.AttributedError
		if errors.As(err, &ae) {
			for i := range ch.ContactPoints {
				cp := &ch.ContactPoints[i]
				if ae.Kind == "ContactPoint" && cp.Namespace == ae.Namespace && cp.Name == ae.Name {
					r.setChildSynced(ctx, cp, ae.Err)
					r.Recorder.Eventf(cp, corev1.EventTypeWarning, "CompileFailed", "%v", ae.Err)
				}
			}
			if ae.Kind == "NotificationPolicy" {
				r.setChildSynced(ctx, ch.Policy, ae.Err)
				r.Recorder.Eventf(ch.Policy, corev1.EventTypeWarning, "CompileFailed", "%v", ae.Err)
			}
		}
		setTenant(metav1.ConditionFalse, v1alpha1.ReasonInvalid, err.Error())
		return nil
	}

	hash := compile.HashAlertmanager(cfg)
	if hash == tenant.Status.AlertmanagerConfigHash && time.Since(r.lastSync(tenant.Name)) < tenant.Resync() {
		setTenant(metav1.ConditionTrue, v1alpha1.ReasonSynced, "")
		setChildren(nil)
		return nil
	}

	current, err := store.Get(ctx)
	if err != nil {
		status, reason, msg := syncedFromErr(err)
		setTenant(status, reason, msg)
		setChildren(err)
		r.Recorder.Eventf(tenant, corev1.EventTypeWarning, "AlertmanagerGetFailed", "%v", err)
		if backend.IsUnavailable(err) {
			return err
		}
		return nil
	}
	if current == nil || current.Config != cfg.Config || !maps.Equal(current.TemplateFiles, cfg.TemplateFiles) {
		if err := store.Set(ctx, cfg); err != nil {
			status, reason, msg := syncedFromErr(err)
			setTenant(status, reason, msg)
			setChildren(err)
			r.Recorder.Eventf(tenant, corev1.EventTypeWarning, "AlertmanagerSetFailed", "%v", err)
			if backend.IsUnavailable(err) {
				return err
			}
			return nil
		}
	}
	tenant.Status.AlertmanagerConfigHash = hash
	r.markSynced(tenant.Name)
	setTenant(metav1.ConditionTrue, v1alpha1.ReasonSynced, "")
	setChildren(nil)
	return nil
}

func (r *TenantReconciler) lastSync(name string) time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastAMSync[name]
}

func (r *TenantReconciler) markSynced(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lastAMSync == nil {
		r.lastAMSync = map[string]time.Time{}
	}
	r.lastAMSync[name] = time.Now()
}

// secretResolver reads Secret keys through the cached client.
func (r *TenantReconciler) secretResolver(ctx context.Context) compile.SecretResolver {
	return func(namespace, name, key string) (string, error) {
		var sec corev1.Secret
		if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &sec); err != nil {
			if apierrors.IsNotFound(err) {
				return "", fmt.Errorf("not found")
			}
			return "", err
		}
		v, ok := sec.Data[key]
		if !ok {
			return "", fmt.Errorf("key not found")
		}
		return string(v), nil
	}
}

// loadTemplates returns the ConfigMap data behind spec.alertmanager.templatesRef, or nil.
func (r *TenantReconciler) loadTemplates(ctx context.Context, tenant *v1alpha1.Tenant) (map[string]string, error) {
	if tenant.Spec.Alertmanager == nil || tenant.Spec.Alertmanager.TemplatesRef == nil {
		return nil, nil
	}
	ref := tenant.Spec.Alertmanager.TemplatesRef
	var cm corev1.ConfigMap
	if err := r.Get(ctx, types.NamespacedName{Namespace: ref.Namespace, Name: ref.Name}, &cm); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("templates ConfigMap %s not found", ref)
		}
		return nil, err
	}
	if len(cm.Data) == 0 {
		return nil, nil
	}
	return maps.Clone(cm.Data), nil
}
```

- [ ] **Step 5: Run tests**

Run: `make test`
Expected: PASS. The "no POST on no-op" step relies on the child reconcilers and the Tenant reconciler skipping status patches when nothing changed (Tasks 14–18); if it flakes, that guard is what to inspect. `TestTenantKeepsStaleGenerationNamespace` drives a `TenantReconciler` directly (bypassing the manager) so its assertions aren't racing the background controllers; if it flakes, check that nothing else patched `stale-m`'s Accepted condition between the forced-stale `Status().Update` and the direct `Reconcile` call.

- [ ] **Step 6: Commit**

```bash
git add internal/controller
git commit -m "feat(controller): Tenant syncs rule namespaces and Alertmanager config with diff and prune"
```

---

### Task 20: Tenant finalizer and metrics

**Files:**
- Create: `internal/controller/tenant_finalizer.go`, `internal/metrics/metrics.go`
- Modify: `internal/controller/tenant_controller.go` (delete `finalize` stub), `internal/controller/tenant_rules.go`, `internal/controller/tenant_alertmanager.go` (record metrics)
- Test: `internal/controller/tenant_finalizer_test.go`, `internal/metrics/metrics_test.go`

**Interfaces:**
- Consumes: `MimirClient`, `TenantReconciler.mimirClient/lokiClient` (Task 18); `backend.RuleStore.List/DeleteNamespace`, `backend.AlertmanagerStore.Delete` (Task 7).
- Produces:
```go
// internal/metrics
const ( TargetAlertmanager = "alertmanager"; TargetMimirRules = "mimir_rules"; TargetLokiRules = "loki_rules" )
var SyncTotal *prometheus.CounterVec          // alerts_operator_sync_total{tenant,target,result="ok"|"error"}
var LastSyncTimestamp *prometheus.GaugeVec    // alerts_operator_last_sync_timestamp_seconds{tenant,target}
func Observe(tenant, target string, err error)
// internal/controller
func (r *TenantReconciler) finalize(ctx context.Context, tenant *v1alpha1.Tenant) error
func deleteOwnedNamespaces(ctx context.Context, store backend.RuleStore, prefix string) error
```

- [ ] **Step 1: Failing tests**

`internal/metrics/metrics_test.go`:

```go
package metrics

import (
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestObserve(t *testing.T) {
	Observe("t1", TargetMimirRules, nil)
	Observe("t1", TargetMimirRules, nil)
	Observe("t1", TargetMimirRules, errors.New("boom"))
	if got := testutil.ToFloat64(SyncTotal.WithLabelValues("t1", TargetMimirRules, "ok")); got != 2 {
		t.Fatalf("ok=%v", got)
	}
	if got := testutil.ToFloat64(SyncTotal.WithLabelValues("t1", TargetMimirRules, "error")); got != 1 {
		t.Fatalf("error=%v", got)
	}
	if got := testutil.ToFloat64(LastSyncTimestamp.WithLabelValues("t1", TargetMimirRules)); got <= 0 {
		t.Fatalf("timestamp not set: %v", got)
	}
	if got := testutil.ToFloat64(LastSyncTimestamp.WithLabelValues("t2", TargetLokiRules)); got != 0 {
		t.Fatalf("untouched gauge should be 0: %v", got)
	}
}
```

`internal/controller/tenant_finalizer_test.go`:

```go
package controller

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	observabilityv1alpha1 "github.com/antnsn/alerts-operator/api/v1alpha1"
	"github.com/antnsn/alerts-operator/internal/backend"
)

func TestTenantFinalizerCleansBackend(t *testing.T) {
	tn, srv := newFakeTenant(t, "fin-tenant", true, true)
	srv.SetRules("1", "other/x", []backend.RuleGroup{{Name: "keep", Rules: []backend.Rule{{Alert: "K", Expr: "up"}}}})
	keep := &observabilityv1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "fin-keep", Namespace: "default"},
		Spec: observabilityv1alpha1.ContactPointSpec{TenantRef: tn.Name, Webhook: []observabilityv1alpha1.WebhookConfig{{URL: "http://keep"}}}}
	pol := &observabilityv1alpha1.NotificationPolicy{ObjectMeta: metav1.ObjectMeta{Name: "fin-pol", Namespace: "default"},
		Spec: observabilityv1alpha1.NotificationPolicySpec{TenantRef: tn.Name, Route: observabilityv1alpha1.Route{Receiver: "fin-keep"}}}
	g := &observabilityv1alpha1.AlertRuleGroup{ObjectMeta: metav1.ObjectMeta{Name: "fin-g", Namespace: "default"},
		Spec: observabilityv1alpha1.AlertRuleGroupSpec{TenantRef: tn.Name, Backend: "loki", Groups: ruleGroups(`{a="b"}`)}}
	for _, o := range []observabilityv1alpha1.Conditioned{keep, pol, g} {
		createAndCleanup(t, o)
	}
	waitCondition(t, tn, observabilityv1alpha1.ConditionReady, metav1.ConditionTrue, "")
	if srv.Alertmanager("1") == nil || len(srv.LokiRules("1")["alerts-operator/default/fin-g"]) != 1 {
		t.Fatal("precondition: backend populated")
	}

	// Backend down: deletion is blocked, finalizer stays, Ready=False/Deleting.
	srv.Fail(503)
	if err := testClient.Delete(testCtx, tn); err != nil {
		t.Fatal(err)
	}
	waitCondition(t, tn, observabilityv1alpha1.ConditionReady, metav1.ConditionFalse, observabilityv1alpha1.ReasonDeleting)
	if !controllerutil.ContainsFinalizer(tn, tenantFinalizer) {
		t.Fatal("finalizer removed while backend down")
	}

	// Backend back: AM config and owned namespaces deleted, foreign namespace kept, object gone.
	srv.Fail(0)
	waitFor(t, func() bool {
		err := testClient.Get(testCtx, clientKey(tn), &observabilityv1alpha1.Tenant{})
		return errors.IsNotFound(err)
	})
	time.Sleep(200 * time.Millisecond)
	if srv.Alertmanager("1") != nil {
		t.Fatal("alertmanager config not deleted")
	}
	if _, ok := srv.LokiRules("1")["alerts-operator/default/fin-g"]; ok {
		t.Fatal("owned loki namespace not deleted")
	}
	if _, ok := srv.Rules("1")["other/x"]; !ok {
		t.Fatal("foreign mimir namespace deleted")
	}
}
```

- [ ] **Step 2: Run, expect failure**

Run: `go test ./internal/metrics/ && make test`
Expected: FAIL (`metrics` package missing; finalizer stub leaves backend populated).

- [ ] **Step 3: Implement metrics**

`internal/metrics/metrics.go`:

```go
// Package metrics exposes operator metrics on the controller-runtime registry.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

const (
	TargetAlertmanager = "alertmanager"
	TargetMimirRules   = "mimir_rules"
	TargetLokiRules    = "loki_rules"
)

var (
	SyncTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "alerts_operator_sync_total",
		Help: "Sync attempts per tenant and target.",
	}, []string{"tenant", "target", "result"})

	LastSyncTimestamp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "alerts_operator_last_sync_timestamp_seconds",
		Help: "Unix time of the last successful sync per tenant and target.",
	}, []string{"tenant", "target"})
)

func init() {
	ctrlmetrics.Registry.MustRegister(SyncTotal, LastSyncTimestamp)
}

// Observe records one sync attempt. A nil err is success.
func Observe(tenant, target string, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	SyncTotal.WithLabelValues(tenant, target, result).Inc()
	if err == nil {
		LastSyncTimestamp.WithLabelValues(tenant, target).SetToCurrentTime()
	}
}
```

Wire it in:
- `tenant_rules.go`, at the end of `syncRules` just before the final `return` statements, and in the `store.List` error branch before `return 0, err`:
  ```go
  target := metrics.TargetMimirRules
  if be == v1alpha1.BackendLoki {
  	target = metrics.TargetLokiRules
  }
  metrics.Observe(tenant.Name, target, worst)   // in the List-error branch: metrics.Observe(tenant.Name, target, err)
  ```
- `tenant_alertmanager.go`: `metrics.Observe(tenant.Name, metrics.TargetAlertmanager, err)` in the `store.Get` and `store.Set` error branches, and `metrics.Observe(tenant.Name, metrics.TargetAlertmanager, nil)` right after `r.markSynced(tenant.Name)`. Compile errors (`ReasonInvalid`) and `NoNotificationPolicy` are not backend syncs and are not counted.

Import `"github.com/antnsn/alerts-operator/internal/metrics"` in both files; that import also triggers the `init()` registration, so `cmd/main.go` needs no blank import.

- [ ] **Step 4: Implement finalizer**

Delete the `finalize` stub from `tenant_controller.go`. Create `internal/controller/tenant_finalizer.go`:

```go
package controller

import (
	"context"
	"fmt"
	"strings"

	"github.com/antnsn/alerts-operator/api/v1alpha1"
	"github.com/antnsn/alerts-operator/internal/backend"
)

// finalize removes everything this tenant owns in its backends: the Alertmanager config
// and every rule namespace under the tenant prefix. Any failure keeps the finalizer.
func (r *TenantReconciler) finalize(ctx context.Context, tenant *v1alpha1.Tenant) error {
	prefix := tenant.Prefix() + "/"
	if tenant.Spec.Mimir != nil {
		mc, err := r.mimirClient(ctx, tenant)
		if err != nil {
			return fmt.Errorf("mimir client: %w", err)
		}
		if err := mc.Delete(ctx); err != nil {
			return fmt.Errorf("delete alertmanager config: %w", err)
		}
		if err := deleteOwnedNamespaces(ctx, mc, prefix); err != nil {
			return fmt.Errorf("mimir rules: %w", err)
		}
	}
	if tenant.Spec.Loki != nil {
		lc, err := r.lokiClient(ctx, tenant)
		if err != nil {
			return fmt.Errorf("loki client: %w", err)
		}
		if err := deleteOwnedNamespaces(ctx, lc, prefix); err != nil {
			return fmt.Errorf("loki rules: %w", err)
		}
	}
	return nil
}

// deleteOwnedNamespaces deletes every rule namespace starting with prefix.
func deleteOwnedNamespaces(ctx context.Context, store backend.RuleStore, prefix string) error {
	actual, err := store.List(ctx)
	if err != nil {
		return err
	}
	for ns := range actual {
		if strings.HasPrefix(ns, prefix) {
			if err := store.DeleteNamespace(ctx, ns); err != nil {
				return err
			}
		}
	}
	return nil
}
```

- [ ] **Step 5: Run tests**

Run: `go test ./internal/metrics/ -v && make test && golangci-lint run ./...`
Expected: all PASS, lint clean.

- [ ] **Step 6: Commit**

```bash
git add internal/controller internal/metrics
git commit -m "feat(controller): Tenant finalizer cleans backends; sync metrics"
```

---


---

---

### Task 21: Helm chart

**Files:**
- Create: `charts/alerts-operator/Chart.yaml`, `charts/alerts-operator/values.yaml`, `charts/alerts-operator/templates/{_helpers.tpl,serviceaccount.yaml,clusterrole.yaml,clusterrolebinding.yaml,role.yaml,rolebinding.yaml,deployment.yaml,service.yaml,servicemonitor.yaml,NOTES.txt}`, `charts/alerts-operator/crds/.gitkeep`, `charts/alerts-operator/.helmignore`, `hack/chart-test.sh`
- Modify: `Makefile` (add `helm-sync-crds`, `chart-test`)

**Interfaces:**
- Consumes: CRDs in `config/crd/bases/*.yaml` (Tasks 3–6); operator flags from scaffolded `cmd/main.go` (`--leader-elect`, `--metrics-bind-address`, `--metrics-secure`, `--health-probe-bind-address`); RBAC needs from Tasks 15–20 (listed below).
- Produces: installable chart `charts/alerts-operator` (version 0.1.0); `make helm-sync-crds` copies CRDs into `charts/alerts-operator/crds/`; `make chart-test` runs `hack/chart-test.sh`.

- [ ] **Step 1: Write the chart test script (fails until chart exists)**

`hack/chart-test.sh`:

```bash
#!/usr/bin/env bash
# Renders the chart and asserts on the output. No cluster needed.
set -euo pipefail
CHART="$(cd "$(dirname "$0")/.." && pwd)/charts/alerts-operator"
OUT="$(mktemp -d)"
trap 'rm -rf "$OUT"' EXIT

helm lint "$CHART"

helm template x "$CHART" --namespace alerts-operator --include-crds > "$OUT/default.yaml"
crds=$(grep -c '^kind: CustomResourceDefinition$' "$OUT/default.yaml" || true)
[ "$crds" -eq 4 ] || { echo "expected 4 CRDs, got $crds (run make helm-sync-crds)"; exit 1; }
grep -q '^kind: Deployment$' "$OUT/default.yaml"
grep -q '^kind: ClusterRole$' "$OUT/default.yaml"
grep -q -- '--leader-elect' "$OUT/default.yaml"
grep -q -- '--metrics-bind-address=:8080' "$OUT/default.yaml"
grep -q -- '--metrics-secure=false' "$OUT/default.yaml"
grep -q 'image: ghcr.io/antnsn/alerts-operator:0.1.0' "$OUT/default.yaml"
! grep -q '^kind: ServiceMonitor$' "$OUT/default.yaml"

helm template x "$CHART" --namespace alerts-operator --include-crds --set serviceMonitor.enabled=true --set image.tag=dev > "$OUT/sm.yaml"
grep -q '^kind: ServiceMonitor$' "$OUT/sm.yaml"
grep -q 'image: ghcr.io/antnsn/alerts-operator:dev' "$OUT/sm.yaml"

helm template x "$CHART" --namespace alerts-operator --include-crds --set leaderElection.enabled=false > "$OUT/nole.yaml"
! grep -q -- '--leader-elect' "$OUT/nole.yaml"
! grep -q '^kind: Role$' "$OUT/nole.yaml"

echo "chart-test: OK"
```

```bash
chmod +x hack/chart-test.sh
```

Append to `Makefile` (after the `##@ Build` section):

```make
##@ Helm

CHART_DIR ?= charts/alerts-operator

.PHONY: helm-sync-crds
helm-sync-crds: manifests ## Copy generated CRDs into the Helm chart.
	mkdir -p $(CHART_DIR)/crds
	rm -f $(CHART_DIR)/crds/*.yaml
	cp -f config/crd/bases/*.yaml $(CHART_DIR)/crds/

.PHONY: chart-test
chart-test: helm-sync-crds ## Lint and render the chart with assertions.
	hack/chart-test.sh
```

- [ ] **Step 2: Run, expect failure**

Run: `make chart-test`
Expected: FAIL — `helm lint` errors with `Chart.yaml file is missing`.

- [ ] **Step 3: Write the chart**

`charts/alerts-operator/Chart.yaml`:

```yaml
apiVersion: v2
name: alerts-operator
description: Kubernetes operator for Grafana Mimir / Loki alert rules and Mimir Alertmanager configuration
type: application
version: 0.1.0
appVersion: "0.1.0"
home: https://github.com/antnsn/alerts-operator
sources:
  - https://github.com/antnsn/alerts-operator
keywords: [mimir, loki, alertmanager, alerting, operator]
maintainers:
  - name: antnsn
    url: https://github.com/antnsn
```

`charts/alerts-operator/.helmignore`:

```
.DS_Store
.git/
.gitignore
*.swp
*.tmp
```

`charts/alerts-operator/values.yaml`:

```yaml
replicaCount: 1

image:
  repository: ghcr.io/antnsn/alerts-operator
  # Defaults to .Chart.AppVersion when empty.
  tag: ""
  pullPolicy: IfNotPresent

imagePullSecrets: []
nameOverride: ""
fullnameOverride: ""

serviceAccount:
  create: true
  name: ""
  annotations: {}

leaderElection:
  enabled: true

metrics:
  enabled: true
  service:
    port: 8080

serviceMonitor:
  enabled: false
  labels: {}
  interval: 30s

# Extra args appended to the manager command line, e.g. ["--zap-log-level=debug"].
extraArgs: []

resources:
  requests:
    cpu: 10m
    memory: 64Mi
  limits:
    memory: 256Mi

podSecurityContext:
  runAsNonRoot: true
  seccompProfile:
    type: RuntimeDefault

securityContext:
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: true
  capabilities:
    drop: ["ALL"]

podAnnotations: {}
podLabels: {}
nodeSelector: {}
tolerations: []
affinity: {}
```

`charts/alerts-operator/templates/_helpers.tpl`:

```
{{- define "alerts-operator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "alerts-operator.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "alerts-operator.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "alerts-operator.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "alerts-operator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "alerts-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "alerts-operator.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "alerts-operator.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "alerts-operator.image" -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) }}
{{- end }}
```

`charts/alerts-operator/templates/serviceaccount.yaml`:

```yaml
{{- if .Values.serviceAccount.create }}
apiVersion: v1
kind: ServiceAccount
metadata:
  name: {{ include "alerts-operator.serviceAccountName" . }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "alerts-operator.labels" . | nindent 4 }}
  {{- with .Values.serviceAccount.annotations }}
  annotations:
    {{- toYaml . | nindent 4 }}
  {{- end }}
{{- end }}
```

`charts/alerts-operator/templates/clusterrole.yaml`:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: {{ include "alerts-operator.fullname" . }}
  labels:
    {{- include "alerts-operator.labels" . | nindent 4 }}
# Must stay identical to the "Appendix: RBAC rules" list at the end of the plan
# (= config/rbac/role.yaml after `make manifests`). Children have no finalizers.
rules:
  - apiGroups: ["observability.antnsn.dev"]
    resources: ["tenants"]
    verbs: ["get", "list", "watch", "update", "patch"]
  - apiGroups: ["observability.antnsn.dev"]
    resources: ["tenants/status"]
    verbs: ["get", "update", "patch"]
  - apiGroups: ["observability.antnsn.dev"]
    resources: ["tenants/finalizers"]
    verbs: ["update"]
  - apiGroups: ["observability.antnsn.dev"]
    resources: ["contactpoints", "notificationpolicies", "alertrulegroups"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["observability.antnsn.dev"]
    resources: ["contactpoints/status", "notificationpolicies/status", "alertrulegroups/status"]
    verbs: ["get", "update", "patch"]
  - apiGroups: [""]
    resources: ["secrets", "configmaps"]
    verbs: ["get", "list", "watch"]
  - apiGroups: [""]
    resources: ["events"]
    verbs: ["create", "patch"]
```

`charts/alerts-operator/templates/clusterrolebinding.yaml`:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: {{ include "alerts-operator.fullname" . }}
  labels:
    {{- include "alerts-operator.labels" . | nindent 4 }}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: {{ include "alerts-operator.fullname" . }}
subjects:
  - kind: ServiceAccount
    name: {{ include "alerts-operator.serviceAccountName" . }}
    namespace: {{ .Release.Namespace }}
```

`charts/alerts-operator/templates/role.yaml` (leader election, release namespace only):

```yaml
{{- if .Values.leaderElection.enabled }}
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: {{ include "alerts-operator.fullname" . }}-leader-election
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "alerts-operator.labels" . | nindent 4 }}
rules:
  - apiGroups: ["coordination.k8s.io"]
    resources: ["leases"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
  - apiGroups: [""]
    resources: ["configmaps"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
  - apiGroups: [""]
    resources: ["events"]
    verbs: ["create", "patch"]
{{- end }}
```

`charts/alerts-operator/templates/rolebinding.yaml`:

```yaml
{{- if .Values.leaderElection.enabled }}
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: {{ include "alerts-operator.fullname" . }}-leader-election
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "alerts-operator.labels" . | nindent 4 }}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: {{ include "alerts-operator.fullname" . }}-leader-election
subjects:
  - kind: ServiceAccount
    name: {{ include "alerts-operator.serviceAccountName" . }}
    namespace: {{ .Release.Namespace }}
{{- end }}
```

`charts/alerts-operator/templates/deployment.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "alerts-operator.fullname" . }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "alerts-operator.labels" . | nindent 4 }}
spec:
  replicas: {{ .Values.replicaCount }}
  selector:
    matchLabels:
      {{- include "alerts-operator.selectorLabels" . | nindent 6 }}
  template:
    metadata:
      labels:
        {{- include "alerts-operator.selectorLabels" . | nindent 8 }}
        {{- with .Values.podLabels }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
      {{- with .Values.podAnnotations }}
      annotations:
        {{- toYaml . | nindent 8 }}
      {{- end }}
    spec:
      {{- with .Values.imagePullSecrets }}
      imagePullSecrets:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      serviceAccountName: {{ include "alerts-operator.serviceAccountName" . }}
      securityContext:
        {{- toYaml .Values.podSecurityContext | nindent 8 }}
      terminationGracePeriodSeconds: 10
      containers:
        - name: manager
          image: {{ include "alerts-operator.image" . }}
          imagePullPolicy: {{ .Values.image.pullPolicy }}
          args:
            {{- if .Values.leaderElection.enabled }}
            - --leader-elect
            {{- end }}
            {{- if .Values.metrics.enabled }}
            - --metrics-bind-address=:{{ .Values.metrics.service.port }}
            - --metrics-secure=false
            {{- else }}
            - --metrics-bind-address=0
            {{- end }}
            - --health-probe-bind-address=:8081
            {{- range .Values.extraArgs }}
            - {{ . | quote }}
            {{- end }}
          ports:
            {{- if .Values.metrics.enabled }}
            - name: metrics
              containerPort: {{ .Values.metrics.service.port }}
              protocol: TCP
            {{- end }}
            - name: probes
              containerPort: 8081
              protocol: TCP
          livenessProbe:
            httpGet:
              path: /healthz
              port: probes
            initialDelaySeconds: 15
            periodSeconds: 20
          readinessProbe:
            httpGet:
              path: /readyz
              port: probes
            initialDelaySeconds: 5
            periodSeconds: 10
          securityContext:
            {{- toYaml .Values.securityContext | nindent 12 }}
          resources:
            {{- toYaml .Values.resources | nindent 12 }}
      {{- with .Values.nodeSelector }}
      nodeSelector:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .Values.affinity }}
      affinity:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .Values.tolerations }}
      tolerations:
        {{- toYaml . | nindent 8 }}
      {{- end }}
```

`charts/alerts-operator/templates/service.yaml`:

```yaml
{{- if .Values.metrics.enabled }}
apiVersion: v1
kind: Service
metadata:
  name: {{ include "alerts-operator.fullname" . }}-metrics
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "alerts-operator.labels" . | nindent 4 }}
spec:
  type: ClusterIP
  selector:
    {{- include "alerts-operator.selectorLabels" . | nindent 4 }}
  ports:
    - name: metrics
      port: {{ .Values.metrics.service.port }}
      targetPort: metrics
      protocol: TCP
{{- end }}
```

`charts/alerts-operator/templates/servicemonitor.yaml`:

```yaml
{{- if and .Values.metrics.enabled .Values.serviceMonitor.enabled }}
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: {{ include "alerts-operator.fullname" . }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "alerts-operator.labels" . | nindent 4 }}
    {{- with .Values.serviceMonitor.labels }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
spec:
  selector:
    matchLabels:
      {{- include "alerts-operator.selectorLabels" . | nindent 6 }}
  namespaceSelector:
    matchNames: [{{ .Release.Namespace | quote }}]
  endpoints:
    - port: metrics
      interval: {{ .Values.serviceMonitor.interval }}
      path: /metrics
{{- end }}
```

`charts/alerts-operator/templates/NOTES.txt`:

```
alerts-operator {{ .Chart.AppVersion }} installed in namespace {{ .Release.Namespace }}.

Next:
  1. Create a cluster-scoped Tenant pointing at your Mimir/Loki gateway.
  2. Create ContactPoints + one NotificationPolicy per Tenant, and AlertRuleGroups.
     Examples: https://github.com/antnsn/alerts-operator/tree/main/docs/examples
  3. Watch status:
       kubectl get tenants
       kubectl get contactpoints,notificationpolicies,alertrulegroups -A
{{- if .Values.metrics.enabled }}

Metrics: http://{{ include "alerts-operator.fullname" . }}-metrics.{{ .Release.Namespace }}.svc:{{ .Values.metrics.service.port }}/metrics
{{- end }}
```

Create `charts/alerts-operator/crds/.gitkeep` (empty). CRD YAML files in `crds/` are generated by `make helm-sync-crds` and committed (chart-releaser packages the directory as-is).

- [ ] **Step 4: Sync CRDs and run the chart test**

Run: `make chart-test`
Expected: last line `chart-test: OK`; `ls charts/alerts-operator/crds/` shows the four `observability.antnsn.dev_*.yaml` files.

- [ ] **Step 5: CI runs the chart test**

In `.github/workflows/ci.yml` replace the last step

```yaml
      - run: test -d charts/alerts-operator && helm lint charts/alerts-operator || echo "no chart yet"
```

with

```yaml
      - run: make chart-test
```

Validate: `yq e . .github/workflows/ci.yml >/dev/null && echo yaml-ok`.

- [ ] **Step 6: Commit**

```bash
git add charts hack/chart-test.sh Makefile .github/workflows/ci.yml
git commit -m "feat(chart): Helm chart with CRDs, RBAC, metrics service and ServiceMonitor"
```

---

### Task 22: Docs — ArgoCD health, examples, migration runbook, README

**Files:**
- Create: `docs/argocd-health.md`, `docs/examples/{tenant,secrets,contactpoints,notificationpolicy,alertrulegroup-mimir,alertrulegroup-loki}.yaml`, `docs/migration.md`
- Modify: `README.md`

**Interfaces:**
- Consumes: CRD schemas (Tasks 3–6), condition names from `api/v1alpha1/conditions.go` (Task 2): `Ready`, `Accepted`, `Synced`, `AlertmanagerSynced`, `MimirRulesSynced`, `LokiRulesSynced`.
- Produces: example manifests used verbatim by the e2e runbook (Task 24).

- [ ] **Step 1: Validation script for examples (fails until files exist)**

Append to `hack/chart-test.sh` before the final `echo`:

```bash
# Examples must exist and parse as YAML; schema validation happens on the home cluster (docs/e2e.md).
EX="$(cd "$(dirname "$0")/.." && pwd)/docs/examples"
for f in tenant secrets contactpoints notificationpolicy alertrulegroup-mimir alertrulegroup-loki; do
  [ -f "$EX/$f.yaml" ] || { echo "missing docs/examples/$f.yaml"; exit 1; }
  yq e . "$EX/$f.yaml" >/dev/null
done
```

(`yq` = `brew install yq`, the Go one by mikefarah.)

- [ ] **Step 2: Run, expect failure**

Run: `make chart-test`
Expected: FAIL `missing docs/examples/tenant.yaml`.

- [ ] **Step 3: Write examples**

`docs/examples/tenant.yaml`:

```yaml
apiVersion: observability.antnsn.dev/v1alpha1
kind: Tenant
metadata:
  name: homelab
spec:
  tenantId: "1"
  mimir:
    address: http://mimir-distributed-nginx.mimir:80
  loki:
    address: http://loki-gateway.loki
  alertmanager: {}
  rulesNamespacePrefix: alerts-operator
  resyncInterval: 5m
```

`docs/examples/secrets.yaml`:

```yaml
# Placeholder secrets. In the home cluster these come from Infisical via ExternalSecret (see docs/migration.md).
apiVersion: v1
kind: Secret
metadata:
  name: keep
  namespace: monitoring
type: Opaque
stringData:
  api-key: REPLACE_ME
---
apiVersion: v1
kind: Secret
metadata:
  name: pushover
  namespace: monitoring
type: Opaque
stringData:
  user: REPLACE_ME
  token: REPLACE_ME
```

`docs/examples/contactpoints.yaml`:

```yaml
apiVersion: observability.antnsn.dev/v1alpha1
kind: ContactPoint
metadata:
  name: keep
  namespace: monitoring
spec:
  tenantRef: homelab
  webhook:
    - url: http://keep-backend.keep:8080/alerts/event/prometheus
      httpConfig:
        bearerTokenSecretRef:
          name: keep
          key: api-key
      sendResolved: true
---
apiVersion: observability.antnsn.dev/v1alpha1
kind: ContactPoint
metadata:
  name: pushover
  namespace: monitoring
spec:
  tenantRef: homelab
  pushover:
    - userKeySecretRef:
        name: pushover
        key: user
      tokenSecretRef:
        name: pushover
        key: token
      priority: "1"
      title: '{{ .CommonLabels.alertname }} ({{ .Status }})'
      sendResolved: true
```

`docs/examples/notificationpolicy.yaml`:

```yaml
apiVersion: observability.antnsn.dev/v1alpha1
kind: NotificationPolicy
metadata:
  name: homelab
  namespace: monitoring
spec:
  tenantRef: homelab
  route:
    receiver: keep
    groupBy: [alertname, namespace]
    groupWait: 30s
    groupInterval: 5m
    repeatInterval: 4h
    routes:
      - receiver: pushover
        matchers: ['severity="critical"']
        continue: true
  inhibitRules:
    - sourceMatchers: ['severity="critical"']
      targetMatchers: ['severity="warning"']
      equal: [alertname, namespace]
```

`docs/examples/alertrulegroup-mimir.yaml`:

```yaml
apiVersion: observability.antnsn.dev/v1alpha1
kind: AlertRuleGroup
metadata:
  name: homelab
  namespace: monitoring
spec:
  tenantRef: homelab
  backend: mimir
  groups:
    - name: node.health
      interval: 1m
      rules:
        - alert: NodeDown
          expr: up{job="node"} == 0
          for: 5m
          labels:
            severity: critical
          annotations:
            summary: "{{ $labels.instance }} is down"
        - record: job:up:ratio
          expr: avg by (job) (up)
```

`docs/examples/alertrulegroup-loki.yaml`:

```yaml
apiVersion: observability.antnsn.dev/v1alpha1
kind: AlertRuleGroup
metadata:
  name: udm
  namespace: monitoring
spec:
  tenantRef: homelab
  backend: loki
  groups:
    - name: udm
      interval: 1m
      rules:
        - alert: UDMErrorBurst
          expr: sum(rate({host="udm"} |= "error" [5m])) > 1
          for: 10m
          labels:
            severity: warning
          annotations:
            summary: "UDM logging errors at {{ $value }}/s"
```

- [ ] **Step 4: ArgoCD health doc**

`docs/argocd-health.md`:

````markdown
# ArgoCD health checks

Without these, ArgoCD shows every alerts-operator CR as `Healthy` the moment it exists.
Add to `argocd-cm` (`resource.customizations.health.<group>_<Kind>`).

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: argocd-cm
  namespace: argocd
data:
  resource.customizations.health.observability.antnsn.dev_Tenant: |
    hs = { status = "Progressing", message = "waiting for Ready condition" }
    if obj.status ~= nil and obj.status.conditions ~= nil then
      for _, c in ipairs(obj.status.conditions) do
        if c.type == "Ready" then
          if c.status == "True" then
            hs.status = "Healthy"
            hs.message = c.reason
          elseif c.reason == "Deleting" then
            hs.status = "Progressing"
            hs.message = c.message
          else
            hs.status = "Degraded"
            hs.message = c.reason .. ": " .. (c.message or "")
          end
        end
      end
    end
    return hs
  resource.customizations.health.observability.antnsn.dev_ContactPoint: &child |
    hs = { status = "Progressing", message = "waiting for Accepted/Synced" }
    if obj.status ~= nil and obj.status.conditions ~= nil then
      local accepted, synced = nil, nil
      for _, c in ipairs(obj.status.conditions) do
        if c.type == "Accepted" then accepted = c end
        if c.type == "Synced" then synced = c end
      end
      if accepted ~= nil and accepted.status == "False" then
        return { status = "Degraded", message = accepted.reason .. ": " .. (accepted.message or "") }
      end
      if synced ~= nil then
        if synced.status == "True" then
          return { status = "Healthy", message = synced.reason }
        end
        return { status = "Degraded", message = synced.reason .. ": " .. (synced.message or "") }
      end
    end
    return hs
  resource.customizations.health.observability.antnsn.dev_NotificationPolicy: *child
  resource.customizations.health.observability.antnsn.dev_AlertRuleGroup: *child
```

YAML anchors are resolved by the apiserver's YAML parser, so the three child kinds share one Lua body.
Verify: `argocd app get <app>` shows `Degraded` for a CR with `Accepted=False`.
````

- [ ] **Step 5: Migration runbook**

`docs/migration.md`:

````markdown
# Home-cluster migration: mimir-sync + Alloy rules sync → alerts-operator

Repo: `antnsn/cluster`. Everything below is a change in that repo unless marked *(cluster command)*.
Order matters: the operator must own the backends before the old writers are removed, and old
backend state must be pruned by hand (the operator only prunes under its own prefix).

## 0. Preconditions

- alerts-operator image + chart released (Task 25), or `:dev` image pushed (docs/e2e.md).
- ArgoCD health Lua from `docs/argocd-health.md` merged into `argocd-cm`.
- Confirm the tenant picture *(cluster command)*:

```bash
M=http://mimir-distributed-nginx.mimir:80
kubectl -n mimir run curl --rm -it --image=curlimages/curl --restart=Never -- sh -c "
  curl -s -H 'X-Scope-OrgID: 1' $M/prometheus/config/v1/rules; echo ---;
  curl -s -H 'X-Scope-OrgID: anonymous' $M/prometheus/config/v1/rules; echo ---;
  curl -s -H 'X-Scope-OrgID: 1' $M/api/v1/alerts | head -40; echo ---;
  curl -s -H 'X-Scope-OrgID: anonymous' $M/api/v1/alerts | head -40"
```

Expected: tenant `1` has `homelab/...` namespaces (Alloy), tenant `anonymous` has `default` (mimir-sync).
Note which tenant currently has an Alertmanager config — that is what alerting has been using.

## 1. Install the operator

`apps/monitoring/alerts-operator/` (ArgoCD Application, Helm source):

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: alerts-operator
  namespace: argocd
spec:
  project: default
  destination: { server: https://kubernetes.default.svc, namespace: alerts-operator }
  source:
    repoURL: https://antnsn.github.io/alerts-operator
    chart: alerts-operator
    targetRevision: 0.1.0
    helm:
      values: |
        serviceMonitor:
          enabled: true
  syncPolicy:
    automated: { prune: true, selfHeal: true }
    syncOptions: [CreateNamespace=true, ServerSideApply=true]
```

Verify *(cluster command)*: `kubectl -n alerts-operator get deploy` → `1/1`; `kubectl get crd | grep observability.antnsn.dev` → 4 CRDs.

## 2. Secrets via ESO

Move the ExternalSecret out of `mimir-sync`. New `apps/monitoring/alerts-config/manifests/secrets.yml` (ns `monitoring`):

```yaml
apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata:
  name: keep
  namespace: monitoring
spec:
  refreshInterval: 1h
  secretStoreRef: { kind: ClusterSecretStore, name: infisical }
  target: { name: keep }
  data:
    - secretKey: api-key
      remoteRef: { key: KEEP-API-KEY }
---
apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata:
  name: pushover
  namespace: monitoring
spec:
  refreshInterval: 1h
  secretStoreRef: { kind: ClusterSecretStore, name: infisical }
  target: { name: pushover }
  data:
    - secretKey: user
      remoteRef: { key: PUSHOVER-USER }
    - secretKey: token
      remoteRef: { key: PUSHOVER-TOKEN }
```

(Use the same `secretStoreRef` as the existing `alertmanager-config` ExternalSecret in `apps/monitoring/mimir-sync/manifests/secrets.yml`.)
Verify: `kubectl -n monitoring get externalsecret keep pushover` → `SecretSynced True`.

## 3. Tenant, ContactPoints, NotificationPolicy

Copy `docs/examples/tenant.yaml`, `contactpoints.yaml`, `notificationpolicy.yaml` into `apps/monitoring/alerts-config/manifests/`.
Keep webhook URL: take the exact URL and provider id from the current `alertmanager-config` template
(`apps/monitoring/mimir-sync/manifests/secrets.yml`) — it embeds the Keep provider id, e.g.
`http://keep-backend.keep:8080/alerts/event/prometheus?provider_id=…`.

Verify *(cluster command)*:

```bash
kubectl get tenant homelab -o jsonpath='{range .status.conditions[*]}{.type}={.status} {.reason}{"\n"}{end}'
```
Expected: `AlertmanagerSynced=True Synced`, `MimirRulesSynced=True Synced`, `LokiRulesSynced=True Synced`, `Ready=True`.

```bash
curl -s -H 'X-Scope-OrgID: 1' $M/api/v1/alerts
```
Expected: `receivers:` contains `monitoring/keep` and `monitoring/pushover`; `route.receiver: monitoring/keep`.

## 4. Rules

- `apps/monitoring/mimir/manifests/rules.yml` (`PrometheusRule homelab-alerts`): convert to
  `AlertRuleGroup` `homelab` in `apps/monitoring/alerts-config/manifests/rules-mimir.yml` —
  the `spec.groups` block is copied unchanged, only `apiVersion/kind` and `tenantRef`/`backend: mimir` change.
- `apps/monitoring/mimir-sync/manifests/loki-homelab.yml` + `loki-udm.yml`: each file's `groups:` becomes
  one `AlertRuleGroup` (`loki-homelab`, `loki-udm`) with `backend: loki` in `rules-loki.yml`.

Verify *(cluster command)*:

```bash
kubectl get alertrulegroups -n monitoring
curl -s -H 'X-Scope-OrgID: 1' $M/prometheus/config/v1/rules | grep '^alerts-operator/'
curl -s -H 'X-Scope-OrgID: 1' http://loki-gateway.loki/loki/api/v1/rules | grep '^alerts-operator/'
```
Expected: `Accepted=True Synced=True` for all; namespaces `alerts-operator/monitoring/homelab`, `alerts-operator/monitoring/loki-homelab`, `alerts-operator/monitoring/loki-udm`.

At this point Mimir has the rules twice (Alloy `homelab/*` and operator `alerts-operator/*`). Alerts fire twice until step 5. Do step 5 the same day.

## 5. Remove the old writers

1. `apps/monitoring/grafana-alloy/manifests/config.alloy`: delete the whole `mimir.rules.kubernetes "kubernetes" { … }` block.
2. Delete `apps/monitoring/mimir/manifests/rules.yml` and its kustomization entry.
3. Delete `apps/monitoring/mimir-sync/` entirely (ArgoCD prunes ns `mimir-sync`, its Jobs and the old ExternalSecret).
4. If `apps/monitoring/mimir/manifests/values.yml` still mounts the dead `alertmanager-config` ConfigMap / `extraEnvFrom: pushover`, remove them.

Verify: `kubectl get ns mimir-sync` → NotFound; Alloy pod restarted and logs show no `mimir.rules.kubernetes` component.

## 6. Prune old backend state (manual, once)

Alloy's prefix and mimir-sync's namespaces are outside `alerts-operator/`, so the operator never touches them.

```bash
# tenant 1 — Alloy-owned namespaces
for ns in $(curl -s -H 'X-Scope-OrgID: 1' $M/prometheus/config/v1/rules | grep -E '^homelab/' | sed 's/:$//'); do
  curl -s -X DELETE -H 'X-Scope-OrgID: 1' "$M/prometheus/config/v1/rules/$(printf %s "$ns" | jq -sRr @uri)"
done
# tenant 1 — Loki direct-sync namespaces
for ns in loki-homelab loki-udm; do
  curl -s -X DELETE -H 'X-Scope-OrgID: 1' "http://loki-gateway.loki/loki/api/v1/rules/$ns"
done
# tenant anonymous — mimir-sync leftovers
curl -s -X DELETE -H 'X-Scope-OrgID: anonymous' $M/prometheus/config/v1/rules/default
curl -s -X DELETE -H 'X-Scope-OrgID: anonymous' $M/api/v1/alerts
```

Verify: `GET /prometheus/config/v1/rules` for tenant `1` lists only `alerts-operator/*`; for `anonymous` returns 404; Loki lists only `alerts-operator/*`.

## 7. Prove alerting end to end

Follow the test-alert steps in `docs/e2e.md` §5 (E2ETest rule → Keep + Pushover → delete → pruned).

## 8. Cleanup

- Archive `antnsn/mimir-sync` and `antnsn/mal-sync` on GitHub with a README note pointing here.
- Update the Cluster vault: remove the stale "mimir-sync loki-rules-sync failing" issue, add the `X-Scope-OrgID`/prefix gotchas.
````

- [ ] **Step 6: README**

Replace `README.md`:

````markdown
# alerts-operator

Kubernetes operator that owns alerting configuration for Grafana Mimir and Grafana Loki:
alert/recording rules for both rulers, and the per-tenant Mimir Alertmanager configuration
(receivers, routing tree, inhibition, templates). Backends are written only by the `Tenant`
reconciler; every other CR is validated and compiled into that single write.

Design: [`docs/superpowers/specs/2026-09-21-alerts-operator-design.md`](docs/superpowers/specs/2026-09-21-alerts-operator-design.md).

## Install

```bash
helm repo add antnsn https://antnsn.github.io/alerts-operator
helm install alerts-operator antnsn/alerts-operator -n alerts-operator --create-namespace
```

CRDs ship inside the chart. ArgoCD users: add the health checks from [`docs/argocd-health.md`](docs/argocd-health.md).

## Quickstart

```bash
kubectl apply -f docs/examples/tenant.yaml
kubectl create ns monitoring
kubectl apply -f docs/examples/secrets.yaml          # replace REPLACE_ME first
kubectl apply -f docs/examples/contactpoints.yaml
kubectl apply -f docs/examples/notificationpolicy.yaml
kubectl apply -f docs/examples/alertrulegroup-mimir.yaml -f docs/examples/alertrulegroup-loki.yaml
kubectl get tenants; kubectl get contactpoints,notificationpolicies,alertrulegroups -A
```

## CRDs (`observability.antnsn.dev/v1alpha1`)

| Kind | Scope | Purpose |
|---|---|---|
| `Tenant` | cluster | Backend addresses, `tenantId` (`X-Scope-OrgID`), optional auth, template ConfigMap, rule-namespace prefix, resync interval. Single writer to Mimir/Loki. |
| `ContactPoint` | namespaced | Alertmanager receiver(s): webhook, pushover, slack, discord, telegram, email. Secrets only via `secretKeyRef` in the same namespace. Becomes receiver `<namespace>/<name>`. |
| `NotificationPolicy` | namespaced | Alertmanager route tree + inhibit rules. One per Tenant (oldest wins). `receiver` names are ContactPoints in the same namespace. |
| `AlertRuleGroup` | namespaced | PrometheusRule-shaped groups for `backend: mimir` or `backend: loki`. Lands in backend namespace `<prefix>/<namespace>/<name>`. |

## Status conditions

| Kind | Condition | Meaning |
|---|---|---|
| Tenant | `Ready` | All configured targets below are `True`. |
| Tenant | `AlertmanagerSynced` | Compiled AM config matches backend. `False/NoNotificationPolicy` when no policy exists (backend untouched). |
| Tenant | `MimirRulesSynced`, `LokiRulesSynced` | Rule namespaces under the prefix match desired state. `False/BackendUnavailable` on 5xx/transport, `False/Rejected` on 4xx. |
| children | `Accepted` | References resolve (Tenant, backend, Secrets, ContactPoints), rules parse. Reasons: `TenantNotFound`, `BackendNotConfigured`, `SecretNotFound`, `ContactPointNotFound`, `Conflict`, `InvalidRule`. |
| children | `Synced` | Included in the last successful backend write. `False/Invalid` names the CR that broke compilation. |

## Guarantees and limits

- Prunes only backend rule namespaces under `<prefix>/`. Everything else in the tenant is left alone.
- Deleting a `Tenant` deletes its Alertmanager config and all rule namespaces under its prefix (finalizer).
- No Tempo support: alert on Tempo metrics-generator series via Mimir rules.
- No mute timings, no cross-namespace references (v1).

## Development

```bash
make test          # envtest + unit tests
make chart-test    # helm lint + render assertions
```
````

- [ ] **Step 7: Run checks**

Run: `make chart-test`
Expected: `chart-test: OK`.

- [ ] **Step 8: Commit**

```bash
git add docs README.md hack/chart-test.sh
git commit -m "docs: ArgoCD health checks, examples, migration runbook, README"
```

---

### Task 23: Release workflow, gh-pages, Dependabot

**Files:**
- Create: `.github/workflows/release.yml`, `.github/dependabot.yml`
- Verify: `Dockerfile` (scaffolded by kubebuilder in Task 1)

**Interfaces:**
- Consumes: `make helm-sync-crds` (Task 21); chart at `charts/alerts-operator`; kubebuilder `Dockerfile`.
- Produces: on tag `v*`: image `ghcr.io/antnsn/alerts-operator:<version-without-v>` + `:latest` (linux/amd64, linux/arm64); chart `alerts-operator-<version>.tgz` on `https://antnsn.github.io/alerts-operator` (gh-pages `index.yaml`).

- [ ] **Step 1: Confirm the Dockerfile is distroless static**

Run: `grep -nE '^(ARG BASE_IMAGE|FROM)' Dockerfile`
Expected: the builder stage resolves to `golang:1.27` (either `ARG BASE_IMAGE=golang:1.27` + `FROM ${BASE_IMAGE} AS builder`, or a literal `FROM golang:1.27 AS builder`, depending on what kubebuilder scaffolded and Task 1 pinned) and the last `FROM` is `gcr.io/distroless/static:nonroot`.
If the final stage differs, replace it with `FROM gcr.io/distroless/static:nonroot` and keep `USER 65532:65532`. The build stage honours `TARGETOS`/`TARGETARCH` build args, so buildx multi-arch works without changes.

- [ ] **Step 2: Create the gh-pages branch and enable Pages (one-off)**

The `origin` remote and `main` already exist from Task 1 (`gh repo create --push`). Build the orphan branch in a separate worktree so the `main` checkout is never touched:

```bash
git worktree add --orphan -b gh-pages /tmp/alerts-operator-gh-pages
( cd /tmp/alerts-operator-gh-pages \
  && printf '# alerts-operator Helm repo\n' > README.md \
  && git add README.md \
  && git -c commit.gpgsign=false commit -m "chore: init gh-pages" \
  && git push origin gh-pages )
git worktree remove /tmp/alerts-operator-gh-pages
gh api -X POST repos/antnsn/alerts-operator/pages -f build_type=legacy -f 'source[branch]=gh-pages' -f 'source[path]=/'
gh api repos/antnsn/alerts-operator/pages --jq .html_url
```
Expected last line: `https://antnsn.github.io/alerts-operator/`. (`git worktree add --orphan` needs git ≥ 2.42; `git --version` to check.)

Also allow Actions to write packages and contents: `gh api -X PUT repos/antnsn/alerts-operator/actions/permissions/workflow -f default_workflow_permissions=write -F can_approve_pull_request_reviews=false`.

- [ ] **Step 3: Write the workflow**

`.github/workflows/release.yml`:

```yaml
name: release
on:
  push:
    tags: ["v*"]

permissions:
  contents: write
  packages: write

env:
  IMAGE: ghcr.io/antnsn/alerts-operator

jobs:
  check-tag:
    runs-on: ubuntu-latest
    steps:
      - name: Require a SemVer tag (vX.Y.Z or vX.Y.Z-pre)
        run: |
          echo "${GITHUB_REF_NAME}" | grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$' \
            || { echo "tag ${GITHUB_REF_NAME} is not SemVer"; exit 1; }

  image:
    needs: check-tag
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-qemu-action@v3
      - uses: docker/setup-buildx-action@v3
      - uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}
      - id: meta
        uses: docker/metadata-action@v5
        with:
          images: ${{ env.IMAGE }}
          tags: |
            type=semver,pattern={{version}}
            type=raw,value=latest
      - uses: docker/build-push-action@v6
        with:
          context: .
          platforms: linux/amd64,linux/arm64
          push: true
          tags: ${{ steps.meta.outputs.tags }}
          labels: ${{ steps.meta.outputs.labels }}
          cache-from: type=gha
          cache-to: type=gha,mode=max

  chart:
    needs: [check-tag, image]
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0
      - uses: actions/setup-go@v5
        with:
          go-version-file: go.mod
      - uses: azure/setup-helm@v4
      - name: Sync CRDs into chart
        run: make helm-sync-crds
      - name: Set chart version from tag
        run: |
          VERSION="${GITHUB_REF_NAME#v}"
          sed -i "s/^version: .*/version: ${VERSION}/" charts/alerts-operator/Chart.yaml
          sed -i "s/^appVersion: .*/appVersion: \"${VERSION}\"/" charts/alerts-operator/Chart.yaml
          helm lint charts/alerts-operator
      - name: Configure git
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
      - uses: helm/chart-releaser-action@v1
        with:
          charts_dir: charts
          skip_existing: true
        env:
          CR_TOKEN: ${{ secrets.GITHUB_TOKEN }}
```

Notes for the reader:
- `docker/metadata-action` with `type=semver,pattern={{version}}` strips the `v`: tag `v0.1.0` → image `ghcr.io/antnsn/alerts-operator:0.1.0`. That equals the chart's `appVersion` (`0.1.0`), which is the chart's default image tag, so the packaged chart resolves to an existing image with no `values.yaml` rewrite.
- chart-releaser creates a GitHub Release `alerts-operator-<version>` with the `.tgz` and updates `index.yaml` on `gh-pages`.

`.github/dependabot.yml`:

```yaml
version: 2
updates:
  - package-ecosystem: gomod
    directory: /
    schedule: { interval: weekly }
    groups:
      k8s: { patterns: ["k8s.io/*", "sigs.k8s.io/*"] }
      prometheus: { patterns: ["github.com/prometheus/*"] }
  - package-ecosystem: github-actions
    directory: /
    schedule: { interval: weekly }
  - package-ecosystem: docker
    directory: /
    schedule: { interval: weekly }
```

- [ ] **Step 4: Validate**

```bash
yq e . .github/workflows/release.yml >/dev/null && echo yaml-ok
yq e . .github/dependabot.yml >/dev/null && echo yaml-ok
command -v actionlint >/dev/null || brew install actionlint
actionlint
```
Expected: two `yaml-ok`, `actionlint` prints nothing (exit 0).

- [ ] **Step 5: Commit**

```bash
git add .github Dockerfile
git commit -m "ci: release workflow for multi-arch image and Helm chart, dependabot"
```

---

### Task 24: Dev image and home-cluster e2e runbook

**Files:**
- Create: `docs/e2e.md`
- Modify: `Makefile` (add `deploy-dev`, `undeploy-dev`), `README.md` (Development section)

**Interfaces:**
- Consumes: chart (Task 21), examples (Task 22), kubebuilder `docker-build`/`docker-push`/`docker-buildx` targets (`IMG` variable, `PLATFORMS` variable).
- Produces: `make deploy-dev` → operator running from `ghcr.io/antnsn/alerts-operator:dev` in ns `alerts-operator` of the current kube-context; `docs/e2e.md` manual acceptance checklist.

- [ ] **Step 1: Makefile targets**

Append to `Makefile` under `##@ Helm`:

```make
DEV_IMG ?= ghcr.io/antnsn/alerts-operator:dev
DEV_NS  ?= alerts-operator

.PHONY: deploy-dev
deploy-dev: helm-sync-crds ## Build+push :dev for the cluster's node arch(s) and install the chart.
	$(MAKE) docker-buildx IMG=$(DEV_IMG) PLATFORMS=$(DEV_PLATFORMS)
	helm upgrade --install alerts-operator $(CHART_DIR) -n $(DEV_NS) --create-namespace \
		--set image.tag=dev --set image.pullPolicy=Always --wait

.PHONY: undeploy-dev
undeploy-dev: ## Uninstall the dev release. CRDs (and every CR) stay; see purge-dev-crds.
	helm uninstall alerts-operator -n $(DEV_NS) || true

.PHONY: purge-dev-crds
purge-dev-crds: ## Delete the CRDs — refuses while any Tenant/ContactPoint/NotificationPolicy/AlertRuleGroup exists.
	@n=$$(kubectl get tenants,contactpoints,notificationpolicies,alertrulegroups -A --no-headers 2>/dev/null | wc -l | tr -d ' '); \
	if [ "$$n" != "0" ]; then echo "refusing: $$n alerts-operator CRs still exist (delete them first so finalizers clean the backends)"; exit 1; fi
	kubectl delete crd tenants.observability.antnsn.dev contactpoints.observability.antnsn.dev \
		notificationpolicies.observability.antnsn.dev alertrulegroups.observability.antnsn.dev --ignore-not-found
```

The scaffolded `docker-buildx` target prefixes its `buildx build --push` line with `-`, which makes Make ignore a failed build and `deploy-dev` would then install a stale image. Edit that target in the Makefile: remove the leading `-` from the `$(CONTAINER_TOOL) buildx build ...` line only (keep it on the `buildx create` and `buildx rm` lines, which are best-effort cleanup). Verify with `grep -n 'buildx build' Makefile` → line starts with a tab, no `-`.

`DEV_PLATFORMS` defaults from node architectures:

```make
DEV_PLATFORMS ?= $(shell kubectl get nodes -o jsonpath='{range .items[*]}linux/{.status.nodeInfo.architecture}{"\n"}{end}' 2>/dev/null | sort -u | paste -sd, -)
```
(put this line above `deploy-dev`). Verify: `make -n deploy-dev` prints `docker-buildx IMG=ghcr.io/antnsn/alerts-operator:dev PLATFORMS=linux/amd64` (or `linux/amd64,linux/arm64`).

`docker-buildx` needs a ghcr login once: `echo $GITHUB_TOKEN | docker login ghcr.io -u antnsn --password-stdin` (token with `write:packages`; `gh auth token` works if the gh scope includes it).

- [ ] **Step 2: Runbook**

`docs/e2e.md`:

````markdown
# E2E on the home cluster

No kind. The acceptance run is against the real Mimir (`mimir-distributed-nginx.mimir:80`) and
Loki (`loki-gateway.loki`), but under its own backend tenant `e2e` (Mimir/Loki multitenancy keeps
its rules and Alertmanager config apart from production tenant `1`). Kubernetes objects live in a
throwaway namespace `e2e`, the Tenant CR is named `e2e`, and the rule prefix is `e2e`.
Keep and Pushover credentials are shared with production, so the test alert really notifies.

Shell helper used throughout (runs curl inside the cluster):

```bash
mcurl() { kubectl -n e2e run curl-$RANDOM --rm -i --restart=Never --image=curlimages/curl:8.10.1 -- curl -s "$@"; }
M=http://mimir-distributed-nginx.mimir:80
L=http://loki-gateway.loki
```

## 1. Deploy

- [ ] `kubectl config current-context` shows the home cluster.
- [ ] `kubectl get nodes -o wide` — note `ARCH` column; `make -n deploy-dev` shows matching `PLATFORMS=`.
- [ ] `make deploy-dev` → ends with `STATUS: deployed`.
- [ ] `kubectl -n alerts-operator get pods` → `1/1 Running`.
- [ ] `kubectl -n alerts-operator logs deploy/alerts-operator | grep -c 'Starting workers'` → `4`.

## 2. Tenant

- [ ] `kubectl create ns e2e`
- [ ] Apply a Tenant with backend tenant `e2e` and prefix `e2e`; production tenant `1` is never touched:

```bash
kubectl apply -f - <<'Y'
apiVersion: observability.antnsn.dev/v1alpha1
kind: Tenant
metadata: { name: e2e }
spec:
  tenantId: "e2e"
  mimir: { address: http://mimir-distributed-nginx.mimir:80 }
  loki: { address: http://loki-gateway.loki }
  alertmanager: {}
  rulesNamespacePrefix: e2e
  resyncInterval: 1m
Y
```
- [ ] `kubectl get tenant e2e -o jsonpath='{range .status.conditions[*]}{.type}={.status}/{.reason}{"\n"}{end}'` →
  `AlertmanagerSynced=False/NoNotificationPolicy`, `MimirRulesSynced=True/Synced`, `LokiRulesSynced=True/Synced`, `Ready=False/...`.
- [ ] `kubectl get tenant e2e -o jsonpath='{.metadata.finalizers}'` → `["observability.antnsn.dev/tenant"]`.
- [ ] `mcurl -o /dev/null -w '%{http_code}\n' -H 'X-Scope-OrgID: e2e' $M/api/v1/alerts` → `404` (operator must not write AM config without a policy).

## 3. Contact points + policy

- [ ] Create secrets from Infisical values (or paste): `kubectl -n e2e create secret generic keep --from-literal=api-key=… ; kubectl -n e2e create secret generic pushover --from-literal=user=… --from-literal=token=…`
- [ ] `sed 's/namespace: monitoring/namespace: e2e/; s/tenantRef: homelab/tenantRef: e2e/' docs/examples/contactpoints.yaml | kubectl apply -f -`
- [ ] `kubectl -n e2e get contactpoints` → both `Accepted=True`, `Synced` empty/False (no policy yet).
- [ ] `sed 's/namespace: monitoring/namespace: e2e/; s/tenantRef: homelab/tenantRef: e2e/' docs/examples/notificationpolicy.yaml | kubectl apply -f -`
- [ ] Within ~5s: `kubectl -n e2e get contactpoints,notificationpolicies` → all `Accepted=True Synced=True`; Tenant `AlertmanagerSynced=True`.
- [ ] `mcurl -H 'X-Scope-OrgID: e2e' $M/api/v1/alerts` → `receiver: e2e/keep`, receivers `e2e/keep`, `e2e/pushover`, `authorization: {credentials: …}` on the webhook.
- [ ] `mcurl -o /dev/null -w '%{http_code}\n' -H 'X-Scope-OrgID: 1' $M/api/v1/alerts` — same status as before this run (production tenant untouched).

- [ ] Negative: `kubectl -n e2e delete secret pushover` → ContactPoint `pushover` `Accepted=False/SecretNotFound`. The policy still routes to `pushover`, so the compile fails: NotificationPolicy `Synced=False/Invalid` (message names receiver `pushover`), Tenant `AlertmanagerSynced=False/Invalid`, and the backend keeps the previous document (`mcurl -H 'X-Scope-OrgID: e2e' $M/api/v1/alerts` still lists `e2e/pushover`). Recreate the secret → everything back to `True` within ~5s.

## 4. Rules

- [ ] `sed 's/namespace: monitoring/namespace: e2e/; s/tenantRef: homelab/tenantRef: e2e/' docs/examples/alertrulegroup-mimir.yaml docs/examples/alertrulegroup-loki.yaml | kubectl apply -f -`
- [ ] `kubectl -n e2e get alertrulegroups` → `Accepted=True Synced=True`; `.status.backendNamespace` = `e2e/e2e/homelab` and `e2e/e2e/udm`.
- [ ] `mcurl -H 'X-Scope-OrgID: e2e' $M/prometheus/config/v1/rules | grep '^e2e/'` → `e2e/e2e/homelab:`.
- [ ] `mcurl -H 'X-Scope-OrgID: e2e' $L/loki/api/v1/rules | grep '^e2e/'` → `e2e/e2e/udm:`.
- [ ] Negative: patch bad PromQL: `kubectl -n e2e patch alertrulegroup homelab --type=json -p '[{"op":"replace","path":"/spec/groups/0/rules/0/expr","value":"up{job=\\""}]'` → `Accepted=False/InvalidRule`, message names group `node.health` rule `0`. Revert with the apply above.
- [ ] Drift repair: `mcurl -X DELETE -H 'X-Scope-OrgID: e2e' "$M/prometheus/config/v1/rules/e2e%2Fe2e%2Fhomelab"`; within `resyncInterval` (1m) the namespace is back.

## 5. Fire a real alert

- [ ] Apply:

```bash
kubectl apply -f - <<'Y'
apiVersion: observability.antnsn.dev/v1alpha1
kind: AlertRuleGroup
metadata: { name: e2etest, namespace: e2e }
spec:
  tenantRef: e2e
  backend: mimir
  groups:
    - name: e2e
      interval: 15s
      rules:
        - alert: E2ETest
          expr: vector(1)
          for: 0m
          labels: { severity: critical }
          annotations: { summary: "alerts-operator e2e" }
Y
```
- [ ] Within ~2 min: `mcurl -H 'X-Scope-OrgID: e2e' $M/prometheus/api/v1/alerts | grep -c E2ETest` → `1` (ruler firing).
- [ ] `mcurl -H 'X-Scope-OrgID: e2e' $M/alertmanager/api/v2/alerts | grep -c E2ETest` → `1` (AM received).
- [ ] Keep UI shows `E2ETest`; Pushover notification arrives (critical route).
- [ ] `kubectl -n e2e delete alertrulegroup e2etest` → `mcurl -H 'X-Scope-OrgID: e2e' $M/prometheus/config/v1/rules | grep -c 'e2e/e2e/e2etest'` → `0`. Alert resolves; Keep shows resolved.

## 6. Metrics

- [ ] `kubectl -n alerts-operator port-forward svc/alerts-operator-metrics 8080 &` then
  `curl -s localhost:8080/metrics | grep alerts_operator_sync_total` → counters with `tenant="e2e"`, `target="mimir_rules"`, `result="ok"`.

## 7. Teardown

- [ ] `kubectl delete tenant e2e` → object gone within 10s (the finalizer deletes tenant `e2e`'s Alertmanager config and every `e2e/*` rule namespace in both backends).
- [ ] Verify: `mcurl -o /dev/null -w '%{http_code}\n' -H 'X-Scope-OrgID: e2e' $M/prometheus/config/v1/rules` → `404`; same for `$L/loki/api/v1/rules`; `mcurl -o /dev/null -w '%{http_code}\n' -H 'X-Scope-OrgID: e2e' $M/api/v1/alerts` → `404`.
- [ ] Fallback if the Tenant is stuck (backend down during delete) and you must clean up by hand:

```bash
mcurl -X DELETE -H 'X-Scope-OrgID: e2e' $M/api/v1/alerts
for ns in $(mcurl -H 'X-Scope-OrgID: e2e' $M/prometheus/config/v1/rules | grep -E '^e2e/' | sed 's/:$//'); do
  mcurl -X DELETE -H 'X-Scope-OrgID: e2e' "$M/prometheus/config/v1/rules/$(printf %s "$ns" | jq -sRr @uri)"
done
for ns in $(mcurl -H 'X-Scope-OrgID: e2e' $L/loki/api/v1/rules | grep -E '^e2e/' | sed 's/:$//'); do
  mcurl -X DELETE -H 'X-Scope-OrgID: e2e' "$L/loki/api/v1/rules/$(printf %s "$ns" | jq -sRr @uri)"
done
kubectl patch tenant e2e --type=json -p '[{"op":"remove","path":"/metadata/finalizers"}]'
```
- [ ] `kubectl delete ns e2e`
- [ ] `make undeploy-dev` (only if not going straight to the production install in docs/migration.md). `make purge-dev-crds` afterwards only on a cluster with no other alerts-operator CRs.

Record the run date and any deviations in the vault session note.
````

- [ ] **Step 3: Verify Makefile**

Run: `make -n deploy-dev | head -3 && make -n undeploy-dev | head -1 && make -n purge-dev-crds | head -1`
Expected: shows the `docker-buildx` and `helm upgrade --install` lines, then `helm uninstall`, then the CR-count guard.

- [ ] **Step 4: README**

In `README.md`, replace the `## Development` code block with:

```bash
make test          # envtest + unit tests
make chart-test    # helm lint + render assertions
make deploy-dev    # push :dev image and install into the current kube-context (docs/e2e.md)
make undeploy-dev  # remove the dev release (CRDs and CRs stay)
make purge-dev-crds # delete the CRDs; refuses while any CR exists
```

- [ ] **Step 5: Commit**

```bash
git add Makefile docs/e2e.md README.md
git commit -m "build: deploy-dev target and home-cluster e2e runbook"
```

---

### Task 25: Release v0.1.0

**Files:**
- Create: `CHANGELOG.md`
- Modify: none (chart version already 0.1.0)

**Interfaces:**
- Consumes: release workflow (Task 23), chart (Task 21), all tests.
- Produces: tag `v0.1.0`; image `ghcr.io/antnsn/alerts-operator:0.1.0` (+`latest`); chart `alerts-operator-0.1.0` on the Helm repo.

- [ ] **Step 1: E2E done**

`docs/e2e.md` checklist completed on the home cluster with no unresolved deviations. If not, stop here and file `bd` issues.

- [ ] **Step 2: Changelog**

`CHANGELOG.md`:

```markdown
# Changelog

All notable changes to this project are documented here. Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Versioning: [SemVer](https://semver.org/).

## [0.1.0] - YYYY-MM-DD

### Added
- CRDs `observability.antnsn.dev/v1alpha1`: `Tenant` (cluster-scoped), `ContactPoint`, `NotificationPolicy`, `AlertRuleGroup`.
- Tenant reconciler as single writer: compiles one Alertmanager document per tenant and rule namespaces `<prefix>/<namespace>/<name>` for Mimir and Loki; diffs against the backend; prunes only under the prefix; drift repair on `resyncInterval`.
- Receivers: webhook, pushover, slack, discord, telegram, email — secrets via `secretKeyRef` only.
- Local validation with Alertmanager's config loader and the PromQL parser; errors attributed to the responsible CR.
- Status conditions `Ready`, `AlertmanagerSynced`, `MimirRulesSynced`, `LokiRulesSynced` (Tenant) and `Accepted`, `Synced` (children); Kubernetes Events on failures.
- Metrics `alerts_operator_sync_total`, `alerts_operator_last_sync_timestamp_seconds`.
- Helm chart with CRDs, RBAC, metrics Service and optional ServiceMonitor; ArgoCD health Lua; migration and e2e runbooks.

### Not included
- Tempo (no ruler), mute timings, cross-namespace references, admission webhooks.

[0.1.0]: https://github.com/antnsn/alerts-operator/releases/tag/v0.1.0
```
Replace `YYYY-MM-DD` with today's date.

- [ ] **Step 3: Quality gates**

```bash
go test ./... && golangci-lint run ./... && make chart-test
bd list --status open
```
Expected: all pass. Every open `bd` issue is either closed now or carries a comment `post-0.1.0` and stays open.

- [ ] **Step 4: Commit, tag, push**

```bash
git add CHANGELOG.md
git commit -m "chore: release 0.1.0"
git pull --rebase
bd dolt push
git push
git tag -a v0.1.0 -m "alerts-operator 0.1.0"
git push origin v0.1.0
gh run watch --exit-status
```
Expected: `gh run watch` ends with `✓ check-tag`, `✓ image` and `✓ chart`.

- [ ] **Step 5: Verify artifacts**

```bash
docker manifest inspect ghcr.io/antnsn/alerts-operator:0.1.0 | grep -E '"architecture"' | sort -u
helm repo add antnsn https://antnsn.github.io/alerts-operator && helm repo update antnsn && helm search repo antnsn/alerts-operator
gh release view alerts-operator-0.1.0 --json assets --jq '.assets[].name'
```
Expected:
```
"architecture": "amd64",
"architecture": "arm64",
NAME                    CHART VERSION  APP VERSION  DESCRIPTION
antnsn/alerts-operator  0.1.0          0.1.0        Kubernetes operator for ...
alerts-operator-0.1.0.tgz
```
If `helm repo add` 404s: Pages can lag a minute after chart-releaser pushes `index.yaml`; retry. If the image is private: `gh api -X PATCH /user/packages/container/alerts-operator/visibility -f visibility=public` (or set in the package settings UI).

- [ ] **Step 6: Hand off**

Vault: `Sessions/<date>.md` with the release, and `Open Issues.md` → the cluster migration (`docs/migration.md`) is the next job. `git status` → `up to date with 'origin/main'`.

---

## Appendix: RBAC rules (for Helm chart)

Consolidated from the `+kubebuilder:rbac` markers in Tasks 15–18; must equal `config/rbac/role.yaml` after `make manifests`.

```yaml
rules:
  - apiGroups: ["observability.antnsn.dev"]
    resources: [tenants]
    verbs: [get, list, watch, update, patch]
  - apiGroups: ["observability.antnsn.dev"]
    resources: [tenants/status]
    verbs: [get, update, patch]
  - apiGroups: ["observability.antnsn.dev"]
    resources: [tenants/finalizers]
    verbs: [update]
  - apiGroups: ["observability.antnsn.dev"]
    resources: [contactpoints, notificationpolicies, alertrulegroups]
    verbs: [get, list, watch]
  - apiGroups: ["observability.antnsn.dev"]
    resources: [contactpoints/status, notificationpolicies/status, alertrulegroups/status]
    verbs: [get, update, patch]
  - apiGroups: [""]
    resources: [secrets, configmaps]
    verbs: [get, list, watch]
  - apiGroups: [""]
    resources: [events]
    verbs: [create, patch]
```

Note for the chart: kubebuilder also scaffolds `leader-election-role` (Role, namespaced: `configmaps`, `coordination.k8s.io/leases` get/list/watch/create/update/patch/delete; `events` create/patch). Copy it into the chart as a namespaced Role + RoleBinding when `leaderElection.enabled`.
