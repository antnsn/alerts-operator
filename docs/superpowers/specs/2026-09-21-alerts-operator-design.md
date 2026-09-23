# alerts-operator — Design

Date: 2026-09-21
Status: approved (brainstorm), pending implementation plan

## 1. Purpose

Kubernetes operator that owns alerting configuration for Grafana Mimir and Grafana Loki:

- Alert/recording rules for Mimir ruler and Loki ruler.
- Mimir Alertmanager configuration (receivers, routing, inhibition, templates).

It is a learning/portfolio project. It replaces, in the home cluster, the Alloy
`mimir.rules.kubernetes` rule sync and the retired `mimir-sync` Jobs.

Out of scope for v1: Tempo (no ruler; trace alerting is Tempo metrics-generator →
Mimir metrics → Mimir rules), mute timings, cross-namespace references, Grafana
(unified alerting) targets, multi-tenant Alertmanager silences.

## 2. API — `observability.antnsn.dev/v1alpha1`

### Tenant (cluster-scoped)

```yaml
apiVersion: observability.antnsn.dev/v1alpha1
kind: Tenant
metadata:
  name: homelab
spec:
  tenantId: "1"                       # sent as X-Scope-OrgID on every backend call
  mimir:                              # optional; enables Alertmanager + Mimir rules
    address: http://mimir-distributed-nginx.mimir:80
    auth:                             # optional
      basicAuthSecretRef: { namespace: mimir, name: mimir-auth }   # keys: username, password
  loki:                               # optional; enables Loki rules
    address: http://loki-gateway.loki
    auth: { basicAuthSecretRef: { namespace: loki, name: loki-auth } }
  alertmanager:
    templatesRef: { namespace: mimir, name: am-templates }   # optional ConfigMap → template_files
  rulesNamespacePrefix: alerts-operator   # default "alerts-operator"
  resyncInterval: 5m                      # default 5m; drift repair period
status:
  observedGeneration: 3
  conditions:
    - type: Ready                 # all configured targets synced
    - type: AlertmanagerSynced
    - type: MimirRulesSynced
    - type: LokiRulesSynced
  alertmanagerConfigHash: sha256:…
  ruleGroups: { mimir: 6, loki: 4 }
```

CEL: at least one of `spec.mimir` / `spec.loki`. `spec.alertmanager` requires `spec.mimir`.

### ContactPoint (namespaced)

```yaml
kind: ContactPoint
metadata: { name: keep, namespace: monitoring }
spec:
  tenantRef: homelab
  webhook:
    - url: http://keep-backend.keep:8080/alerts/event/prometheus
      urlSecretRef: { name: keep, key: url }          # alternative to url
      httpConfig:
        bearerTokenSecretRef: { name: keep, key: api-key }
        basicAuth: { usernameSecretRef, passwordSecretRef }
      sendResolved: true
  pushover:
    - userKeySecretRef: { name: pushover, key: user }
      tokenSecretRef:   { name: pushover, key: token }
      priority: "1"
      title: '{{ template "pushover.default.title" . }}'
  slack:    [{ apiURLSecretRef, channel, title, text, sendResolved }]
  discord:  [{ webhookURLSecretRef, title, message }]
  telegram: [{ botTokenSecretRef, chatID, parseMode, message }]
  email:    [{ to, from, smarthost, authUsername, authPasswordSecretRef, requireTLS }]
status:
  conditions:
    - type: Accepted     # tenantRef resolves, all secretKeyRefs resolve, ≥1 receiver
    - type: Synced       # present in backend AM config (set by Tenant reconciler)
```

Field set mirrors Alertmanager receiver configs (same naming as prometheus-operator
`AlertmanagerConfig`). Every secret-bearing field is a `secretKeyRef` into a Secret
in the ContactPoint's namespace; inline secrets are not allowed. CEL: at least one
receiver type present. Backend receiver name = `<namespace>/<name>`.

### NotificationPolicy (namespaced, one per Tenant)

```yaml
kind: NotificationPolicy
metadata: { name: homelab, namespace: monitoring }
spec:
  tenantRef: homelab
  route:                       # Alertmanager route tree
    receiver: keep             # ContactPoint name in the same namespace
    groupBy: [alertname, namespace]
    groupWait: 30s
    groupInterval: 5m
    repeatInterval: 4h
    matchers: []               # Alertmanager matcher syntax, e.g. 'severity="critical"'
    continue: false
    routes:
      - receiver: pushover
        matchers: ['severity="critical"']
  inhibitRules:
    - sourceMatchers: ['severity="critical"']
      targetMatchers: ['severity="warning"']
      equal: [alertname, namespace]
status:
  conditions: [Accepted, Synced]
```

Exactly one NotificationPolicy per Tenant is used. If several reference the same
Tenant, the oldest (`creationTimestamp`, then name) wins; others get
`Accepted=False reason=Conflict`. CEL: `route.receiver` required.

### AlertRuleGroup (namespaced)

```yaml
kind: AlertRuleGroup
metadata: { name: homelab, namespace: monitoring }
spec:
  tenantRef: homelab
  backend: mimir               # enum: mimir | loki
  groups:                      # PrometheusRule-compatible; existing rules paste in
    - name: node.health
      interval: 1m
      rules:
        - alert: NodeDown
          expr: up{job="node"} == 0
          for: 5m
          labels: { severity: critical }
          annotations: { summary: "{{ $labels.instance }} down" }
        - record: job:up:ratio
          expr: avg by (job) (up)
status:
  conditions: [Accepted, Synced]
  backendNamespace: alerts-operator/monitoring/homelab   # mimir; loki would be alerts-operator_monitoring_homelab
```

Backend rule namespace = `<prefix>/<k8s-namespace>/<name>` for `backend: mimir`, and
`<prefix>_<k8s-namespace>_<name>` for `backend: loki`. The separator differs because Loki's ruler
HTTP router matches on the *decoded* path: `%2F` becomes a path-segment boundary, so any namespace
containing a literal `/` produces more segments than any registered pattern has and every
per-namespace route (POST, DELETE, per-group GET) 404s. Verified against live Loki 3.6.7:
`POST /loki/api/v1/rules/flatns` → 202, `POST /loki/api/v1/rules/e2e%2Fe2e%2Fudm` → 404. Mimir's
ruler routes on the raw escaped path and is unaffected, so its scheme is unchanged. `_` is the
separator for Loki because a Kubernetes namespace or object name can contain `-` and `.` freely but
never `_`; `spec.rulesNamespacePrefix` is pattern-restricted to exclude `_` for the same reason, so
`<prefix>_<ns>_<name>` is unambiguous and ownership can be matched on a true segment boundary.
Group names unique
within one CR (`listType=map` on `groups`). `expr` is a string: PrometheusRule's bare-number
form (`expr: 1`, IntOrString) must be quoted (`expr: "1"`); otherwise groups paste in unchanged. `backend: loki` requires `tenant.spec.loki`; `mimir` requires
`tenant.spec.mimir` (checked at reconcile).

## 3. Reconcile model

**Single writer.** Only the Tenant reconciler talks to backends. Child reconcilers
validate and set `Accepted`, then enqueue their Tenant. Children have no finalizers.

### Child reconcilers (ContactPoint, NotificationPolicy, AlertRuleGroup)

1. Resolve `tenantRef`; missing → `Accepted=False reason=TenantNotFound`.
2. Backend enabled on tenant (rule groups) → else `Accepted=False reason=BackendNotConfigured`.
3. Resolve every `secretKeyRef` (ContactPoint) → missing → `Accepted=False reason=SecretNotFound` (message names secret/key).
4. NotificationPolicy: every `receiver` in the route tree resolves to a ContactPoint in the same namespace with the same `tenantRef` → else `Accepted=False reason=ContactPointNotFound`. Uniqueness per tenant as above.
5. AlertRuleGroup `backend: mimir`: each `expr` parses with `github.com/prometheus/prometheus/promql/parser`; durations parse. `backend: loki`: syntax left to Loki (error surfaced via Synced). → `Accepted=False reason=InvalidRule` with rule index.
6. Patch status, enqueue Tenant.

### Tenant reconciler

1. If deleting → finalizer (below). Else ensure finalizer.
2. List children with `tenantRef == name` via field index; use only `Accepted=True`.
3. **Alertmanager** (if `spec.mimir` set):
   - No accepted NotificationPolicy → `AlertmanagerSynced=False reason=NoNotificationPolicy`; leave backend untouched.
   - Else `compile.Alertmanager(policy, contactPoints, secretValues, templates)` → YAML.
     All accepted ContactPoints of the tenant (any namespace) become receivers, even if
     the route tree does not reference them (Alertmanager allows unreferenced receivers;
     they still get `Synced=True`).
     `{alertmanager_config: <string>, template_files: {…}}`. Validate the inner
     config with `github.com/prometheus/alertmanager/config`; on error attribute to
     the offending ContactPoint/Policy (`Synced=False reason=Invalid`).
   - sha256 of compiled document. If hash == `status.alertmanagerConfigHash` and
     resync not due → skip. Else `GET /api/v1/alerts`, compare; if different `POST`.
4. **Rules** per backend present (`mimir`, `loki`):
   - `compile.Rules(backend, groups)` → map `backendNamespace → []RuleGroup` (the backend
     also selects the namespace separator; see §2).
   - `List()` all namespaces/groups for tenant (bulk endpoint only — sufficient for the
     whole diff, and the read path proven on the live cluster. Loki's per-group GET does
     work; an earlier claim that it returned a malformed 404 was a misdiagnosis of the
     embedded-slash problem above).
   - For each desired namespace whose groups differ → `SetGroup` per group; delete
     groups in that namespace not desired.
   - Delete namespaces with the operator prefix not in desired set (prune).
5. Patch `Synced` on each child used. Patch Tenant conditions, hash, counts,
   `observedGeneration`. `RequeueAfter: spec.resyncInterval`.

Independent conditions: Mimir failing does not block Loki sync and vice versa.

### Watches / triggers

- ContactPoint, NotificationPolicy, AlertRuleGroup → own reconciler; create/update/delete
  events also map to Tenant (mapper reads `spec.tenantRef` from the event object).
- Secret → ContactPoints in that namespace referencing it (index on referenced secret names).
- ConfigMap (templatesRef) → Tenant.
- Tenant → own reconcile; also re-enqueue all children on Tenant spec change (so `Accepted` re-evaluates).

### Finalizer (Tenant)

On delete: `DELETE /api/v1/alerts` (if mimir), delete every rule namespace under
`rulesNamespacePrefix` on each configured backend, then remove finalizer. Backend
unreachable → keep finalizer, backoff, emit Event. Never delete Alertmanager config
in any other path.

### Errors

| Situation | Condition | Requeue |
|---|---|---|
| Backend unreachable / 5xx | `Synced=False reason=BackendUnavailable` | exponential backoff (controller-runtime default) |
| Backend 4xx (validation) | `Synced=False reason=Rejected`, message = backend body (truncated) + Event | on next change / resync |
| Local compile/validate error | `Synced=False reason=Invalid` on the child at fault | on next change |
| Secret/ref missing | `Accepted=False` on child; Tenant compiles without it | on ref appearing (watch) |

Metrics (Prometheus, controller-runtime registry):
`alerts_operator_sync_total{tenant,target=alertmanager|mimir_rules|loki_rules,result=ok|error}`,
`alerts_operator_last_sync_timestamp_seconds{tenant,target}`, standard
controller-runtime metrics. Kubernetes Events on every failure.

## 4. Backend clients — `internal/backend`

```go
type RuleGroup struct { Name string; Interval string; Rules []Rule }  // yaml-compatible

type RuleStore interface {
    List(ctx) (map[string][]RuleGroup, error)          // namespace → groups
    SetGroup(ctx, namespace string, g RuleGroup) error // create-or-replace
    DeleteGroup(ctx, namespace, group string) error
    DeleteNamespace(ctx, namespace string) error
}

type AlertmanagerStore interface {
    Get(ctx) (*AlertmanagerConfig, error)   // nil, nil when unset (404)
    Set(ctx, *AlertmanagerConfig) error
    Delete(ctx) error
}
```

- `backend/mimir`: rules at `/prometheus/config/v1/rules[/{ns}[/{group}]]`,
  Alertmanager at `/api/v1/alerts`. Implements both interfaces.
- `backend/loki`: rules at `/loki/api/v1/rules[/{ns}[/{group}]]`. RuleStore only.
- Plain `net/http`; YAML request/response bodies (`sigs.k8s.io/yaml`); `X-Scope-OrgID`
  header; optional basic auth; per-request timeout (default 30s); path segments
  URL-escaped (namespaces contain `/`).
- `backend/fake`: in-memory `httptest.Server` speaking both Mimir and Loki paths,
  with fault injection (status code, latency). Used by client tests and envtest.
- No `mimirtool`/`lokitool`; image is distroless static.

## 5. Repository layout

```
cmd/main.go                          kubebuilder entrypoint
api/v1alpha1/                        types, CEL markers, deepcopy
internal/controller/                 tenant, contactpoint, notificationpolicy, alertrulegroup
internal/compile/                    pure: CRs → Alertmanager doc / rule groups (golden tests)
internal/backend/{mimir,loki,fake}   clients + fake server
internal/index/                      field indexers (tenantRef, secret names)
config/                              kubebuilder kustomize (source for CRD/RBAC generation)
charts/alerts-operator/              Helm chart; crds/ copied from config/crd at release
docs/argocd-health.md                Lua health checks for the four kinds
docs/examples/                       Tenant + ContactPoint + Policy + rule groups
docs/migration.md                    home-cluster migration runbook
CLAUDE.md, AGENTS.md, .beads/
```

Go 1.27, kubebuilder v4 layout, controller-runtime, CEL validation on CRDs
(no admission webhook server).

## 6. Testing

- `internal/compile`: table tests + golden YAML files.
- `internal/backend/{mimir,loki}`: tests against `backend/fake`.
- `internal/controller`: envtest (real API server, CRDs) + `backend/fake`;
  cover happy path, missing refs, conflict policy, prune, finalizer, backend down.
- E2E: home cluster (no kind). Runbook in `docs/migration.md`:
  `make docker-build docker-push IMG=ghcr.io/antnsn/alerts-operator:dev`,
  deploy chart, apply `docs/examples`, verify with `curl -H 'X-Scope-OrgID: 1'`
  against Mimir/Loki APIs and a fired test alert reaching Keep/Pushover.

## 7. CI / release

GitHub Actions:
- PR: `golangci-lint`, `go test ./...` (envtest assets via `setup-envtest`), `helm lint`.
- Tag `v*`: multi-arch (amd64, arm64) image → `ghcr.io/antnsn/alerts-operator`;
  chart → `antnsn.github.io/alerts-operator` via chart-releaser (same flow as mimir-sync).
- Dependabot: gomod, github-actions, docker.

## 8. Home-cluster migration (follow-up, in `antnsn/cluster`)

1. `apps/monitoring/alerts-operator/` — chart via ArgoCD; Lua health customizations in argocd-cm.
2. `Tenant homelab`: mimir `http://mimir-distributed-nginx.mimir:80`, loki `http://loki-gateway.loki`, tenantId `1`.
3. `PrometheusRule homelab-alerts` → `AlertRuleGroup backend: mimir`.
4. `loki-homelab.yml` / `loki-udm.yml` → `AlertRuleGroup backend: loki`.
5. ESO Secret `alertmanager-config` → Secrets for Keep + Pushover; `ContactPoint keep`, `ContactPoint pushover`, `NotificationPolicy homelab`.
6. Remove Alloy `mimir.rules.kubernetes` block; delete `apps/monitoring/mimir-sync/`.
7. Manual prune of pre-existing backend state: Mimir namespaces `homelab/*` (Alloy prefix), Loki `loki-homelab`/`loki-udm`, everything under tenant `anonymous`.

## 9. Decisions recorded

- Own CRDs, Grafana-style object model, instead of reusing PrometheusRule/AlertmanagerConfig (portfolio clarity, no Alloy overlap).
- Sync functions return an error to controller-runtime (backoff) only for unavailable backends (transport/5xx); 4xx and compile errors set conditions and wait for the next change or resync.
- Cluster-scoped Tenant + namespaced children (platform owns connection, teams own content).
- Single-writer Tenant reconciler (Alertmanager config is one document per tenant; prune and atomicity are simpler).
- Receiver schema mirrors Alertmanager natively; secrets only via `secretKeyRef`.
- No admission webhooks; CEL + reconcile-time validation.
- Direct HTTP clients; no mimirtool/lokitool shellouts.
- Tempo excluded from v1.
