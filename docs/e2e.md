# E2E on the home cluster

> This is a manual acceptance runbook against real Mimir/Loki, deliberately run on the home
> cluster under a dedicated, isolated tenant (see below). It is not the kubebuilder-scaffolded
> `AGENTS.md` "E2E Tests Require an Isolated Kind Cluster" guidance, which refers to a Ginkgo
> `test/e2e/` suite this project does not have (only `internal/controller/suite_test.go`, an
> envtest suite, exists). Kind cannot exercise this project's actual integration surface — real
> Mimir, real Loki, real Alertmanager, real Infisical-backed secrets — which is exactly what this
> task exists to validate. See "Facts established" / blast-radius scoping in the Task 24 report
> for how this was kept safe.

Run executed 2026-09-22 against commit `2ff8497`, image `ghcr.io/antnsn/alerts-operator:sha-2ff8497`.

> **§4's Loki assertions are stale and were never re-run.** They were observed against the broken
> `/`-joined Loki namespace scheme. The fix for that (`alerts-operator-b4o`) has landed but has
> **not** been verified on a live cluster — see §8 for what still has to be proven.

No kind. The acceptance run is against the real Mimir (`mimir-distributed-nginx.mimir:80`) and
Loki (`loki-gateway.loki`), but under its own backend tenant `e2e` (Mimir/Loki multitenancy keeps
its rules and Alertmanager config apart from production tenant `1`, and production tenant
`anonymous` holds the real Alertmanager config — both were verified untouched before and after
this run). Kubernetes objects live in a throwaway namespace `e2e`, the Tenant CR is named `e2e`,
and the rule prefix is `e2e`.

**Deviations from the original design, and why:**

- **Secrets come from Infisical, not `--from-literal`.** ContactPoint secrets (`keep`, `pushover`)
  are synced into the `e2e` namespace via `ExternalSecret` objects against the existing
  `ClusterSecretStore/infisical`, the same pattern already used by the `keep` and `mimir-sync`
  namespaces. No credential is ever pasted on a command line. See step 3 below for the exact
  shape (key names only, no values).
- **Keep is being retired; the "fire a real alert" test does not depend on it, or on paging the
  real Pushover account.** The original runbook asserted "Keep UI shows E2ETest; Pushover
  notification arrives". Keep's production instance is not guaranteed to be running, and the
  `pushover` ContactPoint's credentials in Infisical are the user's real, production Pushover
  account — there is no sandbox/test Pushover credential. Firing a real push notification to the
  user's phone as a side effect of an unattended, unobserved e2e run is not something this task
  should do silently. Instead, a throwaway `echo` receiver (a single `mendhak/http-https-echo` pod
  + Service in the `e2e` namespace) is deployed, and the `NotificationPolicy`'s `severity="critical"`
  route is pointed at it *only for the duration of the live-fire test* (§5), then reverted. This
  keeps the test fully self-contained while still proving a real HTTP delivery through Mimir
  Alertmanager (not just a compiled config document). The `keep`/`pushover` ContactPoints and the
  production-shaped `NotificationPolicy` are still created and validated in §3 exactly as
  documented — those assertions are entirely about the compiled Alertmanager config document
  reaching Mimir, which needs nothing running on the receiving end.

Shell helper used throughout (runs curl inside the cluster):

```bash
mcurl() { kubectl -n e2e run curl-$RANDOM --rm -i --restart=Never --image=curlimages/curl:8.10.1 -- curl -s "$@"; }
M=http://mimir-distributed-nginx.mimir:80
L=http://loki-gateway.loki
```

## 1. Deploy — PASSED

- `kubectl config current-context` → `Home`.
- Commit under test: `2ff8497` (already the tip of `main`, clean, CI green). Image tag:
  `sha-2ff8497`.
- Pull check (image is distroless with no shell, so only the pull event is checked, not exec):

  ```bash
  kubectl run pull-check --image=ghcr.io/antnsn/alerts-operator:sha-2ff8497 --restart=Never --image-pull-policy=Always
  sleep 12 && kubectl describe pod pull-check | grep -E 'Pulled|Failed|BackOff'
  kubectl delete pod pull-check --ignore-not-found
  ```
  Observed: `Normal Pulled ... Successfully pulled image "ghcr.io/antnsn/alerts-operator:sha-2ff8497" ... Image size: 38345056 bytes.` No `Failed`/`BackOff`.
- Package visibility: already public (verified this session before the run started); no
  `imagePullSecret` needed. The brief's "first publish only" pull-secret fallback was not used.
- `make deploy-dev DEV_IMG_TAG=sha-2ff8497` → `STATUS: deployed`, `REVISION: 1`.
- `kubectl -n alerts-operator get pods` → `alerts-operator-5fbb496c55-zps67  1/1  Running`.
- `kubectl -n alerts-operator logs deploy/alerts-operator | grep -c 'Starting workers'` → `4`.

**Finding (Makefile, not the operator):** `make -n deploy-dev` on a clean checkout (no cached
`bin/controller-gen`) prints the `controller-gen` bootstrap/download recipe lines *before* the
`helm upgrade` line, because `deploy-dev` depends on `helm-sync-crds` → `manifests` → `controller-gen`,
and `controller-gen`'s recipe is for the real (non-phony) file target `bin/controller-gen-v0.22.0`,
which didn't exist locally. `make -n deploy-dev | head -2` therefore shows the bootstrap check, not
the `helm upgrade --install` line, unless that binary is already cached. This only affects `make -n`
dry-run output on a fresh clone; the real `make deploy-dev` run above worked correctly (it just also
downloads `controller-gen` first, which is expected, normal behavior). No code change made for this;
noting it because the brief's Step 3 verification command assumes a warm cache.

## 2. Tenant — PASSED

```bash
kubectl create ns e2e
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

Observed:

```
MimirRulesSynced=True/Synced
AlertmanagerSynced=False/NoNotificationPolicy
LokiRulesSynced=True/Synced
Ready=False/NoNotificationPolicy
```

- `kubectl get tenant e2e -o jsonpath='{.metadata.finalizers}'` → `["observability.antnsn.dev/tenant"]`.
- `mcurl -o /dev/null -w '%{http_code}\n' -H 'X-Scope-OrgID: e2e' $M/api/v1/alerts` → `404`.

## 3. Contact points + policy — PASSED (with a secret-recreation deviation, see below)

Secrets from Infisical (no values pasted or logged), following the same pattern as the `keep`
namespace's `ExternalSecret` and the `mimir-sync` namespace's `alertmanager-config`
`ExternalSecret` (whose remote key names were read to find the real Infisical key names):

```yaml
apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata: { name: keep, namespace: e2e }
spec:
  secretStoreRef: { kind: ClusterSecretStore, name: infisical }
  refreshInterval: 1h
  target: { name: keep, creationPolicy: Owner }
  data:
    - secretKey: api-key
      remoteRef: { key: KEEP-API-KEY }
---
apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata: { name: pushover, namespace: e2e }
spec:
  secretStoreRef: { kind: ClusterSecretStore, name: infisical }
  refreshInterval: 1h
  target: { name: pushover, creationPolicy: Owner }
  data:
    - secretKey: user
      remoteRef: { key: PUSHOVER-USER }
    - secretKey: token
      remoteRef: { key: PUSHOVER-TOKEN }
```

Both synced (`STATUS: SecretSynced`, `READY: True`) within 5s.

```bash
sed 's/namespace: monitoring/namespace: e2e/; s/tenantRef: homelab/tenantRef: e2e/' docs/examples/contactpoints.yaml | kubectl apply -f -
```

→ `contactpoint.../keep created`, `contactpoint.../pushover created`; both `Accepted=True`,
`Synced` empty (no policy yet) — matches expected.

```bash
sed 's/namespace: monitoring/namespace: e2e/; s/tenantRef: homelab/tenantRef: e2e/' docs/examples/notificationpolicy.yaml | kubectl apply -f -
```

Within 5s: both ContactPoints and the NotificationPolicy `Accepted=True Synced=True`; Tenant
`AlertmanagerSynced=True/Synced`.

`mcurl -H 'X-Scope-OrgID: e2e' $M/api/v1/alerts` → compiled config contains `receiver: e2e/keep`
at the route root, `receivers: [e2e/keep, e2e/pushover]`, and `authorization: {credentials: <redacted>}`
on the `e2e/keep` webhook — exactly as expected. (The real credential value was visible in the raw
response during verification, as it must be for Alertmanager to work; it is not reproduced here or
anywhere else in this report.)

`mcurl -o /dev/null -w '%{http_code}\n' -H 'X-Scope-OrgID: 1' $M/api/v1/alerts` → `200` (same as the
pre-run baseline — production tenant `1` untouched).

**Negative test — with one adaptation.** `kubectl -n e2e delete secret pushover` alone does **not**
reliably reproduce `SecretNotFound`: because the secret is owned by an `ExternalSecret` with
`creationPolicy: Owner`, the external-secrets-operator's own watch recreated it in ~1-2s — faster
than could be reliably observed, and faster than the brief's assumed ~5s window. To get a stable
window, the `ExternalSecret`'s `remoteRef.key` for the token was first patched to a nonexistent key
(so ESO's own sync fails and it can't recreate the secret), then the secret was deleted:

```bash
kubectl -n e2e patch externalsecret pushover --type=json \
  -p '[{"op":"replace","path":"/spec/data/1/remoteRef/key","value":"PUSHOVER-TOKEN-DOES-NOT-EXIST"}]'
kubectl -n e2e delete secret pushover
```

Observed, stable for 8+ seconds: ContactPoint `pushover` `Accepted=False/SecretNotFound`;
NotificationPolicy `Synced=False/Invalid`, message `receiver "pushover" not found as ContactPoint
e2e/pushover`; Tenant `AlertmanagerSynced=False/Invalid`; backend still lists `e2e/pushover` in
`$M/api/v1/alerts` (stale document kept, not wiped) — exactly as the brief describes. Reverting the
`ExternalSecret`'s key back to `PUSHOVER-TOKEN` brought everything back to `True` within 1s.

## 4. Rules — PASSED for Mimir, BLOCKED for Loki (namespace-scheme collision; fixed in §8, not yet re-run)

**Finding in the runbook itself:** the brief's exact command,

```bash
sed 's/.../e2e/; s/.../e2e/' docs/examples/alertrulegroup-mimir.yaml docs/examples/alertrulegroup-loki.yaml | kubectl apply -f -
```

passes *two filenames* to `sed`, which concatenates their output with **no `---` document
separator** between them. `kubectl apply -f -` then merges the two flat YAML mappings into one
(later duplicate keys — `apiVersion`, `kind`, `metadata`, `spec` — win), so only the *second* file's
object (`udm`) was actually created; `homelab` silently never was. Applying each file separately
fixed this. Recommend the runbook (and any similar multi-file `sed | kubectl apply` invocation) use
`(sed ... file1; echo ---; sed ... file2) | kubectl apply -f -`, or apply the files one at a time.

With that fixed:

- `homelab` (Mimir): `Accepted=True Synced=True`, `.status.backendNamespace=e2e/e2e/homelab`. ✓
- `udm` (Loki): `Accepted=True`, **`Synced=False/Invalid`**, message `loki: backend returned 404: 404 page not found`.

Investigated this Loki failure by hand (bypassing the operator, hitting Loki directly, and hitting
the gateway) rather than assuming it was our client's bug. It turned out to be a collision between
Loki's routing and *this operator's* namespace convention, and the convention is the side that got
fixed — see §8:

- The nginx gateway *does* proxy `/loki/api/v1/rules/` to `loki.loki.svc.cluster.local:3100` — not
  a gateway routing gap.
- Hitting Loki directly, `POST /loki/api/v1/rules/{ns}` for a **single-segment** namespace (no
  embedded `/`) reaches the handler correctly (`400` on a deliberately-invalid body — i.e. it's
  parsed and rejected, proving the route exists).
- The same POST for a namespace containing an embedded `/` (URL-escaped as `%2F`, e.g.
  `e2e%2Fe2e%2Fhomelab`, the shape this operator always uses:
  `rulesNamespacePrefix/CR-namespace/CR-name`) returns **404 with no request even reaching Loki's
  access-logging middleware** — i.e. the route plainly doesn't match once Go's `net/http` has
  decoded `%2F` back into a literal `/` and the path has more segments than the registered pattern
  expects. A 1-embedded-slash namespace (2 segments) instead returns `405 Method Not Allowed` on
  POST, matching the `{namespace}/{group}` (GET/DELETE-single-group) pattern — evidence this
  Loki build's ruler HTTP router treats every literal `/` as a path-segment boundary rather than
  routing on the raw (still-escaped) path, so a namespace **must be a single path segment** to be
  writable or individually readable/deletable through this API.
- Corroborating evidence: production tenant `1`'s existing Loki rules use the single-segment
  namespace `loki-homelab` (no embedded `/`) — whoever set that up already worked around this same
  limitation.
- On this Loki deployment, **every per-namespace/per-group ruler endpoint** — GET-single,
  POST-create and DELETE — is unreachable for any namespace containing a literal `/`. Since this
  operator's namespace scheme was `rulesNamespacePrefix/namespace/name`, **the Loki backend could
  not sync any AlertRuleGroup.** This is a real, reproducible finding about the target environment
  colliding with the operator's namespace convention, not a flake — confirmed twice, with `curl`
  entirely independent of the operator.
- **Correction to what this repo previously believed.** `CLAUDE.md`, the design spec and the
  `internal/backend/loki` comments all used to say that Loki 3.6.7's per-group
  `GET /loki/api/v1/rules/{ns}/{group}` returns a malformed 404 and must never be called. That was a
  **misdiagnosis of this same slash problem**. Re-tested against the live cluster on 2026-09-23 with
  five slash-free separators (`-`, `_`, `.`, `:`, `__`): every one gives `POST=202`, **per-group
  `GET=200`**, `DELETE=202`. The embedded `/` was always the sole cause. The operator still reads
  only the bulk `GET /loki/api/v1/rules` — it is sufficient for the whole diff and already proven —
  but for sufficiency, not because the per-group route is broken.
- Given the size of the fix (either changing the Loki-side namespacing convention to avoid
  embedded slashes, or getting this Loki instance's ruler routing fixed/upgraded) and that this task
  is scoped to observing real behavior, no code change was made. The `udm` AlertRuleGroup was
  deleted (`kubectl -n e2e delete alertrulegroup udm`) rather than left in a permanently-failed
  state; it has no finalizer of its own (single-writer model — the Tenant reconciler owns all
  backend calls), so deletion was immediate and clean.
- Remaining §4 assertions were run against the Mimir (`homelab`) rule group only:
  - `mcurl $M/prometheus/config/v1/rules | grep '^e2e/'` → `e2e/e2e/homelab:`. ✓
  - `mcurl $L/loki/api/v1/rules | grep '^e2e/'` → **not run as a pass/fail check** — confirmed
    instead (see above) that it correctly returns `no rule groups found` for tenant `e2e`, since
    nothing was ever actually written.
  - Negative test (bad PromQL) on `homelab`: `Accepted=False/InvalidRule`, message
    `group node.health rule 0: expr: 1:8: parse error: unterminated quoted string` — exact match.
    Reverted; back to `True/True`.
  - Drift repair: `mcurl -X DELETE ... $M/prometheus/config/v1/rules/e2e%2Fe2e%2Fhomelab` → `202`;
    namespace confirmed gone immediately, then reappeared on its own within 40s (inside the 1m
    `resyncInterval`).

### §4 Loki steps: NOT RE-RUN — pending live verification

The Loki half of §4 above is still recorded as it was observed on 2026-09-22, against the broken
namespace scheme. A fix has landed on `main` (`_` as the Loki namespace separator — see §8) but
**nobody has re-run these steps against a live cluster**, so nothing here may be read as a pass.

## 5. Fire a real alert — PASSED (via a throwaway echo receiver, see deviations above)

Deployed a throwaway receiver and pointed the critical route at it only for this test:

```bash
kubectl apply -f - <<'Y'   # Deployment + Service "echo" in e2e, image mendhak/http-https-echo:31
...
Y
kubectl apply -f - <<'Y'   # ContactPoint "echo": webhook http://echo.e2e.svc.cluster.local:8080/
...
Y
# NotificationPolicy homelab: route severity="critical" -> receiver "echo" (replacing the
# pushover/keep routes for the duration of this test only)
```

Applied the example E2ETest rule exactly as specified (`vector(1)`, `severity: critical`,
`for: 0m`). Within 20s:

- `mcurl $M/prometheus/api/v1/alerts | grep -c E2ETest` → `1` (ruler firing).
- `mcurl $M/alertmanager/api/v2/alerts | grep -c E2ETest` → `1` (AM received).
- Echo receiver logs show the real webhook body Alertmanager sent:
  `"receiver":"e2e/echo","status":"firing","alerts":[{"status":"firing","labels":{"alertname":"E2ETest","severity":"critical"}, ...}]` —
  i.e. a genuine end-to-end delivery through Mimir Alertmanager to a live HTTP receiver, not just a
  compiled document.
- `kubectl -n e2e delete alertrulegroup e2etest` → `mcurl $M/prometheus/config/v1/rules | grep -c 'e2e/e2e/e2etest'` → `0` within 10s. Ruler no longer reports the alert (`grep -c E2ETest` on
  `$M/prometheus/api/v1/alerts` → `0`).
- Resolution: ~180s after the rule group was deleted, echo received a second webhook:
  `"receiver":"e2e/echo","status":"resolved","alerts":[{"status":"resolved","labels":{"alertname":"E2ETest",...}}]`.
  (Alertmanager's default `resolve_timeout`, not overridden by this Tenant, gates how quickly a
  no-longer-refreshed alert is declared resolved and pushed out; it took ~3 minutes here, not the
  ~2 minutes the original brief's timing implied for the whole section, but it did arrive.)
- Keep UI and a real Pushover push were **not** checked, deliberately (see deviations above); the
  `keep`/`pushover` ContactPoints' config-document behavior was already fully verified in §3.

## 6. Metrics — PASSED

```bash
kubectl -n alerts-operator port-forward svc/alerts-operator-metrics 8080:8080 &
curl -s localhost:8080/metrics | grep alerts_operator_sync_total
```

Observed:

```
alerts_operator_sync_total{result="error",target="loki_rules",tenant="e2e"} 21
alerts_operator_sync_total{result="ok",target="alertmanager",tenant="e2e"} 19
alerts_operator_sync_total{result="ok",target="loki_rules",tenant="e2e"} 54
alerts_operator_sync_total{result="ok",target="mimir_rules",tenant="e2e"} 75
```

`tenant="e2e", target="mimir_rules", result="ok"` present as expected. The `loki_rules` `error`
counter (21) is the numeric fingerprint of the §4 finding above; `loki_rules` `ok` (54) corresponds
to the bulk-`GET` reconcile reads, which do succeed.

## 7. Teardown — PASSED

- Reverted the NotificationPolicy's critical route back to `pushover`/`keep` (the production-shaped
  policy verified in §3) before deleting anything, so the tenant's last-known state matches what §3
  validated.
- `kubectl delete tenant e2e` → object gone within 10s (single `kubectl delete` call, no fallback
  manual cleanup needed).
- Verify: `$L/loki/api/v1/rules` and `$M/api/v1/alerts` for `X-Scope-OrgID: e2e` → `404` as
  expected. `$M/prometheus/config/v1/rules` → **`200` with body `{}`**, not `404` — this matches
  the Mimir quirk already documented in `docs/migration.md` §"Prove alerting end to end" (a tenant
  with zero rule groups returns `200 {}` on this cluster's Mimir, not `404`; Loki does return `404`
  for the same "nothing left" situation). Confirmed empty (no leftover `e2e/*` namespaces), which is
  what matters.
- `kubectl -n e2e delete deploy/echo svc/echo` (throwaway receiver removed).
- `kubectl delete ns e2e`.
- `make undeploy-dev` → release uninstalled (not proceeding straight to a production install in
  this session; Task 25 does that separately).
- `make purge-dev-crds` → no CRs of any kind remained, so it deleted the four CRDs, returning the
  cluster to its pre-task state.

Confirmed before and after the whole run, production tenants `1` and `anonymous` are byte-for-byte
unchanged:

| Tenant | Endpoint | Before | After |
|---|---|---|---|
| `1` | `/api/v1/alerts` | `200`, 43B | `200`, 43B |
| `1` | `/prometheus/config/v1/rules` | `200`, 111100B | `200`, 111100B |
| `anonymous` | `/api/v1/alerts` | `200`, 3097B | `200`, 3097B |
| `anonymous` | `/prometheus/config/v1/rules` | `200`, 10550B | `200`, 10550B |

## Summary of deviations from the original brief

1. Secrets via `ExternalSecret`/Infisical, not `--from-literal` (ruling for this task).
2. Live-delivery proof via a throwaway `echo` receiver instead of Keep UI + a real Pushover push
   (ruling for this task, extended to Pushover too — see §5 rationale).
3. `sed file1 file2 | kubectl apply -f -` silently drops one of two AlertRuleGroup examples for
   lack of a `---` separator — worked around by applying files separately (found this session).
4. `kubectl delete secret <eso-owned>` alone does not reliably test `SecretNotFound` because ESO
   recreates it in ~1-2s — worked around by also breaking the `ExternalSecret`'s `remoteRef` (found
   this session).
5. The Loki backend could not sync any rule group on this cluster's Loki, because the operator's
   namespace scheme embedded a literal `/` and this Loki version's ruler HTTP router 404s on any
   per-namespace route once that `/` is present (found this session; root-caused independently of
   the operator with direct `curl`). A code fix has since landed (Loki rule namespaces are joined
   with `_`), but **it has not been verified against a live cluster** — see §8.

## 8. Loki namespace separator fix — PENDING LIVE VERIFICATION

**Status: not verified against a live cluster.** The fix below is committed and covered by unit
tests, but the cluster was unreachable when the change was made (home power outage; `kubectl get
nodes` → `Unable to connect to the server: context deadline exceeded`), so **no step in this
section has been executed against real Mimir/Loki.** Treat every Loki assertion in §4 as
outstanding until this section is filled in with observed output.

### What changed

`alerts-operator-b4o`. A Loki rule namespace is now `<prefix>_<k8s-namespace>_<name>` instead of
`<prefix>/<k8s-namespace>/<name>`. Mimir is untouched and keeps `/` — its ruler handles embedded
slashes and its scheme was verified working in §4. `spec.rulesNamespacePrefix` now rejects `_`, so
the separator stays unambiguous (Kubernetes namespace and object names cannot contain `_`).

The old "Loki 3.6.7's per-group GET returns a malformed 404" claim was a **misdiagnosis** and has
been removed from `CLAUDE.md`, the design spec, `docs/migration.md` and the `internal/backend/loki`
comments. Per-group GET works (verified `200` against live Loki 3.6.7 on 2026-09-23, with five
slash-free separators). The operator still reads only the bulk `GET /loki/api/v1/rules`, because
one bulk read covers the whole diff — not because the per-group route is broken.

### What must be proven once the cluster is back

Run against the commit that carries the fix, deploying the `:sha-<short>` image the `dev-image`
job builds for it (not `:dev`). Namespace `e2e`, backend tenant `e2e`, prefix `e2e`.

1. `make deploy-dev` at the fix commit's `sha-` tag.
2. Apply `docs/examples/alertrulegroup-loki.yaml` rewritten for the `e2e` namespace/tenant
   (apply each example file separately — see deviation 3).
3. `kubectl -n e2e get alertrulegroup udm -o yaml` → `Accepted=True`, **`Synced=True`**, and
   `.status.backendNamespace` = `e2e_e2e_udm` (no `/`).
4. `mcurl -H 'X-Scope-OrgID: e2e' $L/loki/api/v1/rules | grep '^e2e_'` → lists `e2e_e2e_udm`.
5. Drift repair: `mcurl -X DELETE -H 'X-Scope-OrgID: e2e' $L/loki/api/v1/rules/e2e_e2e_udm` →
   expect `202` (not `404` — that 404 was the whole bug), then confirm the namespace reappears
   within the `resyncInterval`.
6. Teardown as in §7, then `make undeploy-dev` and `make purge-dev-crds`.

**Production tenants `1` and `anonymous` must never be written or deleted.** Capture byte sizes of
`/api/v1/alerts` and `/prometheus/config/v1/rules` for both, before and after, and compare against
the §7 table (`1` = 43 / 111100, `anonymous` = 3097 / 10550). Stop and report if any differ.

| Tenant | Endpoint | Before | After |
|---|---|---|---|
| `1` | `/api/v1/alerts` | _not captured_ | _not captured_ |
| `1` | `/prometheus/config/v1/rules` | _not captured_ | _not captured_ |
| `anonymous` | `/api/v1/alerts` | _not captured_ | _not captured_ |
| `anonymous` | `/prometheus/config/v1/rules` | _not captured_ | _not captured_ |

### What the unit tests do and do not prove

Covered now, and not before:

- `compile.BackendNamespace(loki, …)` and every key `compile.Rules(loki, …)` produces contains no
  `/`; `status.backendNamespace` on a Loki `AlertRuleGroup` contains no `/`.
- `backend/fake` models Loki's router: a slash-containing namespace 404s on POST/DELETE/GET
  (one embedded slash instead collides with the `{ns}/{group}` route and yields 405), exactly as
  the live ruler behaves, so this class of bug now fails in `go test`.
- Prune and finalizer ownership match on a segment boundary per backend; reverting the guard to a
  bare `strings.HasPrefix` fails those tests on both backends (verified by doing it).

Not covered, and only a real cluster can settle it:

- That real Loki 3.6.7 accepts `e2e_e2e_udm` through the gateway and stores it — the fake is a
  model of Loki's routing built from `curl` observations, not Loki.
- That LogQL in a real rule group validates server-side and the group actually evaluates.
- That the operator reaches `Synced=True` end to end against the live gateway, with real
  `X-Scope-OrgID` handling and auth.
- That production tenants `1` and `anonymous` are untouched by a run of the new code.

The original defect was invisible to twenty tasks' worth of unit tests precisely because the fake
accepted what real Loki rejects. The new tests close that specific gap; they do not make a live run
unnecessary.
