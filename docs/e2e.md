# E2E on the home cluster

> This is a manual acceptance runbook against real Mimir/Loki, deliberately run on the home
> cluster under a dedicated, isolated tenant (see below). It is not the kubebuilder-scaffolded
> `AGENTS.md` "E2E Tests Require an Isolated Kind Cluster" guidance, which refers to a Ginkgo
> `test/e2e/` suite this project does not have (only `internal/controller/suite_test.go`, an
> envtest suite, exists). Kind cannot exercise this project's actual integration surface — real
> Mimir, real Loki, real Alertmanager, real Infisical-backed secrets — which is exactly what this
> task exists to validate. See the "Production tenants ... before/after proof" and "Deliberate
> deviations from the brief" sections of `.superpowers/sdd/2026-09-21-alerts-operator/task-24-report.md`
> for how this was kept safe.

Run executed 2026-09-22 against commit `2ff8497`, image `ghcr.io/antnsn/alerts-operator:sha-2ff8497`,
for §1–§7. §8 and §9 were executed in a **second, later run on 2026-09-23 against commit `d843315`,
image `ghcr.io/antnsn/alerts-operator:sha-d843315`** (the `v0.1.0` release gate), after the cluster
recovered from a home power outage — see those two sections below for the full observed evidence
(exact commands, output, and the per-field canonicalisation table are inline in §8/§9, not only in
an external file). Both passed; no canonicalisation loop was found in either backend. A superset of
this evidence (same facts, more raw log excerpts) also exists at
`.superpowers/sdd/2026-09-21-alerts-operator/live-verification-report.md`, this project's
established location for this kind of run report (see e.g. `task-24-report.md` in the same
directory) — like every other file there, it is gitignored and local to the machine that ran the
verification, not part of this commit; §8/§9 below are the authoritative, checked-in record.

> **§4's Loki assertions are stale and were never re-run** (as of the 2026-09-22 run). They were
> observed against the broken `/`-joined Loki namespace scheme. The fix for that
> (`alerts-operator-b4o`, commit `67b2239`) landed and **has since been verified on a live
> cluster in §8** (2026-09-23, commit `d843315`) — §4's Loki subsection above is still written up
> against the old scheme and has not itself been edited, but the fix it was waiting on is now
> proven end to end.

> **Tenant stuck `Terminating` mid-teardown?** Skip to "§7 Teardown → If the Tenant delete doesn't
> complete" for the manual-cleanup fallback. This is the one procedure in this doc you're likely to
> need out of order, not front-to-back.

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
namespace scheme. A fix has landed on `main` (`_` as the Loki namespace separator — commit `67b2239`, see §8)
but **nobody has re-run these steps against a live cluster**, so nothing here may be read as a
pass.

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
  manual cleanup needed this run — but see the fallback procedure immediately below, kept in this
  doc for the run where it *is* needed).

### If the Tenant delete doesn't complete (fallback — not exercised this run)

`kubectl delete tenant e2e` normally finishes in seconds: the finalizer deletes the Alertmanager
config and every `e2e/*`/`e2e_*` rule namespace in both backends, then removes itself. If a backend
is unreachable at delete time, the finalizer can't confirm cleanup and the Tenant stays
`Terminating` indefinitely (the operator's retry-forever policy — this is expected behavior, not a
bug; see `alerts-operator-l57`/`docs/migration.md` Appendix B for the analogous missing-Secret
case). If you need to clean up by hand in that situation:

```bash
mcurl -X DELETE -H 'X-Scope-OrgID: e2e' $M/api/v1/alerts
for ns in $(mcurl -H 'X-Scope-OrgID: e2e' $M/prometheus/config/v1/rules | grep -E '^e2e/' | sed 's/:$//'); do
  mcurl -X DELETE -H 'X-Scope-OrgID: e2e' "$M/prometheus/config/v1/rules/$(printf %s "$ns" | jq -sRr @uri)"
done
for ns in $(mcurl -H 'X-Scope-OrgID: e2e' $L/loki/api/v1/rules | grep -E '^e2e_' | sed 's/:$//'); do
  mcurl -X DELETE -H 'X-Scope-OrgID: e2e' "$L/loki/api/v1/rules/$(printf %s "$ns" | jq -sRr @uri)"
done
kubectl patch tenant e2e --type=json -p '[{"op":"remove","path":"/metadata/finalizers"}]'
```

(Mimir rule namespaces are still prefixed `e2e/` — grep `^e2e/`; Loki rule namespaces are now
`e2e_` — grep `^e2e_`, per the §8 separator fix. If you're running this against a pre-`67b2239`
build, use `^e2e/` for both.) This is a manual override of the finalizer's own safety check —
only use it once you've confirmed the backend really is unreachable, not as a first resort.

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
   with `_`) and **has since been verified end to end against a live cluster** — see §8 (run of
   2026-09-23, commit `d843315`).

## 8. Loki namespace separator fix — PASSED (live-verified 2026-09-23)

**Status: verified against a live cluster.** Run executed 2026-09-23 against commit `d843315`
(tip of `main` at the time), image `ghcr.io/antnsn/alerts-operator:sha-d843315`. `kubectl get
nodes` showed 5 of 6 `Ready`; one node, `bigboy`, was `NotReady,SchedulingDisabled`. This was
**not independently confirmed as pre-existing or intentional** — it does not match the brief's
"6 nodes Ready" — but every Mimir/Loki pod was `Running` regardless, so it did not block or affect
anything in this run. See the "Discrepancy noted" section of
`.superpowers/sdd/2026-09-21-alerts-operator/live-verification-report.md` for the full note.
`make deploy-dev DEV_IMG_TAG=sha-d843315` deployed cleanly (`STATUS: deployed`, `REVISION: 1`;
operator pod `Running`, correct image, no errors in logs at startup).

Every step below was executed exactly as specified, namespace `e2e`, backend tenant `e2e`, prefix
`e2e`:

1. Applied `docs/examples/alertrulegroup-loki.yaml` (rewritten for `e2e`/`e2e`) against a freshly
   created `Tenant e2e`. Result: `Accepted=True`, **`Synced=True`**,
   `.status.backendNamespace: e2e_e2e_udm` — exactly as required.
2. `mcurl -H 'X-Scope-OrgID: e2e' $L/loki/api/v1/rules` → top-level key `e2e_e2e_udm:` present,
   matching `^e2e_`.
3. `mcurl -X DELETE -H 'X-Scope-OrgID: e2e' $L/loki/api/v1/rules/e2e_e2e_udm` → **`202`** (the old
   bug returned `404` here). Immediately after, `$L/loki/api/v1/rules` → `no rule groups found`
   (namespace genuinely gone). Polling every 5s, the operator restored `e2e_e2e_udm` within **~20s**
   — well inside the 1m `resyncInterval` — and the `AlertRuleGroup`'s `Synced` condition remained
   `True` throughout (delete-and-restore was faster than one status-poll interval).

No deviations needed this run. §8 is fully PASSED with the fix in place; the old "Loki 3.6.7's
per-group GET returns a malformed 404" claim (already corrected in CLAUDE.md/spec/code comments)
is not reintroduced by anything observed here.

**Original (pre-fix) status text, kept for history:** not verified against a live cluster. The fix
below is committed and covered by unit tests, but the cluster was unreachable when the change was
made (home power outage; `kubectl get nodes` → `Unable to connect to the server: context deadline
exceeded`), so no step in this section had been executed against real Mimir/Loki. Treat every Loki
assertion in §4 as historical until this section was filled in with observed output above.

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

### What was proven, and against what

The commit verified is **`d843315`** (tip of `main` at run time, contains `67b2239` and all
subsequent work), image `ghcr.io/antnsn/alerts-operator:sha-d843315`. Namespace `e2e`, backend
tenant `e2e`, prefix `e2e`. All six steps below were executed; see the numbered list above for
observed output. Cleanup (§ below) also completed: `kubectl delete tenant e2e` (finalizer swept
all three rule namespaces created this run, see §9), `kubectl delete ns e2e`, `make undeploy-dev`,
`make purge-dev-crds` — all succeeded, no CRs left blocking the CRD purge.

**Production tenants `1` and `anonymous` were never written or deleted.** Byte sizes of
`/api/v1/alerts`, `/prometheus/config/v1/rules` and `/loki/api/v1/rules` for both, captured before
any change and again after full teardown:

| Tenant | Endpoint | Before | After |
|---|---|---|---|
| `1` | `/api/v1/alerts` | `200`, 43B | `200`, 43B |
| `1` | `/prometheus/config/v1/rules` | `200`, 111100B | `200`, 111100B |
| `1` | `/loki/api/v1/rules` | `200`, 3429B | `200`, 3429B |
| `anonymous` | `/api/v1/alerts` | `200`, 3097B | `200`, 3097B |
| `anonymous` | `/prometheus/config/v1/rules` | `200`, 10550B | `200`, 10550B |
| `anonymous` | `/loki/api/v1/rules` | `404`, 21B | `404`, 21B |

Every HTTP status code and response size (`Content-Length`, via `curl -w '%{size_download}'`) is
identical before and after — measured via status/size only, not a body hash or full-content diff
(neither the `1` nor `anonymous` response bodies were captured this run, so this is strong but not
absolute evidence against a same-length content change). No stop condition was triggered.

### What the unit tests do and do not prove

Covered now, and not before:

- `compile.BackendNamespace(loki, …)` and every key `compile.Rules(loki, …)` produces contains no
  `/`; `status.backendNamespace` on a Loki `AlertRuleGroup` contains no `/`.
- `backend/fake` models Loki's router: a slash-containing namespace 404s on POST/DELETE/GET
  (one embedded slash instead collides with the `{ns}/{group}` route and yields 405), exactly as
  the live ruler behaves, so this class of bug now fails in `go test`.
- Prune and finalizer ownership match on a segment boundary per backend; reverting the guard to a
  bare `strings.HasPrefix` fails those tests on both backends (verified by doing it).

Was not covered by unit tests, and needed a real cluster to settle — **now settled, live, 2026-09-23,
commit `d843315`** (see the numbered steps above):

- That real Loki 3.6.7 accepts `e2e_e2e_udm` through the gateway and stores it — the fake is a
  model of Loki's routing built from `curl` observations, not Loki. **Settled: it does** (step 1/2
  above — `Synced=True`, `backendNamespace: e2e_e2e_udm`, present in the live bulk `GET`).
- That LogQL in a real rule group validates server-side and the group actually evaluates. **Settled:
  it does, and it actually runs, not just parses** — direct evidence from `loki-0`'s ruler log
  (`kubectl -n loki logs loki-0 --all-containers`): `rule_name=UDMErrorBurst ... msg="evaluating
  rule"` followed by an execution log line with `status=200`, repeating roughly once a minute from
  `10:54:56Z` through teardown; the same for `canonl`'s `ZebraLogAlert`/`AlphaLogAlert` from
  `10:56:18Z` onward. Every evaluation of these three rules in the log window returned `status=200`;
  zero `level=error` lines for any of them. (Zero matching log lines were returned by the query
  itself — there is no real `host="udm"` traffic on this cluster — which is a successful empty
  result, not a failure; distinct from the query never having run at all.)
- That the operator reaches `Synced=True` end to end against the live gateway, with real
  `X-Scope-OrgID` handling and auth. **Settled: it does** — step 1 above, and sustained across the
  delete/restore cycle in step 3.
- That production tenants `1` and `anonymous` are untouched by a run of the new code. **Settled, by
  HTTP status and response size** (not a body-content diff — see the note under the before/after
  table above) — they are.

The original defect was invisible to twenty tasks' worth of unit tests precisely because the fake
accepted what real Loki rejects. The new tests close that specific gap; they do not make a live run
unnecessary.

## 9. Backend canonicalisation of rule-group fields — PASSED, no loop found (live-verified 2026-09-23)

**Status: verified against a live cluster.** Run executed 2026-09-23 against commit `d843315`,
image `ghcr.io/antnsn/alerts-operator:sha-d843315`, folded into the same `e2e`/`e2e`/`e2e` session
as §8. **No canonicalisation loop was found in either backend** for any of the three previously-open
questions (`interval` defaulting, rule ordering, `expr` formatting). Full evidence below; see also
`.superpowers/sdd/2026-09-21-alerts-operator/live-verification-report.md` for the raw request logs.

### Method

Two `AlertRuleGroup`s were posted with **no `interval` field at all** (the common case this section
worried about), deliberately non-canonical durations, deliberately out-of-order rules, and
deliberately whitespace-padded `expr` strings:

- **Mimir** (`canonm`, namespace `e2e`): three rules in the order `ZebraAlert` (alert, `for: 300s`,
  `keep_firing_for: 0s`, `expr: 'vector(1)    >     0'`), `middle:record:ratio` (record, `expr:
  'avg(  vector(1)  )'`), `AlphaAlert` (alert, `for: 90m`, `expr: vector(1) > 0`).
- **Loki** (`canonl`, namespace `e2e`): two rules in the order `ZebraLogAlert` (alert, `for: 300s`,
  `expr: 'sum(rate({host="udm"}    |=    "error"    [5m]))    >    1'`), `AlphaLogAlert` (alert,
  `for: 90m`, `expr: sum(rate({host="udm"} |= "error" [5m])) > 1`).

Both reached `Accepted=True Synced=True` immediately. The exact backend document was read back for
each (`GET .../rules/<namespace>`) right after creation and again after the wait window below, and
found **byte-identical both times**. Then, instead of trusting a single document read, the test
watched for a *loop*: the raw HTTP request logs of `mimir-distributed-nginx` and `loki-gateway`
(access logs, one line per request with method/path/status) were grepped for every request touching
`canonm`'s and `canonl`'s backend namespaces from the initial creation POST (`10:55:24Z`) through
the last status check (`10:59:40Z`) — **4m16s (256s) elapsed, more than four `resyncInterval: 1m`
periods of wall-clock time.** The one direct, timestamped confirmation that the reconciler was
still actively completing successful syncs late in that span, not stalled, is
`alerts_operator_last_sync_timestamp_seconds{tenant="e2e"}` reading `10:59:24Z` (scraped once, at
the end of the window) — one concrete data point, not a count of every individual reconcile that
ran; the wall-clock-vs-interval arithmetic is what supports "several cycles," not a tally of log
lines.

**Result: exactly one `POST` to each namespace across the whole 10:55:24Z–10:59:40Z span — the
initial create. Zero repeat POSTs.** (`grep -c 'POST .../rules/e2e%2Fe2e%2Fcanonm'` → `1`;
`grep -c 'POST .../rules/e2e_e2e_canonl'` → `1`.) This is direct evidence of no write-loop, not an
inference from a single document read.

### Findings per field, both backends

| Field | Mimir (`canonm`) | Loki (`canonl`) |
|---|---|---|
| `interval` posted as absent | Ruler did **not** default/add one — the readback has no `interval:` line at all. No loop. | Same: no `interval:` line in the readback. No loop. |
| Rule order (`Zebra`, `middle:record`, `Alpha` / `Zebra`, `Alpha`) | **Preserved exactly**, insertion order, on every read. | **Preserved exactly**, insertion order, on every read. |
| `expr` whitespace (`'vector(1)    >     0'`, `'avg(  vector(1)  )'`) | **Preserved verbatim**, including the extra internal spaces — not normalised. | **Preserved verbatim** (`'sum(rate({host="udm"}    |=    "error"    [5m]))    >    1'`) — not normalised. |
| `for: 300s` / `for: 90m` | Canonicalised to `5m` / `1h30m` (already known, already fixed — `RulesEqual` via `model.ParseDuration` treats these as equal to what was sent; zero repeat POSTs confirms the fix holds live). | Same canonicalisation (`5m` / `1h30m`), same result: no loop. |
| `keep_firing_for: 0s` | Disappears entirely from the readback (`omitempty` after `rulefmt` decode of a zero duration) — matches the documented `0s → absent` case. Compared equal, no loop. | Not applicable to this backend's rule shape as tested (LogQL alerting rules used here didn't include it). |

**Verdict: this is the most important negative result of the run.** The interval-defaulting hazard
this section most expected to bite — did not bite, on either backend, on this Mimir/Loki version.
Rule ordering is preserved by both rulers. `expr` is stored and returned verbatim by both. No fix
is needed. This does not prove no ruler *ever* canonicalises these fields (a future Mimir/Loki
upgrade could change ruler behaviour), but it is a real, live, request-log-backed result for the
versions running on this cluster today, not an assumption.

**Scope note:** this run live-tested `interval` *absent* (the case the live brief called out as
"the one I most expect to bite") on both backends. It did not separately live-test a *specified*
non-canonical group-level `interval` (e.g. `interval: 60s` → does the ruler rewrite it to `1m`?)
— that case is the same duration-comparison code path as `for`/`keep_firing_for` (`RulesEqual` via
`model.ParseDuration`), already covered by `TestSyncRulesDoesNotRewriteCanonicalisedDurations`, and
was treated as already-adequately-covered by that unit test rather than re-run live here. Flagging
this explicitly rather than letting the PASSED verdict above imply every interval scenario was
live-checked.

**Old text, kept for history:** not verified against a live cluster (same power outage as §8). The
duration half was fixed and covered by tests; the rest of this section was a list of assumptions
about what a real ruler hands back, none of which any test in this repo could settle on its own.

### What changed

`compile.RulesEqual` compared `interval`, `for` and `keep_firing_for` as raw strings, and
`backend/fake` stored the exact `backend.RuleGroup` it decoded from the POST body, so a round-trip
through the fake was the identity function. A real ruler decodes through Prometheus' `rulefmt`,
where those three fields are `model.Duration` with `omitempty`, and re-emits them canonically:
`300s` → `5m`, `60s` → `1m`, `90m` → `1h30m`, `0s` → absent. All of those are legal input, so a
user writing `for: 300s` would have had the operator re-POST that group on every reconcile forever
while reporting `Synced=True`, with ruler write rate the only symptom.

Both halves are fixed: `RulesEqual` now compares durations through `model.ParseDuration`, and
`backend/fake` canonicalises them on write and on seeding, so this class of bug fails in
`go test` (`TestSyncRulesDoesNotRewriteCanonicalisedDurations`) rather than living in production.

### What was proven, live, 2026-09-23

Folded into the §8 run, same `e2e` namespace/tenant/prefix, commit `d843315`, image
`sha-d843315`. See the "Method" and "Findings per field" sections above for the full account: a
Mimir group and a Loki group, each with no `interval`, non-canonical durations, deliberately
out-of-order rules, and whitespace-padded `expr`, were posted and watched across 4+ resync cycles
via live request logs. **Zero repeat POSTs to either namespace.** Full detail, including the raw
`grep` output and timestamps, is in
`.superpowers/sdd/2026-09-21-alerts-operator/live-verification-report.md`.

### Other fields a backend might canonicalise, and what the comparison would do

Durations were the obvious case. These are the ones looked for while fixing it:

| Field | Risk | Status |
|---|---|---|
| `interval` when the operator sends none | If a ruler *defaults* it (e.g. returns `interval: 1m` for a group posted without one) the comparison sees `""` vs `"1m"` and rewrites forever. Same shape as the duration bug, not fixable blind — `""` cannot be normalised to a default this code does not know. | **Verified live 2026-09-23, both backends: no default added.** A `canonm`/`canonl` group posted with no `interval` came back with no `interval` line at all, on both Mimir and Loki, on every read across a 4-minute/4-resync-cycle window. No loop. |
| Rule order within a group | `normalize` sorts *groups* by name but never reorders rules, deliberately: rule order is semantically meaningful to `rulefmt`. A ruler that reordered them would rewrite forever. | **Verified live 2026-09-23, both backends: order preserved.** Three rules posted in deliberately non-alphabetical order (Mimir) and two (Loki) came back in the exact order posted, on every read. |
| `expr` | `rulefmt` stores the expression as a string and does not re-print the parsed AST, so whitespace and formatting should survive verbatim. If any ruler ever normalised PromQL/LogQL text, every group would rewrite forever. | **Verified live 2026-09-23, both backends: preserved verbatim.** Deliberately multi-space-padded PromQL and LogQL expressions came back byte-identical to what was posted, on both backends. No loop. |
| Loki `limit`, `align_evaluation_time_on_interval`; Mimir `source_tenants`, `query_offset`/`evaluation_delay` | Not fields of `backend.RuleGroup`, so the YAML decode drops them. No rewrite loop — but the operator also cannot see drift in them, and would silently discard one if a user's group were ever adopted from outside. | Known and accepted for v1. Not exercised this run (out of scope — no live ruler-side defaulting behavior to observe since the operator never sends these fields either way). |
| `labels` / `annotations` | Decoded into Go maps, compared with `reflect.DeepEqual`, so serialisation order is irrelevant. An empty map and an absent one are both nil-ed by `normalize`. | Covered by tests. Not separately re-verified live this run (labels/annotations were present and identical on every readback observed, incidentally consistent with this). |

The point of §8 applies here unchanged: a fake that agrees with the code's assumption proves
nothing about the assumption.
