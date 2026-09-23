# Home-cluster migration: mimir-sync + Alloy rules sync → alerts-operator

Repo: `antnsn/cluster`. Everything below is a change in that repo unless marked *(cluster command)*.
Order matters: the operator must own the backends before the old writers are removed, and old
backend state must be pruned by hand (the operator only prunes under its own prefix).

Before you run anything here, read **"Before you begin"** right below and the two appendices at
the end (repointing a backend, recovering a wedged Tenant delete). They cover failure modes found
in code review, not in testing — by the time you hit them by surprise, the thing they warn about
has already happened.

## Before you begin

**The operator overwrites whatever Alertmanager config is currently live the moment it gets a
NotificationPolicy, no matter who wrote it.** `syncAlertmanager`
(`internal/controller/tenant_alertmanager.go`) fetches the config currently stored for the
Tenant's `tenantId` and, if it differs from the compiled one, POSTs the compiled one over it —
there is no check for whether this operator (or anything else) wrote what was there before. The
finalizer's ownership guard (`status.alertmanagerConfigHash` + `status.alertmanagerConfigAddress`)
only stops the operator from *deleting* a document it can't prove it wrote; it does nothing to
guard the *write* path. Point a Tenant with `tenantId: "1"` at Mimir, get a NotificationPolicy
Accepted, and whatever was live in tenant `1` before that reconcile is gone.

What the write path *does* guard is the other Tenants this operator manages: if a second Tenant CR
shares this one's `tenantId` and `spec.mimir.address`, both address the same single document (the
API is scoped by `X-Scope-OrgID` and the backend URL only, never by `rulesNamespacePrefix`) and
**neither** writes it. Both report `AlertmanagerSynced=False/Conflict` naming the other and emit an
`AlertmanagerOwnershipConflict` event; their rule namespaces keep syncing normally. So a `tenantId`
already claimed by another Tenant CR will *not* be overwritten — but anything outside this
operator's CRs still will be.

Verified against this cluster today: tenant `1`'s Alertmanager config is currently empty, but
tenant `anonymous` holds the real, currently-serving hand-written config (`mal-sync`, Keep +
Pushover receivers) — step 6 below deletes it permanently via `DELETE /api/v1/alerts`. Back up
**both** before doing anything else, regardless of which looks empty right now — that emptiness
is exactly the kind of thing that stops being true the next time someone re-runs a sync job:

```bash
# (cluster command)
M=http://mimir-distributed-nginx.mimir:80
for org in 1 anonymous; do
  f="alertmanager-config.$org.$(date +%F).bak.yaml"
  kubectl -n mimir run backup-am-$org --restart=Never --image=curlimages/curl --command -- \
    curl -sS --fail-with-body -H "X-Scope-OrgID: $org" "$M/api/v1/alerts"
  kubectl -n mimir wait --for=jsonpath='{.status.phase}'=Succeeded "pod/backup-am-$org" --timeout=30s || true
  kubectl -n mimir logs "backup-am-$org" > "$f" 2>&1
  kubectl -n mimir delete pod "backup-am-$org" --wait=false
  grep -q '^alertmanager_config:' "$f" \
    && echo "tenant $org: backup OK ($f)" \
    || { echo "tenant $org: BACKUP FAILED — do not proceed to step 6 for this tenant until fixed:"; cat "$f"; }
done
```

Plain `curl -s` treats a 401/404/5xx response as success and happily saves the error page as if
it were the config — `--fail-with-body` makes curl itself fail on those, and the `grep` after it
is the real check: Mimir returns `alertmanager_config:` as a top-level key even when the value is
empty (that's what a genuinely-empty tenant looks like), so its absence means the request failed,
not that the config is empty. Don't run step 6 for a tenant whose backup reports `BACKUP FAILED`.

Store the resulting files somewhere outside the cluster — they contain the Pushover user/token
and the Keep provider id in plaintext. This exact pattern (`kubectl run` → `wait` → `logs` →
`delete pod`) is what every other *(cluster command)* curl in this guide should use if you want
clean output in a file instead of eyeballing it in a terminal.

## 0. Preconditions

- alerts-operator image + chart released (Task 25), or `:dev` image pushed (see `docs/e2e.md`).
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

- Do the backup in "Before you begin" now, before step 1, not after you've already pointed a Tenant at Mimir.

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

Bumping `targetRevision` later on an existing install: read [the README's "Upgrading the
chart"](../README.md#upgrading-the-chart) first. `helm upgrade` never refreshes a CRD that's
already on the cluster; this project validates every CR with CEL rules baked into the CRD itself
(no admission webhooks), so a stale CRD after a schema/CEL change means the apiserver quietly
keeps validating against the *old* rules. Leaving this Application's `source.helm.skipCrds`
unset, as above, makes ArgoCD's Helm renderer apply `crds/` on every sync — unlike a bare `helm
upgrade` outside ArgoCD — but confirm that after any sync-policy change rather than assuming it;
the manual re-apply command in the README works regardless of how the chart was installed.

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

> This is the step that triggers "Before you begin" above: once the NotificationPolicy created
> here is Accepted, the Tenant reconciler writes tenant `1`'s Alertmanager config for the first
> time. If you haven't taken the backup yet, do it now — this step is not reversible from inside
> the operator.

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
M=http://mimir-distributed-nginx.mimir:80
kubectl -n mimir run curl --rm -it --image=curlimages/curl --restart=Never -- sh -c \
  "curl -s -H 'X-Scope-OrgID: 1' $M/api/v1/alerts"
```
Expected: `receivers:` contains `monitoring/keep` and `monitoring/pushover`; `route.receiver: monitoring/keep`.

**Before moving on:** everything above proves Mimir *accepted* the compiled config — it does not
prove Keep or Pushover actually received anything. Trigger one real test alert now (the same
technique step 7 formalizes: apply a throwaway always-firing `AlertRuleGroup`, wait for it to
fire, confirm delivery to both Keep and Pushover, then delete it) and confirm delivery before you
get anywhere near step 6. Step 6 permanently deletes tenant `anonymous`'s Alertmanager config —
the one that has actually been delivering Keep/Pushover alerts until now — and `Synced=True` here
is not evidence that its tenant-`1` replacement works.

(Keep is today's consumer of the generic `webhook` receiver, not something this guide or the
operator depends on. Retiring Keep later means replacing the `keep` ContactPoint's URL and secret
with the new receiver's — no operator or CRD change.)

## 4. Rules

- `apps/monitoring/mimir/manifests/rules.yml` (`PrometheusRule homelab-alerts`): convert to
  `AlertRuleGroup` `homelab` in `apps/monitoring/alerts-config/manifests/rules-mimir.yml` —
  the `spec.groups` block is copied unchanged, only `apiVersion/kind` and `tenantRef`/`backend: mimir` change.
- `apps/monitoring/mimir-sync/manifests/loki-homelab.yml` + `loki-udm.yml`: each file's `groups:` becomes
  one `AlertRuleGroup` (`loki-homelab`, `loki-udm`) with `backend: loki` in `rules-loki.yml`.

Verify *(cluster command)*:

```bash
kubectl get alertrulegroups -n monitoring
M=http://mimir-distributed-nginx.mimir:80
kubectl -n mimir run curl --rm -it --image=curlimages/curl --restart=Never -- sh -c "
  curl -s -H 'X-Scope-OrgID: 1' $M/prometheus/config/v1/rules | grep '^alerts-operator/'; echo ---;
  curl -s -H 'X-Scope-OrgID: 1' http://loki-gateway.loki/loki/api/v1/rules | grep '^alerts-operator_'"
```
Expected: `Accepted=True Synced=True` for all; Mimir namespace `alerts-operator/monitoring/homelab`, Loki namespaces `alerts-operator_monitoring_loki-homelab`, `alerts-operator_monitoring_loki-udm` (Loki joins on `_`; its ruler rejects a namespace containing `/`).

(Both `GET` calls above are the bulk `/prometheus/config/v1/rules` and `/loki/api/v1/rules` list
endpoints — one read covers the whole diff, which is why the operator itself only ever lists in
bulk. Loki's per-group `GET /loki/api/v1/rules/{ns}/{group}` does work on 3.6.7; what its ruler
cannot address is a namespace containing `/`, which is why Loki rule namespaces are joined with `_`.)

At this point Mimir has the rules twice (Alloy `homelab/*` and operator `alerts-operator/*`). Alerts fire twice until step 5. Do step 5 the same day.

## 5. Remove the old writers

1. `apps/monitoring/grafana-alloy/manifests/config.alloy`: delete the whole `mimir.rules.kubernetes "kubernetes" { … }` block.
2. Delete `apps/monitoring/mimir/manifests/rules.yml` and its kustomization entry.
3. Delete `apps/monitoring/mimir-sync/` entirely (ArgoCD prunes ns `mimir-sync`, its Jobs and the old ExternalSecret).
4. If `apps/monitoring/mimir/manifests/values.yml` still mounts the dead `alertmanager-config` ConfigMap / `extraEnvFrom: pushover`, remove them.

Verify: `kubectl get ns mimir-sync` → NotFound; Alloy pod restarted and logs show no `mimir.rules.kubernetes` component.

## 6. Prune old backend state (manual, once)

**Do not start this step until you've confirmed real alert delivery on tenant `1`'s Keep/Pushover
routing** — the smoke test called out at the end of step 3, or step 7 done early. `Synced=True`
in step 3 is not that proof. This step deletes tenant `anonymous`'s Alertmanager config, which is
what has actually been delivering alerts until now; the only way back after this step is restoring
your "Before you begin" backup by hand.

Alloy's prefix and mimir-sync's namespaces are outside `alerts-operator/` (Mimir) and `alerts-operator_` (Loki), so the operator never touches them.

This step is irreversible for anything you haven't already backed up. The last line below,
`DELETE /api/v1/alerts` for tenant `anonymous`, wipes that tenant's **entire** Alertmanager
config in one call — there is no per-receiver or per-route delete on this endpoint. Confirm the
"Before you begin" backup for tenant `anonymous` actually has content before running it.

*(cluster command)* — this block needs `curl` **and** `jq` with cluster DNS, which is more than
the plain `curlimages/curl` pod used in step 0 has. Drop into a shell that has both, then paste
the block below into it:

```bash
kubectl -n mimir run prune --rm -it --image=alpine --restart=Never -- \
  sh -c "apk add --no-cache curl jq >/dev/null && sh"
```

```bash
M=http://mimir-distributed-nginx.mimir:80

# Deletes $2 (as $1's X-Scope-OrgID) and prints one OK/FAIL line — plain curl prints nothing
# either way, so a DNS hiccup or backend blip would otherwise look identical to success. 404 is
# treated as OK (already gone), matching backend.HTTP.Delete's own IsNotFound-is-success rule
# (internal/backend/httpx.go) — everything else >= 400, or a transport failure, is a real FAIL.
prune_delete() {
  org=$1; path=$2; label=$3
  code=$(curl -s -o /tmp/prune-body -w '%{http_code}' -X DELETE -H "X-Scope-OrgID: $org" "$path")
  rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "FAIL  $label: curl exit $rc (transport error, no HTTP response)"; return 1
  fi
  case "$code" in
    200|202|204|404) echo "OK    $label ($code)" ;;
    *) echo "FAIL  $label: HTTP $code"; cat /tmp/prune-body; echo; return 1 ;;
  esac
}

# tenant 1 — Alloy-owned namespaces. Discovery is a GET, not a DELETE, but its failure mode is
# the sneakiest one in this script: if it fails, $(...) below yields an empty string, the for
# loop runs zero times, prints nothing, and looks exactly like "nothing to prune" instead of
# "couldn't even check" -- every hardcoded delete further down would still print OK, so a reader
# skimming for FAIL lines would see none and wrongly conclude the whole step succeeded while
# every Alloy-owned homelab/* namespace stayed live. Check the discovery request's own status
# before trusting an empty result from it.
list_code=$(curl -s -o /tmp/homelab-list -w '%{http_code}' -H 'X-Scope-OrgID: 1' "$M/prometheus/config/v1/rules")
if [ "$list_code" != "200" ]; then
  echo "FAIL  mimir tenant 1: namespace discovery returned HTTP $list_code, not 200 — aborting this loop, nothing deleted for it"
  cat /tmp/homelab-list; echo
else
  for ns in $(grep -E '^homelab/' /tmp/homelab-list | sed 's/:$//'); do
    prune_delete 1 "$M/prometheus/config/v1/rules/$(printf %s "$ns" | jq -sRr @uri)" "mimir tenant 1: $ns"
  done
fi
# tenant 1 — Loki direct-sync namespaces
for ns in loki-homelab loki-udm; do
  prune_delete 1 "http://loki-gateway.loki/loki/api/v1/rules/$ns" "loki tenant 1: $ns"
done
# tenant anonymous — mimir-sync leftovers
prune_delete anonymous "$M/prometheus/config/v1/rules/default" "mimir tenant anonymous: default"
# Whole-tenant wipe, see warning above.
prune_delete anonymous "$M/api/v1/alerts" "mimir tenant anonymous: alertmanager config"
```

Every line above should print `OK ...`. Any `FAIL ...` line means that specific delete didn't happen
— re-run just that one (all of these are idempotent) before trusting the Verify step below.

Verify *(cluster command)*:

```bash
M=http://mimir-distributed-nginx.mimir:80
kubectl -n mimir run curl --rm -it --image=curlimages/curl --restart=Never -- sh -c "
  echo 'mimir tenant 1 (expect only alerts-operator/*):'
  curl -s -H 'X-Scope-OrgID: 1' $M/prometheus/config/v1/rules | grep -E '^[A-Za-z]'
  echo ---
  echo 'mimir tenant anonymous (expect no rule groups left):'
  curl -s -i -H 'X-Scope-OrgID: anonymous' $M/prometheus/config/v1/rules
  echo ---
  echo 'loki tenant 1 (expect only alerts-operator_*):'
  curl -s -H 'X-Scope-OrgID: 1' http://loki-gateway.loki/loki/api/v1/rules | grep -E '^[A-Za-z]'"
```

Expected: the tenant-`1` mimir output lists only namespaces starting with `alerts-operator/`; the
loki output lists only namespaces starting with `alerts-operator_`; tenant `anonymous` shows no
rule-group content either way, but don't expect a `404` for it specifically — on this cluster's
Mimir, a tenant with zero rule groups returns `200` with an empty `{}` body (verified against a
synthetic empty tenant), not `404` — `404` is what Loki does for the same situation, a different
backend with different behavior. If your Mimir version differs and you do see `404`, that's an
equally valid "nothing left" signal; either way, the content is what matters, not the status code.

## 7. Prove alerting end to end

Follow the test-alert steps in `docs/e2e.md` §5 (E2ETest rule → Keep + Pushover → delete →
pruned) — written by Task 24, which lands after this one; if you're running this migration before
Task 24 exists, do the equivalent by hand: apply a throwaway `AlertRuleGroup` that always fires,
confirm it reaches Keep and Pushover, delete it, and confirm both backends prune it.

## 8. Cleanup

- Archive `antnsn/mimir-sync` and `antnsn/mal-sync` on GitHub with a README note pointing here.
- Update the Cluster vault: remove the stale "mimir-sync loki-rules-sync failing" issue, add the `X-Scope-OrgID`/prefix gotchas.

---

## Appendix A: repointing a Tenant's Mimir or Loki address later

`spec.mimir.address` and `spec.loki.address` are deliberately **not** CEL-immutable (unlike
`spec.tenantId` and `spec.rulesNamespacePrefix`, which are) — moving a Mimir or Loki instance is
legitimate operations. But the reconciler only ever lists and prunes at the tenant's *current*
address (`internal/controller/tenant_rules.go`, `syncRules`/`conflictingTenant`); it never looks
at an address this Tenant used to have. Edit `spec.mimir.address` and everything the operator
previously wrote at the old address — every rule namespace under the prefix, and the Alertmanager
config if `spec.alertmanager` was set — stays there forever. The finalizer can't reach it either:
it deletes at the address recorded in `status.alertmanagerConfigAddress`/the tenant's *current*
`Prefix()`, which by then is the new one.

Safe sequence:

1. Before editing, record the current address — there is no field that remembers it once you change it:
   ```bash
   kubectl get tenant <name> -o jsonpath='{.spec.mimir.address}{"\n"}{.spec.loki.address}{"\n"}'
   ```
2. Confirm the new backend is reachable and that no other Tenant/tool already owns the same
   `tenantId` + `rulesNamespacePrefix` there (see the `RuleNamespaceOwnershipConflict` event and
   `Conflict` reason on `MimirRulesSynced`/`LokiRulesSynced` if it does).
3. Edit `spec.mimir.address` (or `spec.loki.address`). Wait for `MimirRulesSynced`/`LokiRulesSynced`,
   and `AlertmanagerSynced` if applicable, to report `True`/`Synced` at the new address — that
   confirms every currently-Accepted `AlertRuleGroup`/`NotificationPolicy` has actually been
   rewritten there, not just requested.
4. Only then, against the **old** address you recorded in step 1: bulk-list rule namespaces
   under the tenant's prefix and `DELETE` each one (same technique as migration step 6), and, if
   `spec.alertmanager` was set, `DELETE /api/v1/alerts` — this wipes that whole tenant's
   Alertmanager config at the old address, same warning as step 6 above. The operator will never
   do this cleanup for you; nothing in its reconcile loop ever reads the old address again.
5. Verify the old address no longer lists anything under the prefix, and that the Tenant reports
   `Ready=True` against the new one.

Until step 4 finishes, the same rule groups (and, for Alertmanager, the same routes) exist live at
both addresses — the same double-fire window called out in migration step 4, so do step 4 above
promptly rather than leaving it for later.

## Appendix B: recovering a Tenant stuck `Terminating`


> **This also degrades other Tenants, not just the stuck one.** A Tenant sitting `Terminating`
> still counts as a claimant of its backend targets (its state stays live until its own finalizer
> removes it), so any healthy Tenant sharing its `tenantId` + address keeps reporting
> `Ready=False/Conflict`: its rule-namespace prune stays disabled, and — since the Alertmanager
> ownership guard landed — its Alertmanager document stays frozen too. Nothing bounds how long.
> Tracked as `alerts-operator-bqf`, with `alerts-operator-29k` (the survivor is not re-enqueued
> when the peer finally disappears, so recovery can still take up to one `resyncInterval`). Treat
> a wedged Tenant as urgent for that reason, not only for its own sake.

If a Tenant's `spec.{mimir,loki}.auth.basicAuthSecretRef` names a Secret that no longer exists at
the moment the Tenant is deleted, the finalizer (`internal/controller/tenant_finalizer.go`) cannot
build a backend client — `backendOptions` returns `basic auth secret <namespace>/<name> not
found`, wrapped by `finalize()` as `mimir client: basic auth secret <namespace>/<name> not found`
(or `loki client: ...`) — so `finalize()` fails, the finalizer is never removed, and the Tenant
sits `Terminating` indefinitely. Each retry re-reports:

```
Ready=False Deleting mimir client: basic auth secret <namespace>/<name> not found
```

(`Deleting` here is the literal `Reason` the controller sets while a delete is in progress and
failing — see `api/v1alpha1/conditions.go`.)

**Recovery:** recreate a Secret with the same namespace, name, and `username`/`password` keys the
`basicAuthSecretRef` names, holding credentials the backend actually accepts (the finalizer still
has to authenticate to list/delete rule namespaces, and delete the Alertmanager config if this
Tenant wrote one). `backendOptions` only checks that the Secret exists and has both keys — it
can't itself tell you whether the values are still correct, only the backend's own 401/403 can.
Once the Secret exists again with working credentials, the next reconcile's `finalize()` runs to
completion and the finalizer is removed.

**Ordering that avoids this entirely:** never delete a Secret referenced by a live Tenant's
`basicAuthSecretRef` before, or at the same time as, deleting that Tenant. Delete the Tenant
first, confirm it's actually gone (`kubectl get tenant <name>` → `NotFound`), and only then delete
the Secret.
