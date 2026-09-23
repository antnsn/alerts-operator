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

## Upgrading the chart

`helm upgrade` installs CRDs from the chart's `crds/` directory on first install only — it never
updates a CRD that's already on the cluster, by design (this is standard Helm behaviour, not
specific to this project: see ["Helm does not manage the lifecycle of CRDs"](https://helm.sh/docs/chart_best_practices/custom_resource_definitions/)).
This project has no admission webhooks; every schema and cross-field rule (immutability,
"exactly one of X or Y", etc.) is a CEL rule baked into the CRD itself. After bumping the chart
version, an unrefreshed CRD means the apiserver silently keeps validating against the *old*
schema/CEL rules — a new field is rejected, a loosened rule stays strict, a tightened one doesn't
newly apply — with no error pointing at the real cause. Always re-apply the CRDs by hand as part
of a chart bump:

```bash
helm show crds antnsn/alerts-operator --version <new-version> | kubectl apply --server-side -f -
```

Do this before or immediately after `helm upgrade`, every time `targetRevision`/the chart version
changes. If you install via ArgoCD with a Helm source and leave `source.helm.skipCrds` unset
(false), ArgoCD renders and applies `crds/` on every sync — closer to `helm template --include-crds`
than to a bare `helm upgrade` — but verify this after any sync-policy change rather than assuming
it; the `kubectl apply --server-side` command above is always safe to run regardless of how the
chart was installed.

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
| `AlertRuleGroup` | namespaced | PrometheusRule-shaped groups for `backend: mimir` or `backend: loki`. Lands in backend namespace `<prefix>/<namespace>/<name>` (Mimir) or `<prefix>_<namespace>_<name>` (Loki — its ruler rejects a namespace containing `/`). |

## Status conditions

| Kind | Condition | Meaning |
|---|---|---|
| Tenant | `Ready` | All configured targets below are `True`. |
| Tenant | `AlertmanagerSynced` | Compiled AM config matches backend. `False/NoNotificationPolicy` when no policy exists (backend untouched). `False/Conflict` when another Tenant that has **already written** the Alertmanager document shares this one's `tenantId` **and** Mimir address — they address the same single document, and the first writer keeps it (see Guarantees and limits). |
| Tenant | `MimirRulesSynced`, `LokiRulesSynced` | Rule namespaces under the prefix match desired state. `False/BackendUnavailable` on a transport error or `>= 500` from a write; `False/Rejected` on another `4xx` from a write. A `404` is not an error for reads or deletes — an empty tenant (`List`) or an already-gone rule group (`DeleteGroup`/`DeleteNamespace`) is a normal result, not a failure; a bare `404` only reaches `False/Invalid` if a write (`SetGroup`) itself gets one. |
| children | `Accepted` | References resolve (Tenant, backend, Secrets, ContactPoints) **and the object would pass Alertmanager's own loader on its own**: rules parse (`InvalidRule`), a `ContactPoint`'s receiver validates with its real Secret values (`Invalid`, message redacted), a `NotificationPolicy`'s matchers/durations/inhibit rules load (`Invalid`). An `Accepted=False` child is excluded from the Tenant's compile, so one broken object cannot take the rest of the tenant down. Reasons: `TenantNotFound`, `BackendNotConfigured`, `SecretNotFound`, `ContactPointNotFound`, `Conflict`, `InvalidRule`, `Invalid`. |
| children | `Synced` | Included in the last successful backend write. `False/Invalid` names the CR that broke compilation (defence in depth: with Accepted-time validation this is only reachable for a child accepted at a stale generation). |

## Guarantees and limits

- Prunes only backend rule namespaces under `<prefix>/` (Mimir) / `<prefix>_` (Loki), matched on that segment boundary. Everything else in the tenant is left alone.
- Deleting a `Tenant` deletes its Alertmanager config and all rule namespaces under its prefix (finalizer) — but only what it can prove it wrote (`status.alertmanagerConfigHash`/`status.alertmanagerConfigAddress` matching the current address). Writing is not guarded the same way: the first accepted NotificationPolicy overwrites whatever Alertmanager config is already live for that tenant, hand-written or not. See [`docs/migration.md`](docs/migration.md#before-you-begin) before pointing a Tenant at a tenant ID that already has alerting configured.
- **One writer per (`tenantId`, Mimir address) for Alertmanager.** `GET`/`POST`/`DELETE /api/v1/alerts` is scoped by `X-Scope-OrgID` and the backend URL only — never by `rulesNamespacePrefix` — so two Tenants sharing those two fields address one single document, whatever their prefixes. A `POST` replaces it whole, so writing it while another Tenant already owns it would destroy that Tenant's routing. **First writer keeps it**: the Tenant that wrote the live document goes on owning it at `Ready=True`, and any other Tenant that tries to write the same document reports `AlertmanagerSynced=False/Conflict` naming the owner, emits an `AlertmanagerOwnershipConflict` event and goes `Ready=False/Conflict`, without touching the backend. Give it its own `tenantId` or Mimir address, or delete whichever should not own notifications.
  - **Sharing one org between Tenants is still supported**, which is what `rulesNamespacePrefix` is for: a "platform" Tenant that owns the `NotificationPolicy` and `ContactPoint`s alongside rules-only Tenants on the same `tenantId` and address is fine. A Tenant with no accepted `NotificationPolicy` never writes the document, so it is never a claimant and never blocks the one that does. Only a second Tenant that itself has a policy — i.e. actually wants to write — conflicts. Rules are unaffected either way: different prefixes own different rule namespaces and all of them keep syncing.
  - If two Tenants both write before either has observed the other (a narrow window right after both are created), each writes once and then both freeze at `False/Conflict` with the last write live. It settles after at most one write per Tenant; it never alternates.
- `spec.tenantId` and `spec.rulesNamespacePrefix` are immutable; backend addresses are not. Moving a backend address orphans everything the operator wrote at the old one — see [`docs/migration.md`](docs/migration.md#appendix-a-repointing-a-tenants-mimir-or-loki-address-later).
- **RBAC and caching for Secrets/ConfigMaps.** A `ContactPoint`'s `secretKeyRef`, a `Tenant`'s `basicAuthSecretRef` and a `Tenant`'s `alertmanager.templatesRef` may name any namespace, so the operator holds cluster-wide `get`/`list`/`watch` on both types and watches both cluster-wide. It does **not** cache their contents: the informers strip `data`/`stringData`/`binaryData` (plus `managedFields` and `kubectl.kubernetes.io/last-applied-configuration`) before anything is stored, and every value is read live from the API server at the moment it is needed. Nothing to configure and nothing to label — the 256Mi limit is sized for metadata only. If holding even that much is too much on your cluster, set `cache.labelSelector` (chart) / `--watch-label-selector` (flag); read the note in [`values.yaml`](charts/alerts-operator/values.yaml) first, because it then becomes your job to label every referenced Secret and ConfigMap.
- No Tempo support: alert on Tempo metrics-generator series via Mimir rules.
- No mute timings, no cross-namespace references (v1).

## Development

```bash
make test          # envtest + unit tests
make chart-test    # helm lint + render assertions
make deploy-dev    # install the CI-built :dev image into the current kube-context (docs/e2e.md)
make undeploy-dev  # remove the dev release (CRDs and CRs stay)
make purge-dev-crds # delete the CRDs; refuses while any CR exists
```
