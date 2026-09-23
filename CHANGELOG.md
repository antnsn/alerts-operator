# Changelog

All notable changes to this project are documented here. Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Versioning: [SemVer](https://semver.org/).

## [Unreleased]

### Fixed
- A 409 optimistic-lock conflict on the Tenant's status patch or finalizer update (routine when two children change in the same second, since each child's reconciler patches the Tenant too) is now a quiet requeue instead of a reconciler error. Previously every such burst logged `ERROR Reconciler error` with a stack trace although the retry always converged.

## [0.1.1] - 2026-09-23

### Fixed
- `ContactPoint` receivers are validated when the object is accepted, with the real Secret values, not first when the Tenant compiles its whole Alertmanager document. A malformed receiver (for example a webhook URL that does not parse) is now `Accepted=False` with reason `Invalid`; Secret values are redacted from the message.
- `NotificationPolicy` matchers, durations, `group_by` and inhibit rules are validated when the object is accepted. A policy that routes to a `ContactPoint` which is itself `Accepted=False` is `Accepted=False` with the new reason `ContactPointNotAccepted`, and is re-evaluated when that `ContactPoint` changes.
- Consequence: one malformed `ContactPoint` or `NotificationPolicy` no longer stops the whole tenant's Alertmanager sync. The Tenant compiles only accepted children, so the blast radius is the offending object, as it already was for `AlertRuleGroup`.

## [0.1.0] - 2026-09-23

### Added
- CRDs `observability.antnsn.dev/v1alpha1`: `Tenant` (cluster-scoped), `ContactPoint`, `NotificationPolicy`, `AlertRuleGroup`.
- Tenant reconciler as single writer: compiles one Alertmanager document per tenant and rule namespaces `<prefix>/<namespace>/<name>` for Mimir, `<prefix>_<namespace>_<name>` for Loki (its ruler rejects a namespace containing `/`); diffs against the backend; prunes only under the prefix (matched on that backend's separator); drift repair on `resyncInterval`.
- Receivers: webhook, pushover, slack, discord, telegram, email — secrets via `secretKeyRef` only.
- Local validation with Alertmanager's config loader and the PromQL parser; errors attributed to the responsible CR.
- Status conditions `Ready`, `AlertmanagerSynced`, `MimirRulesSynced`, `LokiRulesSynced` (Tenant) and `Accepted`, `Synced` (children); Kubernetes Events on failures.
- Metrics `alerts_operator_sync_total`, `alerts_operator_last_sync_timestamp_seconds`.
- Helm chart with CRDs, RBAC, metrics Service and optional ServiceMonitor; ArgoCD health Lua; migration and e2e runbooks.

### Not included
- Tempo (no ruler), mute timings, cross-namespace references, admission webhooks.

[0.1.1]: https://github.com/antnsn/alerts-operator/releases/tag/v0.1.1
[0.1.0]: https://github.com/antnsn/alerts-operator/releases/tag/v0.1.0
