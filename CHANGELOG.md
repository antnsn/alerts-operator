# Changelog

All notable changes to this project are documented here. Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Versioning: [SemVer](https://semver.org/).

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

[0.1.0]: https://github.com/antnsn/alerts-operator/releases/tag/v0.1.0
