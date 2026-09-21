# alerts-operator

Kubernetes operator for Grafana Mimir / Loki alert rules and Mimir Alertmanager config.
Design: `docs/superpowers/specs/2026-09-21-alerts-operator-design.md`. Plan: `docs/superpowers/plans/`.
Long-form project memory: `~/Documents/obsidian/Private/projects/Alerts Operator/`.

## Workflow rules

- **All development tasks run in subagents.** Use `superpowers:subagent-driven-development`
  when executing plans; dispatch implementation, test-writing and review work to subagents.
  The main session coordinates, reviews results, and keeps context small.
- **`/codex:review` on every code change.** Not only before push: after each completed
  task/edit set, run `codex:review` against the working tree or staged diff and act on findings
  before moving to the next task.
- TDD (`superpowers:test-driven-development`) for all implementation.
- Beads (`bd`) for task tracking. `bd prime` at session start.
- No `--no-verify`, no force-push to `main`, no amending published commits.
- Commit with `--no-gpg-sign` if signing fails.

## Stack

- Go 1.27, kubebuilder v4 layout, controller-runtime, CEL CRD validation (no webhooks).
- API group `observability.antnsn.dev/v1alpha1`: `Tenant` (cluster), `ContactPoint`,
  `NotificationPolicy`, `AlertRuleGroup` (namespaced).
- Single-writer model: only the Tenant reconciler calls Mimir/Loki. Children validate + enqueue Tenant.
- Backends: plain `net/http` clients in `internal/backend/{mimir,loki}`; `backend/fake` for tests.
  No mimirtool/lokitool.
- Tests: `go test ./...` (envtest via `setup-envtest`), golden files in `internal/compile/testdata`.
- Image `ghcr.io/antnsn/alerts-operator`; chart `charts/alerts-operator`.

## Gotchas

- Loki 3.6.7 ruler: per-group `GET /loki/api/v1/rules/{ns}/{group}` returns malformed 404. Use bulk `GET /loki/api/v1/rules` only.
- Every backend call needs `X-Scope-OrgID` (home cluster tenant `1`).
- Rule namespaces contain `/` — URL-escape path segments.
- Never `DELETE /api/v1/alerts` outside the Tenant finalizer.
