# alerts-operator

Kubernetes operator for Grafana Mimir / Loki alert rules and Mimir Alertmanager config.
Design: `docs/superpowers/specs/2026-09-21-alerts-operator-design.md`. Plan: `docs/superpowers/plans/`.
Long-form project memory: `~/Documents/obsidian/Private/projects/Alerts Operator/`.

## Workflow rules

- **All work runs in subagents** — code, tests, docs, plans, chart. Use
  `superpowers:subagent-driven-development` when executing plans; use `Agent` (fork) for any
  other multi-step task. The main session only briefs, coordinates, reviews results, and
  keeps context small. Writing files directly from the main session is a violation.
- **`/codex:review` on every change, code or docs.** Not only before push: after each completed
  task/edit set (including spec, plan, README, chart, workflow edits), run `codex:review` against
  the working tree or staged diff and act on findings before moving to the next task.
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

- Loki's ruler router matches on the **decoded** path, so a rule namespace containing `/` (even as `%2F`) is
  unaddressable: every per-namespace route 404s. Loki rule namespaces are therefore `<prefix>_<k8s-ns>_<name>`
  (`_`; k8s names can't contain it). Mimir keeps `/` and is unaffected. `rulesNamespacePrefix` forbids `_`.
- Per-group `GET /loki/api/v1/rules/{ns}/{group}` **works** on Loki 3.6.7 (verified 200). The old
  "malformed 404" claim was a misdiagnosis of the slash problem. The operator still reads only the bulk
  `GET /loki/api/v1/rules` — sufficient for the diff and already proven — but not because per-group is broken.
- Every backend call needs `X-Scope-OrgID` (home cluster tenant `1`).
- Rule namespaces contain `/` — URL-escape path segments.
- Never `DELETE /api/v1/alerts` outside the Tenant finalizer.
- Makefile recipes rely on `.SHELLFLAGS = -ec` + `bash -o pipefail`. Stock macOS `/usr/bin/make` is GNU Make
  3.81 and silently ignores `.SHELLFLAGS`, so a recipe that "works" there may abort, or skip a safety abort,
  under Linux/CI/gmake ≥ 4.
- Verify any Makefile-recipe change under gmake ≥ 4.0 (`brew install make`, check `make --version`) or
  reproduce the recipe under `bash -eo pipefail -c` and say so in the report. A macOS-stock-make run is not
  evidence.


<!-- BEGIN BEADS INTEGRATION v:1 profile:minimal hash:5c2c0639 -->
## Beads Issue Tracker

This project uses **bd (beads)** for issue tracking. Run `bd prime` to see full workflow context and commands.

### Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --claim  # Claim work
bd close <id>         # Complete work
```

### Rules

- Use `bd` for ALL task tracking — do NOT use TodoWrite, TaskCreate, or markdown TODO lists
- Run `bd prime` for detailed command reference and session close protocol
- Use `bd remember` for persistent knowledge — do NOT use MEMORY.md files

## Session Completion

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds
<!-- END BEADS INTEGRATION -->
