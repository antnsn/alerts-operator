#!/usr/bin/env bash
# Renders the chart and asserts on the output. No cluster needed.
set -euo pipefail
CHART="$(cd "$(dirname "$0")/.." && pwd)/charts/alerts-operator"
OUT="$(mktemp -d)"
trap 'rm -rf "$OUT"' EXIT

# `! grep -q pattern file` as a bare statement does NOT abort the script under
# `set -e`: bash explicitly exempts commands whose status is inverted with `!`
# from errexit (see bash(1), "set -e"), so a missing/unexpected match is
# silently ignored. Route every assertion through these two helpers instead,
# which use `if` (also exempt, but here that's what we want) and an explicit
# `exit 1` so a failure is never silently swallowed.
assert_has() { # pattern file
  if ! grep -q -- "$1" "$2"; then
    echo "chart-test: expected to find '$1' in $2" >&2
    exit 1
  fi
}
assert_not() { # pattern file
  if grep -q -- "$1" "$2"; then
    echo "chart-test: did not expect to find '$1' in $2" >&2
    exit 1
  fi
}

helm lint "$CHART"

helm template x "$CHART" --namespace alerts-operator --include-crds > "$OUT/default.yaml"
crds=$(grep -c '^kind: CustomResourceDefinition$' "$OUT/default.yaml" || true)
[ "$crds" -eq 4 ] || { echo "expected 4 CRDs, got $crds (run make helm-sync-crds)"; exit 1; }
assert_has '^kind: Deployment$' "$OUT/default.yaml"
assert_has '^kind: ClusterRole$' "$OUT/default.yaml"
assert_has '--leader-elect' "$OUT/default.yaml"
assert_has '--metrics-bind-address=:8080' "$OUT/default.yaml"
assert_has '--metrics-secure=false' "$OUT/default.yaml"
assert_has 'image: ghcr.io/antnsn/alerts-operator:0.1.0' "$OUT/default.yaml"
assert_not '^kind: ServiceMonitor$' "$OUT/default.yaml"

# No admission webhooks anywhere in this project (design constraint) - kubebuilder
# scaffolds these by default, so guard against a chart that accidentally carries one.
assert_not '^kind: MutatingWebhookConfiguration$' "$OUT/default.yaml"
assert_not '^kind: ValidatingWebhookConfiguration$' "$OUT/default.yaml"

# ClusterRole must grant exactly what the +kubebuilder:rbac markers in
# internal/controller/*.go ask for (see clusterrole.yaml's own comment).
assert_has 'resources: \["tenants/finalizers"\]' "$OUT/default.yaml"
assert_has 'resources: \["secrets", "configmaps"\]' "$OUT/default.yaml"
assert_has 'resources: \["events"\]' "$OUT/default.yaml"
assert_has 'resources: \["contactpoints", "notificationpolicies", "alertrulegroups"\]' "$OUT/default.yaml"

# Tenant is cluster-scoped; the other three CRDs are namespaced.
tenant_crd="$(awk '/name: tenants\.observability\.antnsn\.dev/,/^---$/' "$OUT/default.yaml")"
[[ "$tenant_crd" == *"scope: Cluster"* ]]
for r in alertrulegroups contactpoints notificationpolicies; do
  crd="$(awk "/name: ${r}\\.observability\\.antnsn\\.dev/,/^---\$/" "$OUT/default.yaml")"
  [[ "$crd" == *"scope: Namespaced"* ]]
done

# Metrics service ships by default.
assert_has '^kind: Service$' "$OUT/default.yaml"
assert_has 'port: 8080' "$OUT/default.yaml"

helm template x "$CHART" --namespace alerts-operator --include-crds --set serviceMonitor.enabled=true --set image.tag=dev > "$OUT/sm.yaml"
assert_has '^kind: ServiceMonitor$' "$OUT/sm.yaml"
assert_has 'image: ghcr.io/antnsn/alerts-operator:dev' "$OUT/sm.yaml"

helm template x "$CHART" --namespace alerts-operator --include-crds --set leaderElection.enabled=false > "$OUT/nole.yaml"
assert_not '--leader-elect' "$OUT/nole.yaml"
assert_not '^kind: Role$' "$OUT/nole.yaml"

# Disabling metrics drops the Service, the metrics port and args, and (even if
# requested) the ServiceMonitor - it depends on the metrics Service existing.
helm template x "$CHART" --namespace alerts-operator --include-crds --set metrics.enabled=false --set serviceMonitor.enabled=true > "$OUT/nometrics.yaml"
assert_not '^kind: Service$' "$OUT/nometrics.yaml"
assert_not '^kind: ServiceMonitor$' "$OUT/nometrics.yaml"
assert_not '--metrics-bind-address=:8080' "$OUT/nometrics.yaml"
assert_has '--metrics-bind-address=0' "$OUT/nometrics.yaml"

# serviceAccount.create=false uses the given name and renders no ServiceAccount.
helm template x "$CHART" --namespace alerts-operator --include-crds --set serviceAccount.create=false --set serviceAccount.name=custom-sa > "$OUT/customsa.yaml"
assert_not '^kind: ServiceAccount$' "$OUT/customsa.yaml"
assert_has 'serviceAccountName: custom-sa' "$OUT/customsa.yaml"

# Examples must exist and parse as YAML; schema validation happens on the home cluster (docs/e2e.md).
EX="$(cd "$(dirname "$0")/.." && pwd)/docs/examples"
for f in tenant secrets contactpoints notificationpolicy alertrulegroup-mimir alertrulegroup-loki; do
  [ -f "$EX/$f.yaml" ] || { echo "missing docs/examples/$f.yaml"; exit 1; }
  yq e . "$EX/$f.yaml" >/dev/null
done

echo "chart-test: OK"
