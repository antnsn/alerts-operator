// Package compile turns CRs into backend documents. All functions are pure.
package compile

import (
	"reflect"
	"sort"
	"strings"

	"github.com/antnsn/alerts-operator/api/v1alpha1"
	"github.com/antnsn/alerts-operator/internal/backend"
)

const (
	// mimirSeparator joins the segments of a Mimir rule namespace. Mimir's ruler routes on the raw,
	// still-escaped path, so a namespace containing "/" is addressable as a single path segment and
	// this scheme is verified working against a live cluster.
	mimirSeparator = "/"
	// lokiSeparator joins the segments of a Loki rule namespace.
	//
	// It is NOT "/": Loki's ruler HTTP router treats every literal "/" as a path-segment boundary,
	// so once net/http has decoded %2F back into "/" the path has more segments than the registered
	// pattern expects and every per-namespace route 404s. Verified against live Loki 3.6.7:
	// POST /loki/api/v1/rules/flatns -> 202, POST /loki/api/v1/rules/e2e%2Fe2e%2Fudm -> 404
	// (alerts-operator-b4o). With "/" no AlertRuleGroup could ever sync to Loki.
	//
	// "_" specifically, rather than "-" or ".": a Kubernetes namespace or object name can contain
	// "-" and "." freely, so those would make <prefix><sep><namespace><sep><name> ambiguous, while
	// "_" is forbidden in both. Tenant.spec.rulesNamespacePrefix is pattern-restricted to exclude
	// "_" as well (api/v1alpha1/tenant_types.go), which is what makes the separator unambiguous
	// from the first segment onward -- and makes the ownership prefix below a true segment boundary.
	lokiSeparator = "_"
)

// NamespaceSeparator returns the character that joins the segments of a rule namespace on backend
// be. Mimir and Loki deliberately differ; see the constants above.
//
// Backend is a CEL-validated enum of exactly {mimir, loki}, so the default arm is unreachable for
// any object the API server accepted. It resolves to Mimir's scheme rather than panicking because
// this runs inside a reconcile loop: Mimir's separator is the historical one, so an impossible
// value degrades to the pre-existing behaviour instead of crashing the manager.
func NamespaceSeparator(be v1alpha1.Backend) string {
	switch be {
	case v1alpha1.BackendLoki:
		return lokiSeparator
	default:
		return mimirSeparator
	}
}

// BackendNamespace is the ruler namespace owned by one AlertRuleGroup on backend be.
//
// The backend is an explicit parameter rather than two per-backend helpers so that the separator is
// decided in exactly one place and every call site has to name the backend it means: a call site
// that only had the k8s coordinates could not silently pick the wrong scheme, and adding a third
// backend would not mean auditing every caller for a missing variant.
func BackendNamespace(be v1alpha1.Backend, prefix, k8sNamespace, name string) string {
	sep := NamespaceSeparator(be)
	return prefix + sep + k8sNamespace + sep + name
}

// OwnedNamespacePrefix is the string every rule namespace this operator owns under prefix on
// backend be starts with: the prefix plus that backend's separator. Matching on it -- never on the
// bare prefix -- is what keeps "alerts-operator" from claiming "alerts-operator-other_default_x".
func OwnedNamespacePrefix(be v1alpha1.Backend, prefix string) string {
	return prefix + NamespaceSeparator(be)
}

// OwnsNamespace reports whether the backend rule namespace ns belongs to prefix on backend be,
// matched on a segment boundary. The prune loop (internal/controller/tenant_rules.go) and the
// finalizer (internal/controller/tenant_finalizer.go) both gate deletes on it.
func OwnsNamespace(be v1alpha1.Backend, prefix, ns string) bool {
	return strings.HasPrefix(ns, OwnedNamespacePrefix(be, prefix))
}

// Rules maps AlertRuleGroups to backend namespaces on backend be. The caller filters by backend
// first; be also decides the namespace scheme, so passing groups of the other backend would key
// them under the wrong one.
func Rules(be v1alpha1.Backend, prefix string, groups []v1alpha1.AlertRuleGroup) map[string][]backend.RuleGroup {
	out := map[string][]backend.RuleGroup{}
	for _, arg := range groups {
		ns := BackendNamespace(be, prefix, arg.Namespace, arg.Name)
		var gs []backend.RuleGroup
		for _, g := range arg.Spec.Groups {
			bg := backend.RuleGroup{Name: g.Name, Interval: g.Interval}
			for _, r := range g.Rules {
				bg.Rules = append(bg.Rules, backend.Rule{
					Record: r.Record, Alert: r.Alert, Expr: r.Expr, For: r.For, KeepFiringFor: r.KeepFiringFor,
					Labels: copyMap(r.Labels), Annotations: copyMap(r.Annotations),
				})
			}
			gs = append(gs, bg)
		}
		out[ns] = gs
	}
	return out
}

// RulesEqual compares two group lists ignoring group order.
func RulesEqual(a, b []backend.RuleGroup) bool {
	if len(a) != len(b) {
		return false
	}
	return reflect.DeepEqual(normalize(a), normalize(b))
}

// normalize returns a deep copy sorted by group name with empty maps nil-ed; inputs are never mutated.
func normalize(in []backend.RuleGroup) []backend.RuleGroup {
	out := make([]backend.RuleGroup, len(in))
	for i, g := range in {
		g.Rules = make([]backend.Rule, len(in[i].Rules))
		for j, r := range in[i].Rules {
			if len(r.Labels) == 0 {
				r.Labels = nil
			}
			if len(r.Annotations) == 0 {
				r.Annotations = nil
			}
			g.Rules[j] = r
		}
		out[i] = g
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func copyMap(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
