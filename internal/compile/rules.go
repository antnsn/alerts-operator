// Package compile turns CRs into backend documents. All functions are pure.
package compile

import (
	"reflect"
	"sort"

	"github.com/antnsn/alerts-operator/api/v1alpha1"
	"github.com/antnsn/alerts-operator/internal/backend"
)

// BackendNamespace is the ruler namespace owned by one AlertRuleGroup.
func BackendNamespace(prefix, k8sNamespace, name string) string {
	return prefix + "/" + k8sNamespace + "/" + name
}

// Rules maps AlertRuleGroups to backend namespaces. The caller filters by backend first.
func Rules(prefix string, groups []v1alpha1.AlertRuleGroup) map[string][]backend.RuleGroup {
	out := map[string][]backend.RuleGroup{}
	for _, arg := range groups {
		ns := BackendNamespace(prefix, arg.Namespace, arg.Name)
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
