// Package index registers the cache field indexers used by the controllers.
package index

import (
	"context"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/antnsn/alerts-operator/api/v1alpha1"
)

const (
	// IndexTenantRef indexes ContactPoint, NotificationPolicy and AlertRuleGroup by spec.tenantRef.
	IndexTenantRef = "spec.tenantRef"
	// IndexSecretRefs indexes ContactPoint by every Secret name it references.
	IndexSecretRefs = "spec.secretRefs"
)

// Register installs all indexers. Must run before the manager starts.
func Register(ctx context.Context, mgr ctrl.Manager) error {
	idx := mgr.GetFieldIndexer()
	if err := idx.IndexField(ctx, &v1alpha1.ContactPoint{}, IndexTenantRef, func(o client.Object) []string {
		return []string{o.(*v1alpha1.ContactPoint).Spec.TenantRef}
	}); err != nil {
		return err
	}
	if err := idx.IndexField(ctx, &v1alpha1.NotificationPolicy{}, IndexTenantRef, func(o client.Object) []string {
		return []string{o.(*v1alpha1.NotificationPolicy).Spec.TenantRef}
	}); err != nil {
		return err
	}
	if err := idx.IndexField(ctx, &v1alpha1.AlertRuleGroup{}, IndexTenantRef, func(o client.Object) []string {
		return []string{o.(*v1alpha1.AlertRuleGroup).Spec.TenantRef}
	}); err != nil {
		return err
	}
	return idx.IndexField(ctx, &v1alpha1.ContactPoint{}, IndexSecretRefs, func(o client.Object) []string {
		seen := map[string]bool{}
		var out []string
		for _, r := range o.(*v1alpha1.ContactPoint).SecretRefs() {
			if !seen[r.Name] {
				seen[r.Name] = true
				out = append(out, r.Name)
			}
		}
		return out
	})
}
