package controller

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	observabilityv1alpha1 "github.com/antnsn/alerts-operator/api/v1alpha1"
)

func TestContactPointCEL(t *testing.T) {
	cases := []struct {
		name    string
		spec    observabilityv1alpha1.ContactPointSpec
		wantErr bool
	}{
		{"webhook url", observabilityv1alpha1.ContactPointSpec{TenantRef: "t", Webhook: []observabilityv1alpha1.WebhookConfig{{URL: "http://x"}}}, false},
		{"no receivers", observabilityv1alpha1.ContactPointSpec{TenantRef: "t"}, true},
		{"webhook without url or ref", observabilityv1alpha1.ContactPointSpec{TenantRef: "t", Webhook: []observabilityv1alpha1.WebhookConfig{{}}}, true},
		{"webhook with both url and ref", observabilityv1alpha1.ContactPointSpec{TenantRef: "t", Webhook: []observabilityv1alpha1.WebhookConfig{{URL: "http://x", URLSecretRef: &observabilityv1alpha1.SecretKeyRef{Name: "s", Key: "k"}}}}, true},
	}
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			obj := &observabilityv1alpha1.ContactPoint{ObjectMeta: metav1.ObjectMeta{Name: "cel-" + string(rune('a'+i)), Namespace: "default"}, Spec: c.spec}
			err := testClient.Create(testCtx, obj)
			if (err != nil) != c.wantErr {
				t.Fatalf("wantErr=%v got %v", c.wantErr, err)
			}
			if err == nil {
				_ = testClient.Delete(testCtx, obj)
			}
		})
	}
}

func TestContactPointSecretRefs(t *testing.T) {
	cp := &observabilityv1alpha1.ContactPoint{Spec: observabilityv1alpha1.ContactPointSpec{
		Webhook:  []observabilityv1alpha1.WebhookConfig{{URLSecretRef: &observabilityv1alpha1.SecretKeyRef{Name: "keep", Key: "url"}, HTTPConfig: &observabilityv1alpha1.HTTPConfig{BearerTokenSecretRef: &observabilityv1alpha1.SecretKeyRef{Name: "keep", Key: "token"}}}},
		Pushover: []observabilityv1alpha1.PushoverConfig{{UserKeySecretRef: observabilityv1alpha1.SecretKeyRef{Name: "po", Key: "user"}, TokenSecretRef: observabilityv1alpha1.SecretKeyRef{Name: "po", Key: "token"}}},
	}}
	refs := cp.SecretRefs()
	if len(refs) != 4 || refs[0].Key != "url" || refs[3].Key != "token" {
		t.Fatalf("refs %+v", refs)
	}
}
