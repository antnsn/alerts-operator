package v1alpha1

import "testing"

func TestNamespacedNameString(t *testing.T) {
	got := NamespacedName{Namespace: "mimir", Name: "am-templates"}.String()
	if got != "mimir/am-templates" {
		t.Fatalf("got %q", got)
	}
}
