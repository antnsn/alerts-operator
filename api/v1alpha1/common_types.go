package v1alpha1

// SecretKeyRef points at one key of a Secret in the same namespace as the referencing object.
type SecretKeyRef struct {
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
	// +kubebuilder:validation:MinLength=1
	Key string `json:"key"`
}

// NamespacedName references an object in an explicit namespace (used by the cluster-scoped Tenant).
type NamespacedName struct {
	// +kubebuilder:validation:MinLength=1
	Namespace string `json:"namespace"`
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
}

func (n NamespacedName) String() string { return n.Namespace + "/" + n.Name }
