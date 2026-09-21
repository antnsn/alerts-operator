package v1alpha1

// Condition types.
const (
	ConditionReady              = "Ready"
	ConditionAccepted           = "Accepted"
	ConditionSynced             = "Synced"
	ConditionAlertmanagerSynced = "AlertmanagerSynced"
	ConditionMimirRulesSynced   = "MimirRulesSynced"
	ConditionLokiRulesSynced    = "LokiRulesSynced"
)

// Condition reasons.
const (
	ReasonAccepted             = "Accepted"
	ReasonSynced               = "Synced"
	ReasonPending              = "Pending"
	ReasonNotConfigured        = "NotConfigured"
	ReasonTenantNotFound       = "TenantNotFound"
	ReasonBackendNotConfigured = "BackendNotConfigured"
	ReasonSecretNotFound       = "SecretNotFound"
	ReasonContactPointNotFound = "ContactPointNotFound"
	ReasonConflict             = "Conflict"
	ReasonInvalidRule          = "InvalidRule"
	ReasonNoNotificationPolicy = "NoNotificationPolicy"
	ReasonBackendUnavailable   = "BackendUnavailable"
	ReasonRejected             = "Rejected"
	ReasonInvalid              = "Invalid"
	ReasonDeleting             = "Deleting"
)
