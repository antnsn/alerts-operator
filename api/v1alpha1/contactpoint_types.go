/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// HTTPBasicAuth specifies username and password for HTTP basic authentication.
type HTTPBasicAuth struct {
	UsernameSecretRef SecretKeyRef `json:"usernameSecretRef"`
	PasswordSecretRef SecretKeyRef `json:"passwordSecretRef"`
}

// HTTPConfig specifies HTTP authentication options for webhook notifications.
// +kubebuilder:validation:XValidation:rule="!(has(self.bearerTokenSecretRef) && has(self.basicAuth))",message="bearerTokenSecretRef and basicAuth are mutually exclusive"
type HTTPConfig struct {
	// +optional
	BearerTokenSecretRef *SecretKeyRef `json:"bearerTokenSecretRef,omitempty"`
	// +optional
	BasicAuth *HTTPBasicAuth `json:"basicAuth,omitempty"`
}

// WebhookConfig specifies webhook notification settings with URL and HTTP authentication.
// +kubebuilder:validation:XValidation:rule="has(self.url) != has(self.urlSecretRef)",message="exactly one of url or urlSecretRef is required"
type WebhookConfig struct {
	// +kubebuilder:validation:MinLength=1
	// +optional
	URL string `json:"url,omitempty"`
	// +optional
	URLSecretRef *SecretKeyRef `json:"urlSecretRef,omitempty"`
	// +optional
	HTTPConfig *HTTPConfig `json:"httpConfig,omitempty"`
	// +optional
	SendResolved *bool `json:"sendResolved,omitempty"`
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxAlerts *int32 `json:"maxAlerts,omitempty"`
}

// PushoverConfig specifies Pushover notification settings.
type PushoverConfig struct {
	UserKeySecretRef SecretKeyRef `json:"userKeySecretRef"`
	TokenSecretRef   SecretKeyRef `json:"tokenSecretRef"`
	// +optional
	Title string `json:"title,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`
	// +optional
	URL string `json:"url,omitempty"`
	// +optional
	URLTitle string `json:"urlTitle,omitempty"`
	// +optional
	Priority string `json:"priority,omitempty"`
	// +optional
	Sound string `json:"sound,omitempty"`
	// +optional
	SendResolved *bool `json:"sendResolved,omitempty"`
}

// SlackConfig specifies Slack notification settings.
type SlackConfig struct {
	APIURLSecretRef SecretKeyRef `json:"apiURLSecretRef"`
	// +optional
	Channel string `json:"channel,omitempty"`
	// +optional
	Username string `json:"username,omitempty"`
	// +optional
	Title string `json:"title,omitempty"`
	// +optional
	Text string `json:"text,omitempty"`
	// +optional
	IconEmoji string `json:"iconEmoji,omitempty"`
	// +optional
	SendResolved *bool `json:"sendResolved,omitempty"`
}

// DiscordConfig specifies Discord notification settings.
type DiscordConfig struct {
	WebhookURLSecretRef SecretKeyRef `json:"webhookURLSecretRef"`
	// +optional
	Title string `json:"title,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`
	// +optional
	SendResolved *bool `json:"sendResolved,omitempty"`
}

// TelegramConfig specifies Telegram notification settings.
// +kubebuilder:validation:XValidation:rule="self.chatID != 0",message="chatID must be non-zero"
type TelegramConfig struct {
	BotTokenSecretRef SecretKeyRef `json:"botTokenSecretRef"`
	ChatID            int64        `json:"chatID"`
	// +kubebuilder:validation:Enum=MarkdownV2;Markdown;HTML;""
	// +optional
	ParseMode string `json:"parseMode,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`
	// +optional
	SendResolved *bool `json:"sendResolved,omitempty"`
}

// EmailConfig specifies email notification settings.
type EmailConfig struct {
	// +kubebuilder:validation:MinLength=1
	To string `json:"to"`
	// +optional
	From string `json:"from,omitempty"`
	// +optional
	Smarthost string `json:"smarthost,omitempty"`
	// +optional
	Hello string `json:"hello,omitempty"`
	// +optional
	AuthUsername string `json:"authUsername,omitempty"`
	// +optional
	AuthPasswordSecretRef *SecretKeyRef `json:"authPasswordSecretRef,omitempty"`
	// +optional
	RequireTLS *bool `json:"requireTLS,omitempty"`
	// +optional
	SendResolved *bool `json:"sendResolved,omitempty"`
}

// ContactPointSpec mirrors Alertmanager receiver configuration. Secrets only via secretKeyRef.
// +kubebuilder:validation:XValidation:rule="(has(self.webhook) && self.webhook.size() > 0) || (has(self.pushover) && self.pushover.size() > 0) || (has(self.slack) && self.slack.size() > 0) || (has(self.discord) && self.discord.size() > 0) || (has(self.telegram) && self.telegram.size() > 0) || (has(self.email) && self.email.size() > 0)",message="at least one receiver configuration is required"
type ContactPointSpec struct {
	// +kubebuilder:validation:MinLength=1
	TenantRef string `json:"tenantRef"`
	// +optional
	Webhook []WebhookConfig `json:"webhook,omitempty"`
	// +optional
	Pushover []PushoverConfig `json:"pushover,omitempty"`
	// +optional
	Slack []SlackConfig `json:"slack,omitempty"`
	// +optional
	Discord []DiscordConfig `json:"discord,omitempty"`
	// +optional
	Telegram []TelegramConfig `json:"telegram,omitempty"`
	// +optional
	Email []EmailConfig `json:"email,omitempty"`
}

// ContactPointStatus defines the observed state of ContactPoint.
type ContactPointStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// ContactPoint is the Schema for the contactpoints API.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Tenant",type=string,JSONPath=`.spec.tenantRef`
// +kubebuilder:printcolumn:name="Accepted",type=string,JSONPath=`.status.conditions[?(@.type=="Accepted")].status`
// +kubebuilder:printcolumn:name="Synced",type=string,JSONPath=`.status.conditions[?(@.type=="Synced")].status`
type ContactPoint struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              ContactPointSpec   `json:"spec"`
	Status            ContactPointStatus `json:"status,omitempty"`
}

// SecretRefs returns every secretKeyRef used by this ContactPoint, in declaration order.
func (c *ContactPoint) SecretRefs() []SecretKeyRef {
	var out []SecretKeyRef
	add := func(r *SecretKeyRef) {
		if r != nil {
			out = append(out, *r)
		}
	}
	for i := range c.Spec.Webhook {
		w := &c.Spec.Webhook[i]
		add(w.URLSecretRef)
		if w.HTTPConfig != nil {
			add(w.HTTPConfig.BearerTokenSecretRef)
			if w.HTTPConfig.BasicAuth != nil {
				add(&w.HTTPConfig.BasicAuth.UsernameSecretRef)
				add(&w.HTTPConfig.BasicAuth.PasswordSecretRef)
			}
		}
	}
	for i := range c.Spec.Pushover {
		add(&c.Spec.Pushover[i].UserKeySecretRef)
		add(&c.Spec.Pushover[i].TokenSecretRef)
	}
	for i := range c.Spec.Slack {
		add(&c.Spec.Slack[i].APIURLSecretRef)
	}
	for i := range c.Spec.Discord {
		add(&c.Spec.Discord[i].WebhookURLSecretRef)
	}
	for i := range c.Spec.Telegram {
		add(&c.Spec.Telegram[i].BotTokenSecretRef)
	}
	for i := range c.Spec.Email {
		add(c.Spec.Email[i].AuthPasswordSecretRef)
	}
	return out
}

// ContactPointList contains a list of ContactPoint.
// +kubebuilder:object:root=true
type ContactPointList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ContactPoint `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &ContactPoint{}, &ContactPointList{})
		return nil
	})
}
