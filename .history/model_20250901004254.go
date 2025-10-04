package main

type Notification struct {
	TenantID string `json:"tenantId"`
	Channel  string `json:"channel"` // "email", "sms", "slack"
	To       string `json:"to"`
	Subject  string `json:"subject,omitempty"`
	Body     string `json:"body"`
}
