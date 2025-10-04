package main

type Notification struct {
	TenantID string `json:"tenantId"`
	Channel  string `json:"channel"`
	To       string `json:"to"`
	Subject  string `json:"subject,omitempty"`
	Body     string `json:"body"`
}

type Message struct {
	ID           string       `json:"id"`
	Attempt      int          `json:"attempt"`
	Notification Notification `json:"notification"`
}
