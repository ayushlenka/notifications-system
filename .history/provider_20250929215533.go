package main

import (
	"errors"
	"fmt"
)

// Deliver routes a Notification to the correct provider.
// Replace the stubs with SMTP (email), Twilio (sms), FCM (push) later.
func Deliver(n Notification) error {
	switch n.Channel {
	case "email":
		// TODO: implement SMTP send (use n.To, n.Subject, n.Body)
		fmt.Printf("[email] tenant=%s to=%s subject=%q body-len=%d\n",
			n.TenantID, n.To, n.Subject, len(n.Body))
		return nil
	case "sms":
		return errors.New("sms provider not implemented")
	case "push":
		return errors.New("push provider not implemented")
	default:
		return errors.New("unknown channel")
	}
}
