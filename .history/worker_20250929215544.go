package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	k "github.com/segmentio/kafka-go"
)

type Processor struct {
	MaxAttempts int
	BaseBackoff time.Duration
	Deliver     func(Notification) error
	Republish   func(context.Context, Envelope) error
	SendToDLQ   func(context.Context, Envelope) error
}

func ConsumeLoop(ctx context.Context, reader *k.Reader, p *Processor) error {
	for {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			// returning exits the worker; you can choose to continue with sleep instead
			return err
		}

		var envlp Envelope
		if err := json.Unmarshal(m.Value, &envlp); err != nil {
			MetricsProcessedFail.Inc()
			log.Printf("bad message: %v", err)
			continue
		}

		start := time.Now()
		if err := p.Deliver(envlp.Notification); err != nil {
			// failure path → retry or DLQ
			envlp.Attempt++
			if envlp.Attempt >= p.MaxAttempts {
				_ = p.SendToDLQ(context.Background(), envlp)
				MetricsProcessedFail.Inc()
				log.Printf("DLQ id=%s after %d attempts: %v", envlp.ID, envlp.Attempt, err)
				continue
			}
			// exponential backoff (simple)
			delay := p.BaseBackoff * (1 << (envlp.Attempt - 1))
			time.AfterFunc(delay, func() {
				_ = p.Republish(context.Background(), envlp)
			})
			MetricsProcessedFail.Inc()
			log.Printf("retry id=%s attempt=%d in %s: %v", envlp.ID, envlp.Attempt, delay, err)
			continue
		}

		// success
		MetricsProcessedOK.Inc()
		MetricsProcessingTime.Observe(time.Since(start).Seconds())
		log.Printf("delivered id=%s channel=%s to=%s", envlp.ID, envlp.Notification.Channel, envlp.Notification.To)
	}
}
