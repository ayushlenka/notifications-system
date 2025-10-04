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
			return err
		}

		var env Envelope
		if err := json.Unmarshal(m.Value, &env); err != nil {
			MetricsProcessedFail.Inc()
			log.Printf("bad message: %v", err)
			continue
		}

		start := time.Now()
		if err := p.Deliver(env.Notification); err != nil {
			env.Attempt++
			if env.Attempt >= p.MaxAttempts {
				_ = p.SendToDLQ(context.Background(), env)
				MetricsProcessedFail.Inc()
				log.Printf("DLQ id=%s after %d attempts: %v", env.ID, env.Attempt, err)
				continue
			}
			// Exponential backoff
			delay := p.BaseBackoff * (1 << (env.Attempt - 1))
			time.AfterFunc(delay, func() { _ = p.Republish(context.Background(), env) })
			MetricsProcessedFail.Inc()
			log.Printf("retry id=%s attempt=%d in %s: %v", env.ID, env.Attempt, delay, err)
			continue
		}

		MetricsProcessedOK.Inc()
		MetricsProcessingTime.Observe(time.Since(start).Seconds())
		log.Printf("delivered id=%s channel=%s to=%s", env.ID, env.Notification.Channel, env.Notification.To)
	}
}
