package main

import (
	"context"
	"encoding/json"

	k "github.com/segmentio/kafka-go"
)

func Publish(ctx context.Context, writer *k.Writer, envlp Message) error {
	b, _ := json.Marshal(envlp)
	return writer.WriteMessages(ctx, k.Message{Value: b})
}
