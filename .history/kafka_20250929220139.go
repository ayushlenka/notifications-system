package main

import (
	"time"

	k "github.com/segmentio/kafka-go"
)

func NewKafkaWriter(brokers, topic string) *k.Writer {
	return &k.Writer{
		Addr:         k.TCP(brokers),
		Topic:        topic,
		Balancer:     &k.LeastBytes{},
		BatchTimeout: 5 * time.Millisecond,
	}
}

func NewKafkaReader(brokers, topic, groupID string) *k.Reader {
	return k.NewReader(k.ReaderConfig{
		Brokers:  []string{brokers},
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 1e3,
		MaxBytes: 10e6,
		MaxWait:  100 * time.Millisecond,
	})
}
