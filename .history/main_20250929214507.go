package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

type createReq struct {
	Type           string          `json:"type"`    // "email" | "sms" | "push"
	Payload        json.RawMessage `json:"payload"` // free-form per type
	IdempotencyKey string          `json:"idempotencyKey,omitempty"`
}

func runAPI() error {
	apiAddr := env("API_ADDR", ":8080")
	brokers := env("KAFKA_BROKERS", "localhost:9092")
	topic := env("TOPIC_NOTIFS", "notifications.pending")

	writer := NewKafkaWriter(brokers, topic)

	http.HandleFunc("/v1/notifications", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var req createReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
			return
		}
		if req.Type == "" || len(req.Payload) == 0 {
			http.Error(w, "type and payload required", http.StatusBadRequest)
			return
		}

		id := uuid.NewString()
		msg := Message{ID: id, Type: req.Type, Payload: req.Payload, Attempt: 0}
		if err := Publish(context.Background(), writer, msg); err != nil {
			http.Error(w, "publish failed: "+err.Error(), 500)
			return
		}
		MetricsEnqueued.Inc()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"id": id})
	})

	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/live", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	http.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })

	log.Printf("[api] listening on %s (topic=%s, brokers=%s)", apiAddr, topic, brokers)
	return http.ListenAndServe(apiAddr, nil)
}

func runWorker() error {
	brokers := env("KAFKA_BROKERS", "localhost:9092")
	topic := env("TOPIC_NOTIFS", "notifications.pending")
	dlqTopic := env("TOPIC_DLQ", "notifications.dlq")
	groupID := "notifications-workers"
	metricsOn := env("WORKER_METRICS_ADDR", ":9091")

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Printf("[worker] metrics on %s", metricsOn)
		log.Fatal(http.ListenAndServe(metricsOn, nil))
	}()

	reader := NewKafkaReader(brokers, topic, groupID)
	defer reader.Close()
	dlqWriter := NewKafkaWriter(brokers, dlqTopic)
	defer dlqWriter.Close()

	p := Processor{
		MaxAttempts: 5,
		BaseBackoff: 2 * time.Second,
		Deliver:     Deliver, // from provider.go
		Republish: func(ctx context.Context, m Message) error {
			return Publish(ctx, NewKafkaWriter(brokers, topic), m)
		},
		SendToDLQ: func(ctx context.Context, m Message) error {
			return Publish(ctx, dlqWriter, m)
		},
	}

	log.Printf("[worker] consuming topic=%s group=%s brokers=%s", topic, groupID, brokers)
	return ConsumeLoop(context.Background(), reader, &p)
}

func main() {
	_ = godotenv.Load()

	modeFlag := flag.String("mode", "", `mode to run: "api" or "worker" (overrides MODE env)`)
	flag.Parse()

	mode := env("MODE", "api")
	if *modeFlag != "" {
		mode = *modeFlag
	}

	switch mode {
	case "api":
		if err := runAPI(); err != nil {
			log.Fatal(err)
		}
	case "worker":
		if err := runWorker(); err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatalf("unknown MODE=%q (use api or worker)", mode)
	}
}
