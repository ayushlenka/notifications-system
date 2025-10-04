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

// env returns environment variable or default.
func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func runAPI() error {
	apiAddr := env("API_ADDR", ":8080")
	brokers := env("KAFKA_BROKERS", "localhost:9092")
	topic := env("TOPIC_NOTIFS", "notifications.pending")

	writer := NewKafkaWriter(brokers, topic)
	defer writer.Close()

	http.HandleFunc("/v1/notifications", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		var n Notification
		if err := json.NewDecoder(r.Body).Decode(&n); err != nil {
			http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
			return
		}
		// Minimal validation
		if n.Channel == "" || n.To == "" || n.Body == "" {
			http.Error(w, "channel, to, and body are required", http.StatusBadRequest)
			return
		}

		id := uuid.NewString()
		envlp := Message{ID: id, Attempt: 0, Notification: n}
		if err := Publish(r.Context(), writer, envlp); err != nil {
			http.Error(w, "publish failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		MetricsEnqueued.Inc()

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"id": id})
	})

	// health & metrics
	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/live", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	http.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

	log.Printf("[api] listening on %s (topic=%s, brokers=%s)", apiAddr, topic, brokers)
	return http.ListenAndServe(apiAddr, nil)
}

func runWorker() error {
	brokers := env("KAFKA_BROKERS", "localhost:9092")
	topic := env("TOPIC_NOTIFS", "notifications.pending")
	dlqTopic := env("TOPIC_DLQ", "notifications.dlq")
	groupID := env("WORKER_GROUP_ID", "notifications-workers")
	metricsAddr := env("WORKER_METRICS_ADDR", ":9091")

	// metrics server for the worker
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		log.Printf("[worker] metrics on %s", metricsAddr)
		_ = http.ListenAndServe(metricsAddr, mux)
	}()

	reader := NewKafkaReader(brokers, topic, groupID)
	defer reader.Close()

	prodWriter := NewKafkaWriter(brokers, topic)
	defer prodWriter.Close()
	dlqWriter := NewKafkaWriter(brokers, dlqTopic)
	defer dlqWriter.Close()

	p := Processor{
		MaxAttempts: 5,
		BaseBackoff: 2 * time.Second,
		Deliver:     Deliver, // uses your Notification model
		Republish: func(ctx context.Context, envlp Message) error {
			return Publish(ctx, prodWriter, envlp)
		},
		SendToDLQ: func(ctx context.Context, envlp Message) error {
			return Publish(ctx, dlqWriter, envlp)
		},
	}

	log.Printf("[worker] consuming topic=%s group=%s brokers=%s", topic, groupID, brokers)
	return ConsumeLoop(context.Background(), reader, &p)
}

func main() {
	_ = godotenv.Load() // loads .env if present

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
