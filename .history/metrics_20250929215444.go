package main

import "github.com/prometheus/client_golang/prometheus"

var (
	MetricsEnqueued = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "notifications_enqueued_total",
		Help: "Total notifications accepted and published",
	})
	MetricsProcessedOK = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "notifications_processed_ok_total",
		Help: "Successful deliveries",
	})
	MetricsProcessedFail = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "notifications_processed_fail_total",
		Help: "Failed deliveries (including retries and DLQ)",
	})
	MetricsProcessingTime = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "notification_processing_seconds",
		Help: "Time to process a notification end-to-end in the worker",
	})
)

func init() {
	prometheus.MustRegister(
		MetricsEnqueued,
		MetricsProcessedOK,
		MetricsProcessedFail,
		MetricsProcessingTime,
	)
}
