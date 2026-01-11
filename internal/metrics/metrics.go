package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	namespace = "opensspm"
)

var (
	// Sync Metrics
	SyncDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "sync_duration_seconds",
		Help:      "Time taken for a connector sync to complete.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"connector_kind", "connector_name"})

	SyncRunsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "sync_runs_total",
		Help:      "Count of sync executions.",
	}, []string{"connector_kind", "connector_name", "status"})

	SyncLastSuccessTimestamp = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "sync_last_success_timestamp_seconds",
		Help:      "Unix timestamp of the last successful sync.",
	}, []string{"connector_kind", "connector_name"})

	// Resource Metrics
	ResourcesTotal = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "resources_total",
		Help:      "Number of resources ingested.",
	}, []string{"connector_kind", "connector_name", "type"})

	AutoLinksTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "auto_links_total",
		Help:      "Number of identities automatically linked by email.",
	}, []string{"connector_kind", "connector_name"})

	// Rules Engine Metrics
	RuleEvaluationsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "rule_evaluations_total",
		Help:      "Number of individual rule checks performed.",
	}, []string{"ruleset_key", "status"})

	RuleEvaluationDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "rule_evaluation_duration_seconds",
		Help:      "Time taken for rule evaluation logic.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"ruleset_key"})
)
