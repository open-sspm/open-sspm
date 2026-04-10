package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	namespace = "opensspm"
)

var (
	syncDurationBuckets = []float64{1, 2, 5, 10, 30, 60, 120, 300, 600, 1200, 1800, 3600}

	// Sync Metrics
	SyncDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "sync_duration_seconds",
		Help:      "Time taken for a connector sync to complete.",
		Buckets:   syncDurationBuckets,
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

	SyncMetricsCollectionFailuresTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "sync_metrics_collection_failures_total",
		Help:      "Count of metrics collection failures after successful syncs.",
	}, []string{"connector_kind", "connector_name", "reason"})

	// Resource Metrics
	ResourcesTotal = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "resources_total",
		Help:      "Number of resources ingested.",
	}, []string{"connector_kind", "connector_name", "type"})

	DiscoveryEventsIngestedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "discovery_events_ingested_total",
		Help:      "Number of discovery evidence events ingested.",
	}, []string{"source_kind", "signal_kind"})

	DiscoveryIngestFailuresTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "discovery_ingest_failures_total",
		Help:      "Number of discovery ingestion failures.",
	}, []string{"source_kind", "signal_kind", "error_kind"})

	DiscoveryAppsTotal = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "discovery_apps_total",
		Help:      "Current discovered SaaS app inventory by managed state.",
	}, []string{"managed_state"})

	DiscoveryHotspotsTotal = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "discovery_hotspots_total",
		Help:      "Current discovered SaaS hotspots by risk level.",
	}, []string{"risk_level"})

	NonHumanPrincipalsTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "non_human_principals_total",
		Help:      "Current non-human principal inventory across configured sources.",
	})

	NonHumanPrincipalsWithAccountableOwnerTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "non_human_principals_with_accountable_owner_total",
		Help:      "Configured non-human principals that have an accountable owner.",
	})

	NonHumanHighRiskCredentialsTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "non_human_high_risk_credentials_total",
		Help:      "High-risk linked credentials associated with configured non-human principals.",
	})

	NonHumanHighRiskCredentialsWithAttributionTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "non_human_high_risk_credentials_with_attribution_total",
		Help:      "High-risk linked credentials that have accountable ownership or actor attribution.",
	})

	NonHumanAccessWeeklyAdminReviewSessions = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "non_human_access_weekly_admin_review_sessions",
		Help:      "Distinct admin users who interacted with the non-human access workflow in the last 7 days.",
	})

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
