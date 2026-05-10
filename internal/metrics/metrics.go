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

	OktaPushEventsReceivedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "okta_push_events_received_total",
		Help:      "Number of Okta push events received by the ingest endpoint.",
	}, []string{"source_name", "channel", "status"})

	OktaPushEventsProcessedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "okta_push_events_processed_total",
		Help:      "Number of Okta push inbox events processed by final status.",
	}, []string{"source_name", "channel", "status"})

	OktaPushProcessingDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "okta_push_processing_duration_seconds",
		Help:      "Time spent processing a batch of Okta push inbox events.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"source_name", "channel"})

	OktaPushQueueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "okta_push_queue_depth",
		Help:      "Current number of queued Okta push inbox events.",
	}, []string{"source_name", "channel"})

	OktaPushRedisQueueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "okta_push_redis_queue_depth",
		Help:      "Current number of queued Okta push Redis wake-up IDs.",
	}, []string{"queue"})

	OktaPushDeadLetterRows = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "okta_push_dead_letter_rows",
		Help:      "Current number of Okta push inbox rows in dead letter status.",
	}, []string{"source_name", "channel"})

	OktaPushLastReceivedTimestamp = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "okta_push_last_received_timestamp_seconds",
		Help:      "Unix timestamp of the last Okta push event received.",
	}, []string{"source_name", "channel"})

	OktaPushLastProcessedTimestamp = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "okta_push_last_processed_timestamp_seconds",
		Help:      "Unix timestamp of the last Okta push inbox event processed.",
	}, []string{"source_name", "channel"})

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

	// Metric name retains the legacy "non_human_access_" prefix for dashboard/alert continuity after the surface rename.
	NonHumanIdentitiesWeeklyAdminReviewSessions = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "non_human_access_weekly_admin_review_sessions",
		Help:      "Distinct admin users who interacted with the non-human identities workflow in the last 7 days.",
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
