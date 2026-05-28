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

	WorkerLaneUp = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "worker_lane_up",
		Help:      "Whether a worker lane is currently running.",
	}, []string{"lane"})

	WorkerLaneLastLoopTickTimestamp = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "worker_lane_last_loop_tick_timestamp_seconds",
		Help:      "Unix timestamp of the last successful worker lane loop tick.",
	}, []string{"lane"})

	WorkerLaneLastClaimAttemptTimestamp = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "worker_lane_last_claim_attempt_timestamp_seconds",
		Help:      "Unix timestamp of the last queue claim attempt for a worker lane.",
	}, []string{"lane"})

	WorkerLaneLastListenerConnectTimestamp = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "worker_lane_last_listener_connect_timestamp_seconds",
		Help:      "Unix timestamp of the last successful queue notification listener connection for a worker lane.",
	}, []string{"lane"})

	WorkerLaneFailuresTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "worker_lane_failures_total",
		Help:      "Count of critical worker lane goroutine failures.",
	}, []string{"lane", "component"})

	WorkerLaneLeaseLostTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "worker_lane_lease_lost_total",
		Help:      "Count of processing lease losses observed by worker lanes.",
	}, []string{"lane"})

	EventPartitionMaintenanceRunsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "event_partition_maintenance_runs_total",
		Help:      "Count of event partition maintenance runs by status.",
	}, []string{"status"})

	EventPartitionEnsuredUntilTimestamp = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "event_partition_ensured_until_timestamp_seconds",
		Help:      "Unix timestamp of the latest event partition date ensured by maintenance.",
	})

	EventPartitionsDroppedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "event_partitions_dropped_total",
		Help:      "Count of expired event and event target partitions dropped by maintenance.",
	})

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

	EventInboxDeliveriesReceivedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "event_inbox_deliveries_received_total",
		Help:      "Number of event inbox deliveries received by ingest endpoints.",
	}, []string{"source_kind", "source_name", "channel", "status"})

	EventInboxDeliveriesProcessedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "event_inbox_deliveries_processed_total",
		Help:      "Number of event inbox deliveries processed by final status.",
	}, []string{"source_kind", "source_name", "channel", "status"})

	EventInboxProcessingDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "event_inbox_processing_duration_seconds",
		Help:      "Time spent processing an event inbox delivery.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"source_kind", "source_name", "channel"})

	EventInboxQueueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "event_inbox_queue_depth",
		Help:      "Current number of queued event inbox deliveries.",
	}, []string{"source_kind", "source_name", "channel"})

	EventInboxDeadLetterRows = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "event_inbox_dead_letter_rows",
		Help:      "Current number of event inbox deliveries in dead-letter status.",
	}, []string{"source_kind", "source_name", "channel"})

	EventInboxLastReceivedTimestamp = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "event_inbox_last_received_timestamp_seconds",
		Help:      "Unix timestamp of the last event inbox delivery received.",
	}, []string{"source_kind", "source_name", "channel"})

	EventInboxLastProcessedTimestamp = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "event_inbox_last_processed_timestamp_seconds",
		Help:      "Unix timestamp of the last event inbox delivery processed.",
	}, []string{"source_kind", "source_name", "channel"})

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

	// Metric name retains the established "non_human_access_" prefix for dashboard/alert continuity after the surface rename.
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

	// Evaluator metrics
	PolicyChecksTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "policy_checks_total",
		Help:      "Number of individual policy checks performed.",
	}, []string{"ruleset_key", "status"})

	PolicyCheckDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "policy_check_duration_seconds",
		Help:      "Time taken for policy check logic.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"ruleset_key"})
)
