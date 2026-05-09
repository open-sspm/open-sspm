package oktaingest

import (
	"context"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/discovery"
	"github.com/open-sspm/open-sspm/internal/metrics"
	"github.com/open-sspm/open-sspm/internal/testdb"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestProcessQueuedWritesDiscoveryRows(t *testing.T) {
	withOktaIngestTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries) {
		queueOktaPushEvent(t, ctx, q, "acme.okta.com", "evt-sso-1", `{
			"uuid": "evt-sso-1",
			"eventType": "user.authentication.sso",
			"published": "2026-01-01T12:00:00Z",
			"actor": {"id": "00u1", "alternateId": "alice@example.com", "displayName": "Alice"},
			"target": [{"id": "0oa1", "type": "AppInstance", "alternateId": "https://app.example.com", "displayName": "Example App"}]
		}`)

		result, err := ProcessQueued(ctx, q, pool, 100)
		if err != nil {
			t.Fatalf("ProcessQueued(): %v", err)
		}
		if result.Claimed != 1 || result.Processed != 1 {
			t.Fatalf("result = %+v, want claimed=1 processed=1", result)
		}

		var signalKind, status string
		var processedRunID pgtype.Int8
		if err := pool.QueryRow(ctx, `
			SELECT e.signal_kind, i.status, i.processed_run_id
			FROM saas_app_events e
			JOIN okta_push_inbox i
			  ON i.event_external_id = e.event_external_id
			WHERE e.source_kind = 'okta'
			  AND e.source_name = 'acme.okta.com'
			  AND e.event_external_id = 'evt-sso-1'
		`).Scan(&signalKind, &status, &processedRunID); err != nil {
			t.Fatalf("select processed discovery event: %v", err)
		}
		if signalKind != discovery.SignalKindIDPSSO {
			t.Fatalf("signal_kind = %q, want %q", signalKind, discovery.SignalKindIDPSSO)
		}
		if status != "processed" || !processedRunID.Valid {
			t.Fatalf("inbox status/run = %q/%+v, want processed with run", status, processedRunID)
		}
	})
}

func TestProcessQueuedQueuesOktaFullSyncForAssignmentChanges(t *testing.T) {
	withOktaIngestTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries) {
		queueOktaPushEvent(t, ctx, q, "acme.okta.com", "evt-assignment-refresh", `{
			"uuid": "evt-assignment-refresh",
			"eventType": "application.user_membership.add",
			"published": "2026-01-01T12:00:00Z",
			"actor": {"id": "00u1", "alternateId": "alice@example.com", "displayName": "Alice"},
			"target": [{"id": "0oa1", "type": "AppInstance", "alternateId": "https://app.example.com", "displayName": "Example App"}]
		}`)

		result, err := ProcessQueued(ctx, q, pool, 100)
		if err != nil {
			t.Fatalf("ProcessQueued(): %v", err)
		}
		if result.Processed != 1 {
			t.Fatalf("processed = %d, want 1", result.Processed)
		}

		assertQueuedOktaFullSync(t, ctx, pool, "acme.okta.com")
	})
}

func TestProcessQueuedQueuesOktaFullSyncForStateRefreshOnlyEvents(t *testing.T) {
	withOktaIngestTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries) {
		queueOktaPushEvent(t, ctx, q, "acme.okta.com", "evt-user-refresh", `{
			"uuid": "evt-user-refresh",
			"eventType": "user.lifecycle.deactivate",
			"published": "2026-01-01T12:00:00Z",
			"actor": {"id": "00u1", "alternateId": "alice@example.com", "displayName": "Alice"}
		}`)

		result, err := ProcessQueued(ctx, q, pool, 100)
		if err != nil {
			t.Fatalf("ProcessQueued(): %v", err)
		}
		if result.Processed != 1 || result.Ignored != 0 {
			t.Fatalf("result = %+v, want processed=1 ignored=0", result)
		}

		var status, runStatus, sourceKind string
		var processedRunID pgtype.Int8
		if err := pool.QueryRow(ctx, `
			SELECT i.status, i.processed_run_id, sr.source_kind, sr.status
			FROM okta_push_inbox i
			JOIN sync_runs sr ON sr.id = i.processed_run_id
			WHERE i.event_external_id = 'evt-user-refresh'
		`).Scan(&status, &processedRunID, &sourceKind, &runStatus); err != nil {
			t.Fatalf("select processed state-refresh event: %v", err)
		}
		if status != "processed" || !processedRunID.Valid {
			t.Fatalf("inbox status/run = %q/%+v, want processed with run", status, processedRunID)
		}
		if sourceKind != SourceKindOktaPush || runStatus != "success" {
			t.Fatalf("run = %s/%s, want %s/success", sourceKind, runStatus, SourceKindOktaPush)
		}

		var discoveryEvents int
		if err := pool.QueryRow(ctx, `
			SELECT count(*)
			FROM saas_app_events
			WHERE event_external_id = 'evt-user-refresh'
		`).Scan(&discoveryEvents); err != nil {
			t.Fatalf("count discovery events: %v", err)
		}
		if discoveryEvents != 0 {
			t.Fatalf("discovery events = %d, want 0", discoveryEvents)
		}

		assertQueuedOktaFullSync(t, ctx, pool, "acme.okta.com")
	})
}

func TestProcessQueuedUsesOktaPushRunWithoutReclaimingExistingRuns(t *testing.T) {
	withOktaIngestTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries) {
		var discoveryRunID int64
		if err := pool.QueryRow(ctx, `
			INSERT INTO sync_runs (source_kind, source_name, status, started_at, message)
			VALUES ('okta_discovery', 'acme.okta.com', 'running', now(), '')
			RETURNING id
		`).Scan(&discoveryRunID); err != nil {
			t.Fatalf("insert running discovery run: %v", err)
		}
		var pushRunID int64
		if err := pool.QueryRow(ctx, `
			INSERT INTO sync_runs (source_kind, source_name, status, started_at, message)
			VALUES ('okta_push', 'acme.okta.com', 'running', now(), '')
			RETURNING id
		`).Scan(&pushRunID); err != nil {
			t.Fatalf("insert running push run: %v", err)
		}
		queueOktaPushEvent(t, ctx, q, "acme.okta.com", "evt-assignment-1", `{
			"uuid": "evt-assignment-1",
			"eventType": "application.user_membership.add",
			"published": "2026-01-01T12:00:00Z",
			"actor": {"id": "00u1", "alternateId": "alice@example.com", "displayName": "Alice"},
			"target": [{"id": "0oa1", "type": "AppInstance", "alternateId": "https://app.example.com", "displayName": "Example App"}]
		}`)

		result, err := ProcessQueued(ctx, q, pool, 100)
		if err != nil {
			t.Fatalf("ProcessQueued(): %v", err)
		}
		if result.Processed != 1 {
			t.Fatalf("processed = %d, want 1", result.Processed)
		}

		var status string
		if err := pool.QueryRow(ctx, `
			SELECT status
			FROM sync_runs
			WHERE id = $1
		`, discoveryRunID).Scan(&status); err != nil {
			t.Fatalf("select discovery run status: %v", err)
		}
		if status != "running" {
			t.Fatalf("okta_discovery status = %q, want running", status)
		}
		if err := pool.QueryRow(ctx, `
			SELECT status
			FROM sync_runs
			WHERE id = $1
		`, pushRunID).Scan(&status); err != nil {
			t.Fatalf("select peer push run status: %v", err)
		}
		if status != "running" {
			t.Fatalf("peer okta_push status = %q, want running", status)
		}

		var pushRuns int
		if err := pool.QueryRow(ctx, `
			SELECT count(*)
			FROM sync_runs
			WHERE source_kind = 'okta_push'
			  AND source_name = 'acme.okta.com'
			  AND status = 'success'
		`).Scan(&pushRuns); err != nil {
			t.Fatalf("select okta_push runs: %v", err)
		}
		if pushRuns != 1 {
			t.Fatalf("okta_push success runs = %d, want 1", pushRuns)
		}
	})
}

func TestProcessQueuedIgnoresNonDiscoveryEvents(t *testing.T) {
	withOktaIngestTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries) {
		queueOktaPushEvent(t, ctx, q, "acme.okta.com", "evt-policy-1", `{
			"uuid": "evt-policy-1",
			"eventType": "policy.lifecycle.update",
			"published": "2026-01-01T12:00:00Z",
			"actor": {"id": "00u1", "alternateId": "alice@example.com", "displayName": "Alice"},
			"target": [{"id": "0oa1", "type": "AppInstance", "alternateId": "https://app.example.com", "displayName": "Example App"}]
		}`)

		result, err := ProcessQueued(ctx, q, pool, 100)
		if err != nil {
			t.Fatalf("ProcessQueued(): %v", err)
		}
		if result.Ignored != 1 {
			t.Fatalf("ignored = %d, want 1", result.Ignored)
		}

		var status string
		if err := pool.QueryRow(ctx, `
			SELECT status
			FROM okta_push_inbox
			WHERE event_external_id = 'evt-policy-1'
		`).Scan(&status); err != nil {
			t.Fatalf("select ignored inbox row: %v", err)
		}
		if status != "ignored" {
			t.Fatalf("status = %q, want ignored", status)
		}
	})
}

func TestProcessQueuedDeadLettersAfterMaxAttempts(t *testing.T) {
	withOktaIngestTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries) {
		queueOktaPushEvent(t, ctx, q, "acme.okta.com", "evt-sso-max-attempts", `{
			"uuid": "evt-sso-max-attempts",
			"eventType": "user.authentication.sso",
			"published": "2026-01-01T12:00:00Z",
			"actor": {"id": "00u1", "alternateId": "alice@example.com", "displayName": "Alice"},
			"target": [{"id": "0oa1", "type": "AppInstance", "alternateId": "https://app.example.com", "displayName": "Example App"}]
		}`)
		if _, err := pool.Exec(ctx, `
			UPDATE okta_push_inbox
			SET attempts = 1
			WHERE event_external_id = 'evt-sso-max-attempts'
		`); err != nil {
			t.Fatalf("set attempts: %v", err)
		}

		result, err := ProcessQueuedWithConfig(ctx, q, pool, 100, Config{MaxAttempts: 1})
		if err != nil {
			t.Fatalf("ProcessQueuedWithConfig(): %v", err)
		}
		if result.DeadLetter != 1 || result.Processed != 0 {
			t.Fatalf("result = %+v, want dead-letter only", result)
		}

		var status, message string
		if err := pool.QueryRow(ctx, `
			SELECT status, error_message
			FROM okta_push_inbox
			WHERE event_external_id = 'evt-sso-max-attempts'
		`).Scan(&status, &message); err != nil {
			t.Fatalf("select max-attempt row: %v", err)
		}
		if status != "dead_letter" {
			t.Fatalf("status = %q, want dead_letter", status)
		}
		if message != "max processing attempts exceeded" {
			t.Fatalf("error_message = %q, want max-attempt message", message)
		}
	})
}

func TestRequeueStaleProcessingRows(t *testing.T) {
	withOktaIngestTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries) {
		queueOktaPushEvent(t, ctx, q, "acme.okta.com", "evt-stale-processing", `{
			"uuid": "evt-stale-processing",
			"eventType": "user.authentication.sso",
			"published": "2026-01-01T12:00:00Z",
			"actor": {"id": "00u1", "alternateId": "alice@example.com", "displayName": "Alice"},
			"target": [{"id": "0oa1", "type": "AppInstance", "alternateId": "https://app.example.com", "displayName": "Example App"}]
		}`)
		if _, err := pool.Exec(ctx, `
			UPDATE okta_push_inbox
			SET status = 'processing',
			    updated_at = now() - interval '10 minutes'
			WHERE event_external_id = 'evt-stale-processing'
		`); err != nil {
			t.Fatalf("set stale processing status: %v", err)
		}

		if err := requeueStaleProcessingRows(ctx, q, Config{StaleProcessingAfter: time.Minute}); err != nil {
			t.Fatalf("requeueStaleProcessingRows(): %v", err)
		}

		var status string
		if err := pool.QueryRow(ctx, `
			SELECT status
			FROM okta_push_inbox
			WHERE event_external_id = 'evt-stale-processing'
		`).Scan(&status); err != nil {
			t.Fatalf("select stale row: %v", err)
		}
		if status != "queued" {
			t.Fatalf("status = %q, want queued", status)
		}
	})
}

func TestRedeliveryDoesNotRefreshProcessingUpdatedAt(t *testing.T) {
	withOktaIngestTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries) {
		queueOktaPushEvent(t, ctx, q, "acme.okta.com", "evt-processing-redelivery", `{
			"uuid": "evt-processing-redelivery",
			"eventType": "user.authentication.sso",
			"published": "2026-01-01T12:00:00Z",
			"actor": {"id": "00u1", "alternateId": "alice@example.com", "displayName": "Alice"},
			"target": [{"id": "0oa1", "type": "AppInstance", "alternateId": "https://app.example.com", "displayName": "Example App"}]
		}`)
		staleUpdatedAt := time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)
		if _, err := pool.Exec(ctx, `
			UPDATE okta_push_inbox
			SET status = 'processing',
			    updated_at = $1,
			    last_received_at = $1
			WHERE event_external_id = 'evt-processing-redelivery'
		`, staleUpdatedAt); err != nil {
			t.Fatalf("set processing status: %v", err)
		}

		queueOktaPushEvent(t, ctx, q, "acme.okta.com", "evt-processing-redelivery", `{
			"uuid": "evt-processing-redelivery",
			"eventType": "user.authentication.sso",
			"published": "2026-01-01T12:00:00Z",
			"actor": {"id": "00u1", "alternateId": "alice@example.com", "displayName": "Alice"},
			"target": [{"id": "0oa1", "type": "AppInstance", "alternateId": "https://app.example.com", "displayName": "Example App"}]
		}`)

		var status string
		var updatedAt, lastReceivedAt time.Time
		if err := pool.QueryRow(ctx, `
			SELECT status, updated_at, last_received_at
			FROM okta_push_inbox
			WHERE event_external_id = 'evt-processing-redelivery'
		`).Scan(&status, &updatedAt, &lastReceivedAt); err != nil {
			t.Fatalf("select redelivered row: %v", err)
		}
		if status != "processing" {
			t.Fatalf("status = %q, want processing", status)
		}
		if !updatedAt.Equal(staleUpdatedAt) {
			t.Fatalf("updated_at = %s, want preserved %s", updatedAt, staleUpdatedAt)
		}
		if !lastReceivedAt.After(staleUpdatedAt) {
			t.Fatalf("last_received_at = %s, want refreshed after %s", lastReceivedAt, staleUpdatedAt)
		}
	})
}

func TestRetryOrDeadLetterAppliesExponentialBackoff(t *testing.T) {
	withOktaIngestTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries) {
		queueOktaPushEvent(t, ctx, q, "acme.okta.com", "evt-backoff", `{
			"uuid": "evt-backoff",
			"eventType": "user.authentication.sso",
			"published": "2026-01-01T12:00:00Z",
			"actor": {"id": "00u1", "alternateId": "alice@example.com", "displayName": "Alice"},
			"target": [{"id": "0oa1", "type": "AppInstance", "alternateId": "https://app.example.com", "displayName": "Example App"}]
		}`)
		// Pretend three previous attempts already happened. The next claim will
		// bump attempts to 4, so the post-claim backoff should be base * 2^3.
		if _, err := pool.Exec(ctx, `
			UPDATE okta_push_inbox
			SET attempts = 3
			WHERE event_external_id = 'evt-backoff'
		`); err != nil {
			t.Fatalf("seed attempts: %v", err)
		}

		rows, err := q.ClaimQueuedOktaPushInboxEvents(ctx, 10)
		if err != nil {
			t.Fatalf("ClaimQueuedOktaPushInboxEvents(): %v", err)
		}
		if len(rows) != 1 {
			t.Fatalf("claimed = %d, want 1", len(rows))
		}

		cfg := Config{
			RetryDelay:    30 * time.Second,
			RetryDelayMax: 15 * time.Minute,
			MaxAttempts:   10,
		}.normalized()
		before := time.Now().UTC()
		if err := retryOrDeadLetterRows(ctx, q, "acme.okta.com", rows, errStub("simulated transient failure"), cfg); err != nil {
			t.Fatalf("retryOrDeadLetterRows(): %v", err)
		}

		var nextAttemptAt pgtype.Timestamptz
		var status, message string
		if err := pool.QueryRow(ctx, `
			SELECT status, error_message, next_attempt_at
			FROM okta_push_inbox
			WHERE event_external_id = 'evt-backoff'
		`).Scan(&status, &message, &nextAttemptAt); err != nil {
			t.Fatalf("select retry row: %v", err)
		}
		if status != "queued" {
			t.Fatalf("status = %q, want queued", status)
		}
		if message != "simulated transient failure" {
			t.Fatalf("error_message = %q, want simulated message", message)
		}
		if !nextAttemptAt.Valid {
			t.Fatalf("next_attempt_at not set")
		}

		// attempts post-claim = 4, expected delay = 30s * 2^3 = 240s.
		expected := 240 * time.Second
		actualDelay := nextAttemptAt.Time.Sub(before)
		if actualDelay < expected-2*time.Second || actualDelay > expected+30*time.Second {
			t.Fatalf("retry delay = %v, want close to %v", actualDelay, expected)
		}
	})
}

func TestRefreshMetricsPrunesStaleSourceChannels(t *testing.T) {
	withOktaIngestTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries) {
		resetOktaPushMetricLabelsForTesting(t)

		queueOktaPushEvent(t, ctx, q, "acme.okta.com", "evt-metrics", `{
			"uuid": "evt-metrics",
			"eventType": "user.authentication.sso",
			"published": "2026-01-01T12:00:00Z",
			"actor": {"id": "00u1", "alternateId": "alice@example.com", "displayName": "Alice"},
			"target": [{"id": "0oa1", "type": "AppInstance", "alternateId": "https://app.example.com", "displayName": "Example App"}]
		}`)

		if err := RefreshMetrics(ctx, q); err != nil {
			t.Fatalf("RefreshMetrics(): %v", err)
		}
		if got := testutil.ToFloat64(metrics.OktaPushQueueDepth.WithLabelValues("acme.okta.com", "event_hook")); got != 1 {
			t.Fatalf("queue depth = %v, want 1", got)
		}

		if _, err := pool.Exec(ctx, `DELETE FROM okta_push_inbox WHERE event_external_id = 'evt-metrics'`); err != nil {
			t.Fatalf("delete inbox row: %v", err)
		}

		if err := RefreshMetrics(ctx, q); err != nil {
			t.Fatalf("RefreshMetrics() after cleanup: %v", err)
		}
		if got := testutil.CollectAndCount(metrics.OktaPushQueueDepth, "opensspm_okta_push_queue_depth"); got != 0 {
			t.Fatalf("queue depth time series count = %d, want 0", got)
		}
		if got := testutil.CollectAndCount(metrics.OktaPushDeadLetterRows, "opensspm_okta_push_dead_letter_rows"); got != 0 {
			t.Fatalf("dead-letter time series count = %d, want 0", got)
		}
	})
}

type errStub string

func (e errStub) Error() string { return string(e) }

func resetOktaPushMetricLabelsForTesting(t *testing.T) {
	t.Helper()
	oktaPushMetricLabelsMu.Lock()
	for key := range oktaPushMetricLabels {
		metrics.OktaPushQueueDepth.DeleteLabelValues(key.sourceName, key.channel)
		metrics.OktaPushDeadLetterRows.DeleteLabelValues(key.sourceName, key.channel)
		metrics.OktaPushLastReceivedTimestamp.DeleteLabelValues(key.sourceName, key.channel)
		metrics.OktaPushLastProcessedTimestamp.DeleteLabelValues(key.sourceName, key.channel)
	}
	oktaPushMetricLabels = map[sourceChannelKey]struct{}{}
	oktaPushMetricLabelsMu.Unlock()
}

func withOktaIngestTestDatabase(t *testing.T, fn func(context.Context, *pgxpool.Pool, *gen.Queries)) {
	t.Helper()

	testdb.WithDatabase(t, testdb.Options{NamePrefix: "opensspm_okta_ingest"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)
		fn(ctx, pool, gen.New(pool))
	})
}

func queueOktaPushEvent(t *testing.T, ctx context.Context, q *gen.Queries, sourceName, eventExternalID, raw string) {
	t.Helper()

	if _, err := q.UpsertOktaPushInboxEventsBulk(ctx, gen.UpsertOktaPushInboxEventsBulkParams{
		SourceName:          sourceName,
		Channel:             "event_hook",
		DeliveryExternalIds: []string{"delivery-" + eventExternalID},
		EventExternalIds:    []string{eventExternalID},
		EventTypes:          []string{""},
		EventIndexes:        []int32{0},
		PublishedAts:        []pgtype.Timestamptz{{Time: time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC), Valid: true}},
		RawJsons:            [][]byte{[]byte(raw)},
	}); err != nil {
		t.Fatalf("queue okta push event: %v", err)
	}
}

func assertQueuedOktaFullSync(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sourceName string) {
	t.Helper()

	var count int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM sync_jobs
		WHERE lane = 'full'
		  AND connector_kind = 'okta'
		  AND source_name = $1
		  AND trigger_kind = 'manual'
		  AND status = 'pending'
	`, sourceName).Scan(&count); err != nil {
		t.Fatalf("count queued okta full sync jobs: %v", err)
	}
	if count != 1 {
		t.Fatalf("queued okta full sync jobs = %d, want 1", count)
	}
}
