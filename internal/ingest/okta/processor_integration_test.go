package oktaingest

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/discovery"
	genericinbox "github.com/open-sspm/open-sspm/internal/ingest/inbox"
	"github.com/open-sspm/open-sspm/internal/metrics"
	"github.com/open-sspm/open-sspm/internal/records"
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

		var canonicalEvents, canonicalTargets int
		if err := pool.QueryRow(ctx, `
			SELECT count(*)
			FROM events
			WHERE source_kind = 'okta'
			  AND source_name = 'acme.okta.com'
			  AND provider_event_id = 'evt-sso-1'
		`).Scan(&canonicalEvents); err != nil {
			t.Fatalf("count canonical events: %v", err)
		}
		if err := pool.QueryRow(ctx, `
			SELECT count(*)
			FROM event_targets et
			JOIN events e
			  ON e.received_at = et.event_received_at
			 AND e.id = et.event_id
			WHERE e.provider_event_id = 'evt-sso-1'
		`).Scan(&canonicalTargets); err != nil {
			t.Fatalf("count canonical targets: %v", err)
		}
		if canonicalEvents != 1 {
			t.Fatalf("canonical events = %d, want 1", canonicalEvents)
		}
		if canonicalTargets != 1 {
			t.Fatalf("canonical targets = %d, want 1", canonicalTargets)
		}

		var tailJobs int
		if err := pool.QueryRow(ctx, `
			SELECT count(*)
			FROM sync_jobs
			WHERE lane = 'tail'
			  AND connector_kind = 'okta'
			  AND source_name = 'acme.okta.com'
			  AND resource = 'system_log'
			  AND status = 'pending'
		`).Scan(&tailJobs); err != nil {
			t.Fatalf("count queued tail jobs: %v", err)
		}
		if tailJobs != 1 {
			t.Fatalf("tail jobs = %d, want 1", tailJobs)
		}
	})
}

func TestProcessGenericQueuedDispatchesOktaDelivery(t *testing.T) {
	withOktaIngestTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries) {
		raw := []byte(`{
			"uuid": "evt-generic-sso-1",
			"eventType": "user.authentication.sso",
			"published": "2026-01-01T12:00:00Z",
			"actor": {"id": "00u1", "alternateId": "alice@example.com", "displayName": "Alice"},
			"target": [{"id": "0oa1", "type": "AppInstance", "alternateId": "https://app.example.com", "displayName": "Example App"}]
		}`)
		store := genericinbox.NewStore(q)
		if _, err := store.Enqueue(ctx, genericinbox.Delivery{
			Source:          records.SourceRef{Kind: "okta", Name: "acme.okta.com"},
			Channel:         "event_hook",
			ExternalEventID: "evt-generic-sso-1",
			DedupeKey:       "provider:evt-generic-sso-1",
			RawBody:         raw,
		}); err != nil {
			t.Fatalf("enqueue generic event inbox delivery: %v", err)
		}

		result, err := ProcessGenericQueued(ctx, q, pool, 100)
		if err != nil {
			t.Fatalf("ProcessGenericQueued(): %v", err)
		}
		if result.Claimed != 1 || result.Processed != 1 {
			t.Fatalf("result = %+v, want claimed=1 processed=1", result)
		}

		var inboxStatus string
		if err := pool.QueryRow(ctx, `
			SELECT status::text
			FROM event_inbox
			WHERE dedupe_key = 'provider:evt-generic-sso-1'
		`).Scan(&inboxStatus); err != nil {
			t.Fatalf("select generic inbox status: %v", err)
		}
		if inboxStatus != "processed" {
			t.Fatalf("generic inbox status = %q, want processed", inboxStatus)
		}

		var signalKind string
		if err := pool.QueryRow(ctx, `
			SELECT signal_kind
			FROM saas_app_events
			WHERE source_kind = 'okta'
			  AND source_name = 'acme.okta.com'
			  AND event_external_id = 'evt-generic-sso-1'
		`).Scan(&signalKind); err != nil {
			t.Fatalf("select generic discovery event: %v", err)
		}
		if signalKind != discovery.SignalKindIDPSSO {
			t.Fatalf("signal_kind = %q, want %q", signalKind, discovery.SignalKindIDPSSO)
		}

		var canonicalEvents, legacyInboxRows, tailJobs int
		if err := pool.QueryRow(ctx, `
			SELECT count(*)
			FROM events
			WHERE source_kind = 'okta'
			  AND source_name = 'acme.okta.com'
			  AND provider_event_id = 'evt-generic-sso-1'
		`).Scan(&canonicalEvents); err != nil {
			t.Fatalf("count generic canonical events: %v", err)
		}
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM okta_push_inbox WHERE event_external_id = 'evt-generic-sso-1'`).Scan(&legacyInboxRows); err != nil {
			t.Fatalf("count legacy okta inbox rows: %v", err)
		}
		if err := pool.QueryRow(ctx, `
			SELECT count(*)
			FROM sync_jobs
			WHERE lane = 'tail'
			  AND connector_kind = 'okta'
			  AND source_name = 'acme.okta.com'
			  AND resource = 'system_log'
			  AND status = 'pending'
		`).Scan(&tailJobs); err != nil {
			t.Fatalf("count generic tail jobs: %v", err)
		}
		if canonicalEvents != 1 {
			t.Fatalf("canonical events = %d, want 1", canonicalEvents)
		}
		if legacyInboxRows != 0 {
			t.Fatalf("legacy okta inbox rows = %d, want 0", legacyInboxRows)
		}
		if tailJobs != 1 {
			t.Fatalf("tail jobs = %d, want 1", tailJobs)
		}
	})
}

func TestProcessQueuedObservesLoopTickOnEmptyQueue(t *testing.T) {
	withOktaIngestTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries) {
		ticks := 0
		result, err := ProcessQueuedWithConfig(ctx, q, pool, 100, Config{
			OnLoopTick: func() {
				ticks++
			},
		})
		if err != nil {
			t.Fatalf("ProcessQueuedWithConfig(): %v", err)
		}
		if result.Claimed != 0 {
			t.Fatalf("claimed = %d, want 0", result.Claimed)
		}
		if ticks != 1 {
			t.Fatalf("loop ticks = %d, want 1", ticks)
		}
	})
}

func TestProcessQueuedIDsClaimsOnlyRedisQueuedRows(t *testing.T) {
	withOktaIngestTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries) {
		queueOktaPushEvent(t, ctx, q, "acme.okta.com", "evt-sso-redis", `{
			"uuid": "evt-sso-redis",
			"eventType": "user.authentication.sso",
			"published": "2026-01-01T12:00:00Z",
			"actor": {"id": "00u1", "alternateId": "alice@example.com", "displayName": "Alice"},
			"target": [{"id": "0oa1", "type": "AppInstance", "alternateId": "https://app.example.com", "displayName": "Example App"}]
		}`)

		id := oktaPushInboxIDByExternalID(t, ctx, pool, "evt-sso-redis")

		result, err := ProcessQueuedIDsWithConfig(ctx, q, pool, []int64{id}, 100, DefaultConfig())
		if err != nil {
			t.Fatalf("ProcessQueuedIDsWithConfig(): %v", err)
		}
		if result.Claimed != 1 || result.Processed != 1 {
			t.Fatalf("result = %+v, want claimed=1 processed=1", result)
		}

		result, err = ProcessQueuedIDsWithConfig(ctx, q, pool, []int64{id}, 100, DefaultConfig())
		if err != nil {
			t.Fatalf("ProcessQueuedIDsWithConfig() duplicate: %v", err)
		}
		if result.Claimed != 0 {
			t.Fatalf("duplicate result = %+v, want claimed=0", result)
		}
	})
}

func TestClaimQueuedOktaPushInboxEventsByIDsFiltersStatusAndLimit(t *testing.T) {
	withOktaIngestTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries) {
		raw := `{
			"uuid": "evt-placeholder",
			"eventType": "user.authentication.sso",
			"published": "2026-01-01T12:00:00Z",
			"actor": {"id": "00u1", "alternateId": "alice@example.com", "displayName": "Alice"},
			"target": [{"id": "0oa1", "type": "AppInstance", "alternateId": "https://app.example.com", "displayName": "Example App"}]
		}`
		queueOktaPushEvent(t, ctx, q, "acme.okta.com", "evt-claim-a", raw)
		queueOktaPushEvent(t, ctx, q, "acme.okta.com", "evt-claim-b", raw)
		queueOktaPushEvent(t, ctx, q, "acme.okta.com", "evt-claim-c", raw)
		queueOktaPushEvent(t, ctx, q, "acme.okta.com", "evt-claim-d", raw)
		ids := oktaPushInboxIDsByExternalID(t, ctx, pool, "evt-claim-a", "evt-claim-b", "evt-claim-c", "evt-claim-d")
		if _, err := pool.Exec(ctx, `
			UPDATE okta_push_inbox
			SET status = CASE event_external_id
				WHEN 'evt-claim-b' THEN 'processing'
				WHEN 'evt-claim-c' THEN 'processed'
				ELSE status
			END
			WHERE event_external_id IN ('evt-claim-b', 'evt-claim-c')
		`); err != nil {
			t.Fatalf("seed non-queued statuses: %v", err)
		}

		missingID := ids["evt-claim-d"] + 999999
		rows, err := q.ClaimQueuedOktaPushInboxEventsByIDs(ctx, gen.ClaimQueuedOktaPushInboxEventsByIDsParams{
			Ids:          []int64{ids["evt-claim-d"], ids["evt-claim-c"], ids["evt-claim-b"], ids["evt-claim-a"], ids["evt-claim-a"], missingID},
			LimitRows:    1,
			ClaimedBy:    "test-claimant",
			ClaimToken:   "test-token-a",
			LeaseSeconds: 60,
		})
		if err != nil {
			t.Fatalf("ClaimQueuedOktaPushInboxEventsByIDs() limit=1: %v", err)
		}
		if len(rows) != 1 || rows[0].EventExternalID != "evt-claim-a" {
			t.Fatalf("claimed rows = %#v, want only evt-claim-a", rows)
		}

		rows, err = q.ClaimQueuedOktaPushInboxEventsByIDs(ctx, gen.ClaimQueuedOktaPushInboxEventsByIDsParams{
			Ids:          []int64{ids["evt-claim-d"], ids["evt-claim-c"], ids["evt-claim-b"], ids["evt-claim-a"], missingID},
			LimitRows:    10,
			ClaimedBy:    "test-claimant",
			ClaimToken:   "test-token-b",
			LeaseSeconds: 60,
		})
		if err != nil {
			t.Fatalf("ClaimQueuedOktaPushInboxEventsByIDs() remaining: %v", err)
		}
		if len(rows) != 1 || rows[0].EventExternalID != "evt-claim-d" {
			t.Fatalf("claimed remaining rows = %#v, want only evt-claim-d", rows)
		}
	})
}

func TestRunLoopWithQueueProcessesRedisIDsAndStopsConsumer(t *testing.T) {
	withOktaIngestTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries) {
		redis := miniredis.RunT(t)
		inboxQueue, err := NewRedisInboxQueue("redis://"+redis.Addr()+"/0", "test")
		if err != nil {
			t.Fatalf("NewRedisInboxQueue(): %v", err)
		}
		defer inboxQueue.Close()

		runCtx, cancelRun := context.WithCancel(ctx)
		errCh := make(chan error, 1)
		go func() {
			errCh <- RunLoopWithQueue(runCtx, q, pool, Config{
				BatchSize:               10,
				PollInterval:            5 * time.Second,
				CleanupInterval:         time.Hour,
				StaleProcessingAfter:    time.Hour,
				ProcessedRetentionDays:  30,
				DeadLetterRetentionDays: 90,
			}, inboxQueue)
		}()
		defer cancelRun()

		queueOktaPushEvent(t, ctx, q, "acme.okta.com", "evt-runloop-redis", `{
			"uuid": "evt-runloop-redis",
			"eventType": "user.authentication.sso",
			"published": "2026-01-01T12:00:00Z",
			"actor": {"id": "00u1", "alternateId": "alice@example.com", "displayName": "Alice"},
			"target": [{"id": "0oa1", "type": "AppInstance", "alternateId": "https://app.example.com", "displayName": "Example App"}]
		}`)
		id := oktaPushInboxIDByExternalID(t, ctx, pool, "evt-runloop-redis")
		if err := inboxQueue.Enqueue(ctx, []int64{id}); err != nil {
			t.Fatalf("enqueue redis id: %v", err)
		}

		waitForOktaPushInboxStatus(t, ctx, pool, "evt-runloop-redis", "processed")
		if got := testutil.ToFloat64(metrics.OktaPushRedisQueueDepth.WithLabelValues(oktaPushInboxQueueName)); got != 0 {
			t.Fatalf("redis queue depth metric = %v, want 0", got)
		}
		cancelRun()
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("RunLoopWithQueue() error = %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("RunLoopWithQueue() did not stop after cancellation")
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

func TestProcessQueuedRecordsStateRefreshStatsForMixedBatch(t *testing.T) {
	withOktaIngestTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries) {
		queueOktaPushEvent(t, ctx, q, "acme.okta.com", "evt-sso-1", `{
			"uuid": "evt-sso-1",
			"eventType": "user.authentication.sso",
			"published": "2026-01-01T12:00:00Z",
			"actor": {"id": "00u1", "alternateId": "alice@example.com", "displayName": "Alice"},
			"target": [{"id": "0oa1", "type": "AppInstance", "alternateId": "https://app.example.com", "displayName": "Example App"}]
		}`)
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
		if result.Processed != 2 {
			t.Fatalf("processed = %d, want 2", result.Processed)
		}

		var rawStats []byte
		if err := pool.QueryRow(ctx, `
			SELECT stats
			FROM sync_runs
			WHERE source_kind = 'okta_push'
			  AND source_name = 'acme.okta.com'
			  AND status = 'success'
			ORDER BY id DESC
			LIMIT 1
		`).Scan(&rawStats); err != nil {
			t.Fatalf("select okta_push stats: %v", err)
		}
		var stats struct {
			Counts map[string]int64 `json:"counts"`
		}
		if err := json.Unmarshal(rawStats, &stats); err != nil {
			t.Fatalf("unmarshal okta_push stats: %v", err)
		}
		if stats.Counts["state_refresh_user"] != 1 || stats.Counts["state_refresh_events"] != 1 {
			t.Fatalf("state refresh counts = %+v, want user=1 events=1", stats.Counts)
		}
		if stats.Counts["saas_app_events_observed"] != 1 {
			t.Fatalf("saas_app_events_observed = %d, want 1", stats.Counts["saas_app_events_observed"])
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

func TestOktaPushInboxExpiredLeaseCanBeReclaimed(t *testing.T) {
	withOktaIngestTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries) {
		queueOktaPushEvent(t, ctx, q, "acme.okta.com", "evt-lease-reclaim", `{
			"uuid": "evt-lease-reclaim",
			"eventType": "user.authentication.sso",
			"published": "2026-01-01T12:00:00Z",
			"actor": {"id": "00u1", "alternateId": "alice@example.com", "displayName": "Alice"},
			"target": [{"id": "0oa1", "type": "AppInstance", "alternateId": "https://app.example.com", "displayName": "Example App"}]
		}`)

		rows, err := q.ClaimQueuedOktaPushInboxEvents(ctx, gen.ClaimQueuedOktaPushInboxEventsParams{
			ClaimedBy:    "old-processor",
			ClaimToken:   "old-token",
			LeaseSeconds: 1,
			LimitRows:    10,
		})
		if err != nil {
			t.Fatalf("initial claim: %v", err)
		}
		if len(rows) != 1 {
			t.Fatalf("initial claim returned %d rows, want 1", len(rows))
		}
		if _, err := pool.Exec(ctx, `
			UPDATE okta_push_inbox
			SET lease_expires_at = now() - interval '1 second'
			WHERE event_external_id = 'evt-lease-reclaim'
		`); err != nil {
			t.Fatalf("expire lease: %v", err)
		}

		if err := requeueStaleProcessingRows(ctx, q, Config{StaleProcessingAfter: time.Minute}); err != nil {
			t.Fatalf("requeueStaleProcessingRows(): %v", err)
		}
		rows, err = q.ClaimQueuedOktaPushInboxEvents(ctx, gen.ClaimQueuedOktaPushInboxEventsParams{
			ClaimedBy:    "new-processor",
			ClaimToken:   "new-token",
			LeaseSeconds: 60,
			LimitRows:    10,
		})
		if err != nil {
			t.Fatalf("reclaim: %v", err)
		}
		if len(rows) != 1 || rows[0].ClaimedBy.String != "new-processor" || rows[0].ClaimToken.String != "new-token" {
			t.Fatalf("reclaimed rows = %#v, want new processor/token", rows)
		}
	})
}

func TestOktaPushInboxOldProcessorCannotFinalizeAfterLeaseLoss(t *testing.T) {
	withOktaIngestTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries) {
		queueOktaPushEvent(t, ctx, q, "acme.okta.com", "evt-lease-finalize", `{
			"uuid": "evt-lease-finalize",
			"eventType": "user.authentication.sso",
			"published": "2026-01-01T12:00:00Z",
			"actor": {"id": "00u1", "alternateId": "alice@example.com", "displayName": "Alice"},
			"target": [{"id": "0oa1", "type": "AppInstance", "alternateId": "https://app.example.com", "displayName": "Example App"}]
		}`)
		rows, err := q.ClaimQueuedOktaPushInboxEvents(ctx, gen.ClaimQueuedOktaPushInboxEventsParams{
			ClaimedBy:    "old-processor",
			ClaimToken:   "old-token",
			LeaseSeconds: 1,
			LimitRows:    10,
		})
		if err != nil {
			t.Fatalf("initial claim: %v", err)
		}
		if len(rows) != 1 {
			t.Fatalf("initial claim returned %d rows, want 1", len(rows))
		}
		if _, err := pool.Exec(ctx, `
			UPDATE okta_push_inbox
			SET lease_expires_at = now() - interval '1 second'
			WHERE event_external_id = 'evt-lease-finalize'
		`); err != nil {
			t.Fatalf("expire lease: %v", err)
		}
		if err := requeueStaleProcessingRows(ctx, q, Config{StaleProcessingAfter: time.Minute}); err != nil {
			t.Fatalf("requeueStaleProcessingRows(): %v", err)
		}
		if _, err := q.ClaimQueuedOktaPushInboxEvents(ctx, gen.ClaimQueuedOktaPushInboxEventsParams{
			ClaimedBy:    "new-processor",
			ClaimToken:   "new-token",
			LeaseSeconds: 60,
			LimitRows:    10,
		}); err != nil {
			t.Fatalf("reclaim: %v", err)
		}

		var runID int64
		if err := pool.QueryRow(ctx, `
			INSERT INTO sync_runs (source_kind, source_name, status, started_at, message)
			VALUES ('okta_push', 'acme.okta.com', 'running', now(), '')
			RETURNING id
		`).Scan(&runID); err != nil {
			t.Fatalf("insert sync run: %v", err)
		}
		marked, err := q.MarkOktaPushInboxProcessed(ctx, gen.MarkOktaPushInboxProcessedParams{
			ProcessedRunID: runID,
			Ids:            []int64{rows[0].ID},
			ClaimedBy:      "old-processor",
			ClaimToken:     "old-token",
		})
		if err != nil {
			t.Fatalf("old finalize: %v", err)
		}
		if marked != 0 {
			t.Fatalf("old finalize marked %d rows, want 0", marked)
		}

		var status, claimedBy, claimToken string
		if err := pool.QueryRow(ctx, `
			SELECT status, claimed_by, claim_token
			FROM okta_push_inbox
			WHERE event_external_id = 'evt-lease-finalize'
		`).Scan(&status, &claimedBy, &claimToken); err != nil {
			t.Fatalf("select row after old finalize: %v", err)
		}
		if status != "processing" || claimedBy != "new-processor" || claimToken != "new-token" {
			t.Fatalf("row = status %q claimed_by %q token %q, want processing/new-processor/new-token", status, claimedBy, claimToken)
		}
	})
}

func TestOktaPushInboxOldProcessorCannotCommitSideEffectsAfterLeaseLoss(t *testing.T) {
	withOktaIngestTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries) {
		queueOktaPushEvent(t, ctx, q, "acme.okta.com", "evt-lease-side-effects", `{
			"uuid": "evt-lease-side-effects",
			"eventType": "application.user_membership.add",
			"published": "2026-01-01T12:00:00Z",
			"actor": {"id": "00u1", "alternateId": "alice@example.com", "displayName": "Alice"},
			"target": [{"id": "0oa1", "type": "AppInstance", "alternateId": "https://app.example.com", "displayName": "Example App"}]
		}`)
		rows, err := q.ClaimQueuedOktaPushInboxEvents(ctx, gen.ClaimQueuedOktaPushInboxEventsParams{
			ClaimedBy:    "old-processor",
			ClaimToken:   "old-token",
			LeaseSeconds: 1,
			LimitRows:    10,
		})
		if err != nil {
			t.Fatalf("initial claim: %v", err)
		}
		if len(rows) != 1 {
			t.Fatalf("initial claim returned %d rows, want 1", len(rows))
		}
		if _, err := pool.Exec(ctx, `
			UPDATE okta_push_inbox
			SET lease_expires_at = now() - interval '1 second'
			WHERE event_external_id = 'evt-lease-side-effects'
		`); err != nil {
			t.Fatalf("expire lease: %v", err)
		}
		if err := requeueStaleProcessingRows(ctx, q, Config{StaleProcessingAfter: time.Minute}); err != nil {
			t.Fatalf("requeueStaleProcessingRows(): %v", err)
		}
		if _, err := q.ClaimQueuedOktaPushInboxEvents(ctx, gen.ClaimQueuedOktaPushInboxEventsParams{
			ClaimedBy:    "new-processor",
			ClaimToken:   "new-token",
			LeaseSeconds: 60,
			LimitRows:    10,
		}); err != nil {
			t.Fatalf("reclaim: %v", err)
		}

		result, err := processClaimedRows(ctx, q, pool, rows, DefaultConfig())
		if !errors.Is(err, errOktaPushInboxLeaseLost) {
			t.Fatalf("processClaimedRows() error = %v, want lease lost", err)
		}
		if result.Processed != 0 || result.Ignored != 0 || result.DeadLetter != 0 {
			t.Fatalf("result = %+v, want no terminal processing", result)
		}

		var pushRuns, discoveryEvents, fullJobs int
		if err := pool.QueryRow(ctx, `
			SELECT count(*)
			FROM sync_runs
			WHERE source_kind = 'okta_push'
			  AND source_name = 'acme.okta.com'
		`).Scan(&pushRuns); err != nil {
			t.Fatalf("count okta_push sync runs: %v", err)
		}
		if err := pool.QueryRow(ctx, `
			SELECT count(*)
			FROM saas_app_events
			WHERE source_kind = 'okta'
			  AND source_name = 'acme.okta.com'
			  AND event_external_id = 'evt-lease-side-effects'
		`).Scan(&discoveryEvents); err != nil {
			t.Fatalf("count discovery events: %v", err)
		}
		if err := pool.QueryRow(ctx, `
			SELECT count(*)
			FROM sync_jobs
			WHERE lane = 'full'
			  AND connector_kind = 'okta'
			  AND source_name = 'acme.okta.com'
		`).Scan(&fullJobs); err != nil {
			t.Fatalf("count full sync jobs: %v", err)
		}
		if pushRuns != 0 || discoveryEvents != 0 || fullJobs != 0 {
			t.Fatalf("side effects = pushRuns:%d discoveryEvents:%d fullJobs:%d, want all zero", pushRuns, discoveryEvents, fullJobs)
		}
	})
}

func TestOktaPushInboxHeartbeatCancelsAfterLeaseLoss(t *testing.T) {
	withOktaIngestTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries) {
		queueOktaPushEvent(t, ctx, q, "acme.okta.com", "evt-heartbeat-lease-loss", `{
			"uuid": "evt-heartbeat-lease-loss",
			"eventType": "user.authentication.sso",
			"published": "2026-01-01T12:00:00Z",
			"actor": {"id": "00u1", "alternateId": "alice@example.com", "displayName": "Alice"},
			"target": [{"id": "0oa1", "type": "AppInstance", "alternateId": "https://app.example.com", "displayName": "Example App"}]
		}`)
		rows, err := q.ClaimQueuedOktaPushInboxEvents(ctx, gen.ClaimQueuedOktaPushInboxEventsParams{
			ClaimedBy:    "old-processor",
			ClaimToken:   "old-token",
			LeaseSeconds: 1,
			LimitRows:    10,
		})
		if err != nil {
			t.Fatalf("initial claim: %v", err)
		}
		claim, err := newProcessingClaim(q, rows, Config{LeaseTTL: time.Second})
		if err != nil {
			t.Fatalf("newProcessingClaim(): %v", err)
		}
		heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
		defer cancelHeartbeat()
		lostCh := make(chan error, 1)
		stopHeartbeat := startProcessingLeaseHeartbeat(heartbeatCtx, claim, Config{
			LeaseTTL:          time.Second,
			HeartbeatInterval: 10 * time.Millisecond,
		}.normalized(), func(err error) {
			lostCh <- err
		})
		defer stopHeartbeat()

		if _, err := pool.Exec(ctx, `
			UPDATE okta_push_inbox
			SET lease_expires_at = now() - interval '1 second'
			WHERE event_external_id = 'evt-heartbeat-lease-loss'
		`); err != nil {
			t.Fatalf("expire lease: %v", err)
		}
		if err := requeueStaleProcessingRows(ctx, q, Config{StaleProcessingAfter: time.Minute}); err != nil {
			t.Fatalf("requeueStaleProcessingRows(): %v", err)
		}
		if _, err := q.ClaimQueuedOktaPushInboxEvents(ctx, gen.ClaimQueuedOktaPushInboxEventsParams{
			ClaimedBy:    "new-processor",
			ClaimToken:   "new-token",
			LeaseSeconds: 60,
			LimitRows:    10,
		}); err != nil {
			t.Fatalf("reclaim: %v", err)
		}

		select {
		case err := <-lostCh:
			if !errors.Is(err, errOktaPushInboxLeaseLost) {
				t.Fatalf("heartbeat error = %v, want lease lost", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("heartbeat did not report lease loss")
		}
	})
}

func TestOktaPushInboxHeartbeatRenewsActiveLease(t *testing.T) {
	withOktaIngestTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries) {
		queueOktaPushEvent(t, ctx, q, "acme.okta.com", "evt-heartbeat-renewal", `{
			"uuid": "evt-heartbeat-renewal",
			"eventType": "user.authentication.sso",
			"published": "2026-01-01T12:00:00Z",
			"actor": {"id": "00u1", "alternateId": "alice@example.com", "displayName": "Alice"},
			"target": [{"id": "0oa1", "type": "AppInstance", "alternateId": "https://app.example.com", "displayName": "Example App"}]
		}`)
		rows, err := q.ClaimQueuedOktaPushInboxEvents(ctx, gen.ClaimQueuedOktaPushInboxEventsParams{
			ClaimedBy:    "heartbeat-owner",
			ClaimToken:   "heartbeat-token",
			LeaseSeconds: 1,
			LimitRows:    10,
		})
		if err != nil {
			t.Fatalf("initial claim: %v", err)
		}
		if len(rows) != 1 {
			t.Fatalf("initial claim returned %d rows, want 1", len(rows))
		}
		claim, err := newProcessingClaim(q, rows, Config{LeaseTTL: time.Second})
		if err != nil {
			t.Fatalf("newProcessingClaim(): %v", err)
		}

		initialLeaseExpiresAt := oktaPushInboxLeaseExpiresAt(t, ctx, pool, "evt-heartbeat-renewal")
		heartbeatInterval := 20 * time.Millisecond
		lostCh := make(chan error, 1)
		stopHeartbeat := startProcessingLeaseHeartbeat(ctx, claim, Config{
			LeaseTTL:          time.Second,
			HeartbeatInterval: heartbeatInterval,
		}.normalized(), func(err error) {
			lostCh <- err
		})
		defer stopHeartbeat()

		timeout := time.NewTimer(2 * time.Second)
		defer timeout.Stop()
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()
		for {
			leaseExpiresAt := oktaPushInboxLeaseExpiresAt(t, ctx, pool, "evt-heartbeat-renewal")
			if leaseExpiresAt.After(initialLeaseExpiresAt.Add(2 * heartbeatInterval)) {
				return
			}
			select {
			case err := <-lostCh:
				t.Fatalf("heartbeat reported lease loss: %v", err)
			case <-ticker.C:
			case <-timeout.C:
				t.Fatalf("lease_expires_at = %s, want after %s", leaseExpiresAt, initialLeaseExpiresAt.Add(2*heartbeatInterval))
			}
		}
	})
}

func TestOktaPushInboxHeartbeatStopCancelsInFlightRenewal(t *testing.T) {
	withOktaIngestTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries) {
		queueOktaPushEvent(t, ctx, q, "acme.okta.com", "evt-heartbeat-stop-cancel", `{
			"uuid": "evt-heartbeat-stop-cancel",
			"eventType": "user.authentication.sso",
			"published": "2026-01-01T12:00:00Z",
			"actor": {"id": "00u1", "alternateId": "alice@example.com", "displayName": "Alice"},
			"target": [{"id": "0oa1", "type": "AppInstance", "alternateId": "https://app.example.com", "displayName": "Example App"}]
		}`)
		rows, err := q.ClaimQueuedOktaPushInboxEvents(ctx, gen.ClaimQueuedOktaPushInboxEventsParams{
			ClaimedBy:    "heartbeat-owner",
			ClaimToken:   "heartbeat-token",
			LeaseSeconds: 5,
			LimitRows:    10,
		})
		if err != nil {
			t.Fatalf("initial claim: %v", err)
		}
		claim, err := newProcessingClaim(q, rows, Config{LeaseTTL: 5 * time.Second})
		if err != nil {
			t.Fatalf("newProcessingClaim(): %v", err)
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin lock transaction: %v", err)
		}
		defer func() {
			_ = tx.Rollback(context.Background())
		}()
		if _, err := tx.Exec(ctx, `
			SELECT id
			FROM okta_push_inbox
			WHERE event_external_id = $1
			FOR UPDATE
		`, "evt-heartbeat-stop-cancel"); err != nil {
			t.Fatalf("lock inbox row: %v", err)
		}

		lostCh := make(chan error, 1)
		heartbeatInterval := 750 * time.Millisecond
		stopHeartbeat := startProcessingLeaseHeartbeat(ctx, claim, Config{
			LeaseTTL:          5 * time.Second,
			HeartbeatInterval: heartbeatInterval,
		}.normalized(), func(err error) {
			lostCh <- err
		})

		time.Sleep(heartbeatInterval + 100*time.Millisecond)
		startedStopping := time.Now()
		stopHeartbeat()
		if elapsed := time.Since(startedStopping); elapsed > 300*time.Millisecond {
			t.Fatalf("stopHeartbeat() took %s, want under 300ms", elapsed)
		}

		select {
		case err := <-lostCh:
			t.Fatalf("heartbeat reported canceled renewal as lease loss: %v", err)
		default:
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

		rows, err := q.ClaimQueuedOktaPushInboxEvents(ctx, gen.ClaimQueuedOktaPushInboxEventsParams{
			LimitRows:    10,
			ClaimedBy:    "test-claimant",
			ClaimToken:   "test-token-retry",
			LeaseSeconds: 60,
		})
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
		claim, err := newProcessingClaim(q, rows, cfg)
		if err != nil {
			t.Fatalf("newProcessingClaim(): %v", err)
		}
		if err := retryOrDeadLetterRows(ctx, "acme.okta.com", rows, errStub("simulated transient failure"), cfg, claim); err != nil {
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

func oktaPushInboxIDByExternalID(t *testing.T, ctx context.Context, pool *pgxpool.Pool, eventExternalID string) int64 {
	t.Helper()
	return oktaPushInboxIDsByExternalID(t, ctx, pool, eventExternalID)[eventExternalID]
}

func oktaPushInboxLeaseExpiresAt(t *testing.T, ctx context.Context, pool *pgxpool.Pool, eventExternalID string) time.Time {
	t.Helper()

	var leaseExpiresAt pgtype.Timestamptz
	if err := pool.QueryRow(ctx, `
		SELECT lease_expires_at
		FROM okta_push_inbox
		WHERE event_external_id = $1
	`, eventExternalID).Scan(&leaseExpiresAt); err != nil {
		t.Fatalf("select lease_expires_at for %q: %v", eventExternalID, err)
	}
	if !leaseExpiresAt.Valid {
		t.Fatalf("lease_expires_at for %q is null", eventExternalID)
	}
	return leaseExpiresAt.Time
}

func oktaPushInboxIDsByExternalID(t *testing.T, ctx context.Context, pool *pgxpool.Pool, eventExternalIDs ...string) map[string]int64 {
	t.Helper()

	ids := make(map[string]int64, len(eventExternalIDs))
	for _, eventExternalID := range eventExternalIDs {
		var id int64
		if err := pool.QueryRow(ctx, `
			SELECT id
			FROM okta_push_inbox
			WHERE event_external_id = $1
		`, eventExternalID).Scan(&id); err != nil {
			t.Fatalf("select inbox id for %q: %v", eventExternalID, err)
		}
		ids[eventExternalID] = id
	}
	return ids
}

func waitForOktaPushInboxStatus(t *testing.T, ctx context.Context, pool *pgxpool.Pool, eventExternalID, want string) {
	t.Helper()

	timeout := time.NewTimer(2 * time.Second)
	defer timeout.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	var last string
	for {
		if err := pool.QueryRow(ctx, `
			SELECT status
			FROM okta_push_inbox
			WHERE event_external_id = $1
		`, eventExternalID).Scan(&last); err != nil {
			t.Fatalf("select inbox status for %q: %v", eventExternalID, err)
		}
		if last == want {
			return
		}
		select {
		case <-ticker.C:
		case <-timeout.C:
			t.Fatalf("status for %q = %q, want %q", eventExternalID, last, want)
		}
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
