package oktaingest

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/discovery"
	genericinbox "github.com/open-sspm/open-sspm/internal/ingest/inbox"
	"github.com/open-sspm/open-sspm/internal/metrics"
	"github.com/open-sspm/open-sspm/internal/records"
	"github.com/open-sspm/open-sspm/internal/testdb"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestProcessQueuedDispatchesOktaDelivery(t *testing.T) {
	withOktaIngestTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries) {
		enqueueOktaEventInboxDelivery(t, ctx, q, "acme.okta.com", "event_hook", "evt-generic-sso-1", `{
			"uuid": "evt-generic-sso-1",
			"eventType": "user.authentication.sso",
			"published": "2026-01-01T12:00:00Z",
			"actor": {"id": "00u1", "alternateId": "alice@example.com", "displayName": "Alice"},
			"target": [{"id": "0oa1", "type": "AppInstance", "alternateId": "https://app.example.com", "displayName": "Example App"}]
		}`)

		result, err := processQueued(ctx, q, pool, 100)
		if err != nil {
			t.Fatalf("ProcessQueued(): %v", err)
		}
		if result.Claimed != 1 || result.Processed != 1 {
			t.Fatalf("result = %+v, want claimed=1 processed=1", result)
		}

		if got := eventInboxStatus(t, ctx, pool, "provider:evt-generic-sso-1"); got != "processed" {
			t.Fatalf("event_inbox status = %q, want processed", got)
		}

		var signalKind string
		if err := pool.QueryRow(ctx, `
			SELECT signal_kind
			FROM saas_app_events
			WHERE source_kind = 'okta'
			  AND source_name = 'acme.okta.com'
			  AND event_external_id = 'evt-generic-sso-1'
		`).Scan(&signalKind); err != nil {
			t.Fatalf("select discovery event: %v", err)
		}
		if signalKind != discovery.SignalKindIDPSSO {
			t.Fatalf("signal_kind = %q, want %q", signalKind, discovery.SignalKindIDPSSO)
		}
		if got := countRows(t, ctx, pool, `
			SELECT count(*)
			FROM events
			WHERE source_kind = 'okta'
			  AND source_name = 'acme.okta.com'
			  AND provider_event_id = 'evt-generic-sso-1'
		`); got != 1 {
			t.Fatalf("canonical events = %d, want 1", got)
		}
		if got := countRows(t, ctx, pool, `
			SELECT count(*)
			FROM sync_jobs
			WHERE lane = 'tail'
			  AND connector_kind = 'okta'
			  AND source_name = 'acme.okta.com'
			  AND resource = 'system_log'
			  AND status = 'pending'
		`); got != 1 {
			t.Fatalf("tail jobs = %d, want 1", got)
		}
	})
}

func TestProcessQueuedQueuesOktaFullSyncForStateRefreshOnlyEvents(t *testing.T) {
	withOktaIngestTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries) {
		enqueueOktaEventInboxDelivery(t, ctx, q, "acme.okta.com", "event_hook", "evt-user-refresh", `{
			"uuid": "evt-user-refresh",
			"eventType": "user.lifecycle.deactivate",
			"published": "2026-01-01T12:00:00Z",
			"actor": {"id": "00u1", "alternateId": "alice@example.com", "displayName": "Alice"}
		}`)

		result, err := processQueued(ctx, q, pool, 100)
		if err != nil {
			t.Fatalf("ProcessQueued(): %v", err)
		}
		if result.Claimed != 1 || result.Processed != 1 {
			t.Fatalf("result = %+v, want claimed=1 processed=1", result)
		}
		assertQueuedOktaFullSync(t, ctx, pool, "acme.okta.com")
		assertStateRefreshStats(t, ctx, pool, "acme.okta.com", "user")
	})
}

func TestProcessQueuedIgnoresNonDiscoveryEvents(t *testing.T) {
	withOktaIngestTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries) {
		enqueueOktaEventInboxDelivery(t, ctx, q, "acme.okta.com", "event_hook", "evt-policy-1", `{
			"uuid": "evt-policy-1",
			"eventType": "policy.lifecycle.update",
			"published": "2026-01-01T12:00:00Z",
			"actor": {"id": "00u1", "alternateId": "alice@example.com", "displayName": "Alice"}
		}`)

		result, err := processQueued(ctx, q, pool, 100)
		if err != nil {
			t.Fatalf("ProcessQueued(): %v", err)
		}
		if result.Claimed != 1 || result.Ignored != 1 {
			t.Fatalf("result = %+v, want claimed=1 ignored=1", result)
		}
		if got := eventInboxStatus(t, ctx, pool, "provider:evt-policy-1"); got != "ignored" {
			t.Fatalf("event_inbox status = %q, want ignored", got)
		}
		if got := countRows(t, ctx, pool, `
			SELECT count(*)
			FROM events
			WHERE source_kind = 'okta'
			  AND provider_event_id = 'evt-policy-1'
		`); got != 0 {
			t.Fatalf("canonical events = %d, want 0", got)
		}
	})
}

func TestProcessQueuedObservesLoopTickOnEmptyQueue(t *testing.T) {
	withOktaIngestTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries) {
		ticks := 0
		result, err := processQueuedWithConfig(ctx, q, pool, 100, genericinbox.Config{
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

func TestRunLoopProcessesGenericInbox(t *testing.T) {
	withOktaIngestTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries) {
		enqueueOktaEventInboxDelivery(t, ctx, q, "acme.okta.com", "event_hook", "evt-runloop", `{
			"uuid": "evt-runloop",
			"eventType": "user.authentication.sso",
			"published": "2026-01-01T12:00:00Z",
			"actor": {"id": "00u1", "alternateId": "alice@example.com", "displayName": "Alice"},
			"target": [{"id": "0oa1", "type": "AppInstance", "alternateId": "https://app.example.com", "displayName": "Example App"}]
		}`)

		runCtx, cancel := context.WithCancel(ctx)
		errCh := make(chan error, 1)
		go func() {
			errCh <- genericinbox.RunLoop(runCtx, q, NewEventInboxHandler(q, pool), genericinbox.Config{
				BatchSize:       100,
				PollInterval:    10 * time.Millisecond,
				CleanupInterval: time.Hour,
				LeaseTTL:        time.Minute,
			})
		}()
		waitForEventInboxStatus(t, ctx, pool, "provider:evt-runloop", "processed")
		cancel()

		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("RunLoop() error = %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("RunLoop() did not stop after cancellation")
		}
	})
}

func TestRefreshMetricsReadsEventInbox(t *testing.T) {
	resetEventInboxMetrics()
	defer resetEventInboxMetrics()

	withOktaIngestTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries) {
		enqueueOktaEventInboxDelivery(t, ctx, q, "acme.okta.com", "eventbridge", "evt-dead", `{`)
		if _, err := processQueued(ctx, q, pool, 1); err != nil {
			t.Fatalf("ProcessQueued(): %v", err)
		}
		enqueueOktaEventInboxDelivery(t, ctx, q, "acme.okta.com", "event_hook", "evt-queued", `{
			"uuid": "evt-queued",
			"eventType": "user.authentication.sso",
			"published": "2026-01-01T12:00:00Z",
			"actor": {"id": "00u1", "alternateId": "alice@example.com", "displayName": "Alice"},
			"target": [{"id": "0oa1", "type": "AppInstance", "alternateId": "https://app.example.com", "displayName": "Example App"}]
		}`)

		if err := genericinbox.RefreshMetrics(ctx, q); err != nil {
			t.Fatalf("RefreshMetrics(): %v", err)
		}
		if got := testutil.ToFloat64(metrics.EventInboxQueueDepth.WithLabelValues(configstore.KindOkta, "acme.okta.com", "event_hook")); got != 1 {
			t.Fatalf("queue depth metric = %v, want 1", got)
		}
		if got := testutil.ToFloat64(metrics.EventInboxDeadLetterRows.WithLabelValues(configstore.KindOkta, "acme.okta.com", "eventbridge")); got != 1 {
			t.Fatalf("dead metric = %v, want 1", got)
		}
	})
}

func withOktaIngestTestDatabase(t *testing.T, fn func(context.Context, *pgxpool.Pool, *gen.Queries)) {
	t.Helper()

	testdb.WithDatabase(t, testdb.Options{NamePrefix: "opensspm_okta_ingest"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)
		fn(ctx, pool, gen.New(pool))
	})
}

func enqueueOktaEventInboxDelivery(t *testing.T, ctx context.Context, q *gen.Queries, sourceName, channel, eventExternalID, raw string) {
	t.Helper()

	store := genericinbox.NewStore(q)
	if _, err := store.Enqueue(ctx, genericinbox.Delivery{
		Source:          records.SourceRef{Kind: "okta", Name: sourceName},
		Channel:         channel,
		ExternalEventID: eventExternalID,
		DedupeKey:       "provider:" + eventExternalID,
		RawBody:         []byte(raw),
	}); err != nil {
		t.Fatalf("enqueue event inbox delivery: %v", err)
	}
}

func processQueued(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, limit int32) (genericinbox.RunOnceResult, error) {
	return processQueuedWithConfig(ctx, q, pool, limit, genericinbox.DefaultConfig())
}

func processQueuedWithConfig(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, limit int32, cfg genericinbox.Config) (genericinbox.RunOnceResult, error) {
	return genericinbox.ProcessQueued(ctx, q, NewEventInboxHandler(q, pool), limit, cfg)
}

func eventInboxStatus(t *testing.T, ctx context.Context, pool *pgxpool.Pool, dedupeKey string) string {
	t.Helper()

	var status string
	if err := pool.QueryRow(ctx, `
		SELECT status::text
		FROM event_inbox
		WHERE dedupe_key = $1
	`, dedupeKey).Scan(&status); err != nil {
		t.Fatalf("select event_inbox status for %q: %v", dedupeKey, err)
	}
	return status
}

func waitForEventInboxStatus(t *testing.T, ctx context.Context, pool *pgxpool.Pool, dedupeKey, want string) {
	t.Helper()

	timeout := time.NewTimer(2 * time.Second)
	defer timeout.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		last := eventInboxStatus(t, ctx, pool, dedupeKey)
		if last == want {
			return
		}
		select {
		case <-ticker.C:
		case <-timeout.C:
			t.Fatalf("event_inbox status for %q = %q, want %q", dedupeKey, last, want)
		}
	}
}

func countRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string) int {
	t.Helper()

	var count int
	if err := pool.QueryRow(ctx, query).Scan(&count); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	return count
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
		t.Fatalf("queued okta full sync jobs for %s = %d, want 1", sourceName, count)
	}
}

func assertStateRefreshStats(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sourceName, refreshKind string) {
	t.Helper()

	var raw []byte
	if err := pool.QueryRow(ctx, `
		SELECT stats
		FROM sync_runs
		WHERE source_kind = $1
		  AND source_name = $2
		  AND run_mode = $3
		ORDER BY id DESC
		LIMIT 1
	`, configstore.KindOkta, sourceName, string(registry.RunModeEventInbox)).Scan(&raw); err != nil {
		t.Fatalf("select sync run stats: %v", err)
	}
	var stats struct {
		Counts map[string]int64 `json:"counts"`
	}
	if err := json.Unmarshal(raw, &stats); err != nil {
		t.Fatalf("decode sync run stats: %v", err)
	}
	if got := stats.Counts["state_refresh_"+refreshKind]; got != 1 {
		t.Fatalf("state_refresh_%s count = %d, want 1", refreshKind, got)
	}
	if got := stats.Counts["state_refresh_events"]; got != 1 {
		t.Fatalf("state_refresh_events count = %d, want 1", got)
	}
}

func resetEventInboxMetrics() {
	keys := []struct {
		sourceKind string
		sourceName string
		channel    string
	}{
		{sourceKind: configstore.KindOkta, sourceName: "acme.okta.com", channel: "event_hook"},
		{sourceKind: configstore.KindOkta, sourceName: "acme.okta.com", channel: "eventbridge"},
	}
	for _, key := range keys {
		metrics.EventInboxQueueDepth.DeleteLabelValues(key.sourceKind, key.sourceName, key.channel)
		metrics.EventInboxDeadLetterRows.DeleteLabelValues(key.sourceKind, key.sourceName, key.channel)
		metrics.EventInboxLastReceivedTimestamp.DeleteLabelValues(key.sourceKind, key.sourceName, key.channel)
		metrics.EventInboxLastProcessedTimestamp.DeleteLabelValues(key.sourceKind, key.sourceName, key.channel)
	}
}
