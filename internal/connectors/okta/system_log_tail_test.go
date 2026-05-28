package okta

import (
	"context"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/testdb"
)

func TestOktaSystemLogTailWritesCanonicalEventsAndAdvancesCursor(t *testing.T) {
	testdb.WithDatabase(t, testdb.Options{NamePrefix: "okta_system_log_tail"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)

		q := gen.New(pool)
		sourceName := "example.okta.com"
		runID, err := registry.StartSyncRunWithMode(ctx, q, "okta", sourceName, registry.RunModeTail)
		if err != nil {
			t.Fatalf("StartSyncRun() err = %v", err)
		}

		published := time.Date(2026, time.May, 16, 12, 0, 0, 0, time.UTC)
		var requestedSince time.Time
		integration := &OktaIntegration{
			sourceName: sourceName,
			systemLogLister: func(_ context.Context, since time.Time) ([]SystemLogEvent, error) {
				requestedSince = since
				return []SystemLogEvent{
					{
						ID:            "evt-1",
						EventType:     "user.authentication.sso",
						Published:     published,
						OutcomeResult: "SUCCESS",
						AppID:         "0oa1",
						AppName:       "Payroll",
						ActorID:       "00u1",
						ActorEmail:    "alice@example.com",
						RawJSON:       []byte(`{"uuid":"evt-1","eventType":"user.authentication.sso"}`),
					},
					{
						ID:        "evt-ignored",
						EventType: "policy.lifecycle.update",
						Published: published.Add(time.Minute),
						AppID:     "0oa1",
						AppName:   "Payroll",
						RawJSON:   []byte(`{"uuid":"evt-ignored","eventType":"policy.lifecycle.update"}`),
					},
				}, nil
			},
		}

		stats, err := integration.tailSystemLogWithCursor(ctx, pool, runID, SystemLogTailResource)
		if err != nil {
			t.Fatalf("tailSystemLogWithCursor() err = %v", err)
		}
		if requestedSince.IsZero() {
			t.Fatal("expected lister to receive a since watermark")
		}
		if stats.Fetched != 2 || stats.Written != 1 || stats.Ignored != 1 || stats.Duplicates != 0 {
			t.Fatalf("tail stats = %+v, want fetched=2 written=1 ignored=1 duplicates=0", stats)
		}

		var eventCount, targetCount int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM events WHERE source_kind = 'okta' AND source_name = $1`, sourceName).Scan(&eventCount); err != nil {
			t.Fatalf("count canonical events: %v", err)
		}
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM event_targets`).Scan(&targetCount); err != nil {
			t.Fatalf("count event targets: %v", err)
		}
		if eventCount != 1 {
			t.Fatalf("event count = %d, want 1", eventCount)
		}
		if targetCount != 1 {
			t.Fatalf("target count = %d, want 1", targetCount)
		}

		cursor, err := q.GetConnectorCursorState(ctx, gen.GetConnectorCursorStateParams{
			SourceKind: "okta",
			SourceName: sourceName,
			Resource:   SystemLogTailResource,
		})
		if err != nil {
			t.Fatalf("GetConnectorCursorState() err = %v", err)
		}
		if !cursor.Watermark.Valid || !cursor.Watermark.Time.Equal(published.Add(time.Minute)) {
			t.Fatalf("cursor watermark = %v, want %v", cursor.Watermark, published.Add(time.Minute))
		}
		if !cursor.LastRunID.Valid || cursor.LastRunID.Int64 != runID {
			t.Fatalf("cursor last_run_id = %+v, want %d", cursor.LastRunID, runID)
		}
		if cursor.LastProviderEventID != "evt-ignored" {
			t.Fatalf("last provider event id = %q, want evt-ignored", cursor.LastProviderEventID)
		}
	})
}
