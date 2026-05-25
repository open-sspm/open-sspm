package tail

import (
	"context"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/testdb"
)

func TestSchedulerCoalescesTailWakeupsBySourceResource(t *testing.T) {
	testdb.WithDatabase(t, testdb.Options{NamePrefix: "tail_scheduler"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)

		scheduler := NewScheduler(gen.New(pool))
		created, err := scheduler.Wake(ctx, Wakeup{
			SourceKind: "Okta",
			SourceName: "example.okta.com",
			Resource:   "system_log",
			Reason:     "push_wakeup",
			Priority:   10,
		})
		if err != nil {
			t.Fatalf("first wake: %v", err)
		}
		if !created {
			t.Fatal("expected first wake to create a tail job")
		}

		created, err = scheduler.Wake(ctx, Wakeup{
			SourceKind: "okta",
			SourceName: "example.okta.com",
			Resource:   "system_log",
			Reason:     "push_wakeup",
			Priority:   10,
		})
		if err != nil {
			t.Fatalf("duplicate wake: %v", err)
		}
		if created {
			t.Fatal("expected duplicate wake to coalesce")
		}

		var count int
		if err := pool.QueryRow(ctx, `
			SELECT count(*)
			FROM sync_jobs
			WHERE lane = 'tail'
			  AND connector_kind = 'okta'
			  AND source_name = 'example.okta.com'
			  AND resource = 'system_log'
		`).Scan(&count); err != nil {
			t.Fatalf("count tail jobs: %v", err)
		}
		if count != 1 {
			t.Fatalf("tail jobs = %d, want 1", count)
		}
	})
}

func TestUpsertConnectorCursorStatePreservesSuccessfulRunOnHeartbeat(t *testing.T) {
	testdb.WithDatabase(t, testdb.Options{NamePrefix: "tail_cursor"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)

		q := gen.New(pool)
		now := time.Now().UTC().Truncate(time.Second)
		var runID int64
		if err := pool.QueryRow(ctx, `
			INSERT INTO sync_runs (source_kind, source_name, status, started_at, finished_at, message)
			VALUES ('okta', 'example.okta.com', 'success', $1, $2, '')
			RETURNING id
		`, now.Add(-time.Minute), now).Scan(&runID); err != nil {
			t.Fatalf("insert sync run: %v", err)
		}

		if err := q.UpsertConnectorCursorState(ctx, gen.UpsertConnectorCursorStateParams{
			SourceKind:          "okta",
			SourceName:          "example.okta.com",
			Resource:            "system_log",
			CursorKind:          "log_cursor",
			CursorJson:          []byte(`{"phase":"success"}`),
			LastSuccessAt:       pgtype.Timestamptz{Time: now, Valid: true},
			LastAttemptAt:       pgtype.Timestamptz{Time: now, Valid: true},
			LastRunID:           pgtype.Int8{Int64: runID, Valid: true},
			LastProviderEventID: "evt-1",
			NeedsFullResync:     false,
		}); err != nil {
			t.Fatalf("initial UpsertConnectorCursorState(): %v", err)
		}

		heartbeatAt := now.Add(30 * time.Second)
		if err := q.UpsertConnectorCursorState(ctx, gen.UpsertConnectorCursorStateParams{
			SourceKind:          "okta",
			SourceName:          "example.okta.com",
			Resource:            "system_log",
			CursorKind:          "log_cursor",
			CursorJson:          []byte(`{"phase":"heartbeat"}`),
			LastAttemptAt:       pgtype.Timestamptz{Time: heartbeatAt, Valid: true},
			LastError:           "",
			LastProviderEventID: "evt-1",
			NeedsFullResync:     false,
		}); err != nil {
			t.Fatalf("heartbeat UpsertConnectorCursorState(): %v", err)
		}

		state, err := q.GetConnectorCursorState(ctx, gen.GetConnectorCursorStateParams{
			SourceKind: "okta",
			SourceName: "example.okta.com",
			Resource:   "system_log",
		})
		if err != nil {
			t.Fatalf("GetConnectorCursorState(): %v", err)
		}
		if !state.LastSuccessAt.Valid || !state.LastSuccessAt.Time.Equal(now) {
			t.Fatalf("last_success_at = %+v, want %s", state.LastSuccessAt, now)
		}
		if !state.LastRunID.Valid || state.LastRunID.Int64 != runID {
			t.Fatalf("last_run_id = %+v, want %d", state.LastRunID, runID)
		}
		if !state.LastAttemptAt.Valid || !state.LastAttemptAt.Time.Equal(heartbeatAt) {
			t.Fatalf("last_attempt_at = %+v, want %s", state.LastAttemptAt, heartbeatAt)
		}
	})
}

func TestCursorStoreInitializesAndLocksCursorState(t *testing.T) {
	testdb.WithDatabase(t, testdb.Options{NamePrefix: "tail_cursor"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)

		store := NewCursorStore(pool)
		err := store.WithLockedCursor(ctx, CursorKey{
			SourceKind: "okta",
			SourceName: "example.okta.com",
			Resource:   "system_log",
			CursorKind: "log_cursor",
		}, func(ctx context.Context, q *gen.Queries, state gen.ConnectorCursorState) error {
			if state.SourceKind != "okta" || state.SourceName != "example.okta.com" || state.Resource != "system_log" {
				t.Fatalf("state = %+v, want okta/example/system_log", state)
			}
			return q.MarkConnectorCursorNeedsFullResync(ctx, gen.MarkConnectorCursorNeedsFullResyncParams{
				LastError:  "test",
				SourceKind: state.SourceKind,
				SourceName: state.SourceName,
				Resource:   state.Resource,
			})
		})
		if err != nil {
			t.Fatalf("WithLockedCursor(): %v", err)
		}

		var needsFull bool
		if err := pool.QueryRow(ctx, `
			SELECT needs_full_resync
			FROM connector_cursor_state
			WHERE source_kind = 'okta'
			  AND source_name = 'example.okta.com'
			  AND resource = 'system_log'
		`).Scan(&needsFull); err != nil {
			t.Fatalf("select cursor state: %v", err)
		}
		if !needsFull {
			t.Fatal("expected cursor state update made under lock to commit")
		}
	})
}
