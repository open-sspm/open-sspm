package tail

import (
	"context"
	"testing"

	"github.com/golang-migrate/migrate/v4"
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
