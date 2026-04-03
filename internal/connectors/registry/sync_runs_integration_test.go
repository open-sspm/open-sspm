package registry

import (
	"context"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

func TestReclaimRunningSyncRunsBySourcePreservesLatestFinishedRun(t *testing.T) {
	t.Parallel()

	withSyncRunsTestDB(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, migrator *migrate.Migrate) {
		migrateUp(t, migrator)

		startedAt := time.Now().Add(-96 * time.Hour).UTC().Truncate(time.Microsecond)
		oldRunID := insertSyncRunRow(t, ctx, pool, syncRunSeed{
			SourceKind: "okta",
			SourceName: "acme",
			Status:     "running",
			StartedAt:  startedAt,
		})
		successFinishedAt := time.Now().Add(-time.Hour).UTC().Truncate(time.Microsecond)
		insertSyncRunRow(t, ctx, pool, syncRunSeed{
			SourceKind: "okta",
			SourceName: "acme",
			Status:     "success",
			StartedAt:  successFinishedAt.Add(-5 * time.Minute),
			FinishedAt: &successFinishedAt,
		})

		rows, err := q.ReclaimRunningSyncRunsBySource(ctx, gen.ReclaimRunningSyncRunsBySourceParams{
			SourceKinds: []string{"okta"},
			SourceName:  "acme",
			Message:     syncRunReclaimedMessage,
			ErrorKind:   SyncErrorKindStaleReclaimed,
		})
		if err != nil {
			t.Fatalf("ReclaimRunningSyncRunsBySource() err = %v", err)
		}
		if rows != 1 {
			t.Fatalf("ReclaimRunningSyncRunsBySource() rows = %d, want 1", rows)
		}

		state := fetchSyncRunState(t, ctx, pool, oldRunID)
		if state.Status != SyncStatusCanceled {
			t.Fatalf("reclaimed status = %q, want %q", state.Status, SyncStatusCanceled)
		}
		if state.ErrorKind != SyncErrorKindStaleReclaimed {
			t.Fatalf("reclaimed error_kind = %q, want %q", state.ErrorKind, SyncErrorKindStaleReclaimed)
		}
		if state.Message != syncRunReclaimedMessage {
			t.Fatalf("reclaimed message = %q, want %q", state.Message, syncRunReclaimedMessage)
		}
		if !state.FinishedAt.Valid {
			t.Fatalf("reclaimed finished_at is invalid")
		}
		if !state.FinishedAt.Time.Equal(state.StartedAt) {
			t.Fatalf("reclaimed finished_at = %v, want started_at %v", state.FinishedAt.Time, state.StartedAt)
		}

		rollups, err := q.GetSyncRunRollupsForSources(ctx, gen.GetSyncRunRollupsForSourcesParams{
			SourceKinds: []string{"okta"},
			SourceNames: []string{"acme"},
		})
		if err != nil {
			t.Fatalf("GetSyncRunRollupsForSources() err = %v", err)
		}
		if len(rollups) != 1 {
			t.Fatalf("GetSyncRunRollupsForSources() rows = %d, want 1", len(rollups))
		}
		row := rollups[0]
		if row.RunningCount != 0 {
			t.Fatalf("running_count = %d, want 0", row.RunningCount)
		}
		if !row.LastRunStatus.Valid || row.LastRunStatus.String != "success" {
			t.Fatalf("last_run_status = %+v, want success", row.LastRunStatus)
		}
	})
}

func TestSyncRunTransitionsNoOpAfterReclaim(t *testing.T) {
	t.Parallel()

	withSyncRunsTestDB(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, migrator *migrate.Migrate) {
		migrateUp(t, migrator)

		startedAt := time.Now().Add(-24 * time.Hour).UTC().Truncate(time.Microsecond)
		runID := insertSyncRunRow(t, ctx, pool, syncRunSeed{
			SourceKind: "github",
			SourceName: "acme",
			Status:     "running",
			StartedAt:  startedAt,
		})

		if _, err := q.ReclaimRunningSyncRunsBySource(ctx, gen.ReclaimRunningSyncRunsBySourceParams{
			SourceKinds: []string{"github"},
			SourceName:  "acme",
			Message:     syncRunReclaimedMessage,
			ErrorKind:   SyncErrorKindStaleReclaimed,
		}); err != nil {
			t.Fatalf("ReclaimRunningSyncRunsBySource() err = %v", err)
		}

		if err := q.MarkSyncRunSuccess(ctx, gen.MarkSyncRunSuccessParams{
			ID:    runID,
			Stats: []byte(`{"counts":{}}`),
		}); err != nil {
			t.Fatalf("MarkSyncRunSuccess() err = %v", err)
		}
		if err := q.FailSyncRun(ctx, gen.FailSyncRunParams{
			ID:        runID,
			Status:    SyncStatusError,
			Message:   "late failure",
			ErrorKind: SyncErrorKindDB,
		}); err != nil {
			t.Fatalf("FailSyncRun() err = %v", err)
		}

		state := fetchSyncRunState(t, ctx, pool, runID)
		if state.Status != SyncStatusCanceled {
			t.Fatalf("status after late transitions = %q, want %q", state.Status, SyncStatusCanceled)
		}
		if state.ErrorKind != SyncErrorKindStaleReclaimed {
			t.Fatalf("error_kind after late transitions = %q, want %q", state.ErrorKind, SyncErrorKindStaleReclaimed)
		}
		if state.Message != syncRunReclaimedMessage {
			t.Fatalf("message after late transitions = %q, want %q", state.Message, syncRunReclaimedMessage)
		}
		if !state.FinishedAt.Valid || !state.FinishedAt.Time.Equal(state.StartedAt) {
			t.Fatalf("finished_at after late transitions = %+v, want started_at %v", state.FinishedAt, state.StartedAt)
		}
	})
}

func TestStartSyncRunReclaimsExistingRunningRows(t *testing.T) {
	t.Parallel()

	withSyncRunsTestDB(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, migrator *migrate.Migrate) {
		migrateUp(t, migrator)

		oldRunID := insertSyncRunRow(t, ctx, pool, syncRunSeed{
			SourceKind: "google_workspace",
			SourceName: "C0123",
			Status:     "running",
			StartedAt:  time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Microsecond),
		})

		newRunID, err := StartSyncRun(ctx, q, "google_workspace", "C0123")
		if err != nil {
			t.Fatalf("StartSyncRun() err = %v", err)
		}
		if newRunID == 0 {
			t.Fatalf("StartSyncRun() returned zero id")
		}

		oldState := fetchSyncRunState(t, ctx, pool, oldRunID)
		if oldState.Status != SyncStatusCanceled {
			t.Fatalf("old run status = %q, want %q", oldState.Status, SyncStatusCanceled)
		}
		if oldState.ErrorKind != SyncErrorKindStaleReclaimed {
			t.Fatalf("old run error_kind = %q, want %q", oldState.ErrorKind, SyncErrorKindStaleReclaimed)
		}

		newState := fetchSyncRunState(t, ctx, pool, newRunID)
		if newState.Status != "running" {
			t.Fatalf("new run status = %q, want running", newState.Status)
		}
		if newState.FinishedAt.Valid {
			t.Fatalf("new run finished_at = %+v, want NULL", newState.FinishedAt)
		}

		var (
			runningCount int
			runningID    int64
		)
		if err := pool.QueryRow(ctx, `
			SELECT count(*), COALESCE(max(id), 0)
			FROM sync_runs
			WHERE source_kind = $1
			  AND source_name = $2
			  AND status = 'running'
		`, "google_workspace", "C0123").Scan(&runningCount, &runningID); err != nil {
			t.Fatalf("count running sync_runs: %v", err)
		}
		if runningCount != 1 {
			t.Fatalf("running sync_runs = %d, want 1", runningCount)
		}
		if runningID != newRunID {
			t.Fatalf("running sync_run id = %d, want %d", runningID, newRunID)
		}
	})
}

func TestStartSyncRunReclaimsCrossLaneRunningRows(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		existingKind string
		startSource  string
		sourceName   string
	}{
		{
			name:         "starting full reclaims discovery",
			existingKind: "okta_discovery",
			startSource:  "okta",
			sourceName:   "acme",
		},
		{
			name:         "starting discovery reclaims full",
			existingKind: "okta",
			startSource:  "okta_discovery",
			sourceName:   "acme",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			withSyncRunsTestDB(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, migrator *migrate.Migrate) {
				migrateUp(t, migrator)

				oldRunID := insertSyncRunRow(t, ctx, pool, syncRunSeed{
					SourceKind: tt.existingKind,
					SourceName: tt.sourceName,
					Status:     "running",
					StartedAt:  time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Microsecond),
				})

				newRunID, err := StartSyncRun(ctx, q, tt.startSource, tt.sourceName)
				if err != nil {
					t.Fatalf("StartSyncRun() err = %v", err)
				}

				oldState := fetchSyncRunState(t, ctx, pool, oldRunID)
				if oldState.Status != SyncStatusCanceled {
					t.Fatalf("old run status = %q, want %q", oldState.Status, SyncStatusCanceled)
				}
				if oldState.ErrorKind != SyncErrorKindStaleReclaimed {
					t.Fatalf("old run error_kind = %q, want %q", oldState.ErrorKind, SyncErrorKindStaleReclaimed)
				}

				newState := fetchSyncRunState(t, ctx, pool, newRunID)
				if newState.Status != "running" {
					t.Fatalf("new run status = %q, want running", newState.Status)
				}

				scopeKinds := SyncRunScopeKinds(tt.startSource)
				var runningCount int
				if err := pool.QueryRow(ctx, `
					SELECT count(*)
					FROM sync_runs
					WHERE source_kind = ANY($1::text[])
					  AND source_name = $2
					  AND status = 'running'
				`, scopeKinds, tt.sourceName).Scan(&runningCount); err != nil {
					t.Fatalf("count running sync_runs in family: %v", err)
				}
				if runningCount != 1 {
					t.Fatalf("running sync_runs in family = %d, want 1", runningCount)
				}
			})
		})
	}
}

func TestSyncRunsMigration31ReclaimsOnlyOldRunningRows(t *testing.T) {
	t.Parallel()

	withSyncRunsTestDB(t, func(ctx context.Context, pool *pgxpool.Pool, _ *gen.Queries, migrator *migrate.Migrate) {
		migrateToVersion(t, migrator, 30)

		oldRunID := insertSyncRunRow(t, ctx, pool, syncRunSeed{
			SourceKind: "okta",
			SourceName: "legacy",
			Status:     "running",
			StartedAt:  time.Now().Add(-96 * time.Hour).UTC().Truncate(time.Microsecond),
		})
		recentRunID := insertSyncRunRow(t, ctx, pool, syncRunSeed{
			SourceKind: "okta",
			SourceName: "recent",
			Status:     "running",
			StartedAt:  time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Microsecond),
		})

		migrateUp(t, migrator)

		oldState := fetchSyncRunState(t, ctx, pool, oldRunID)
		if oldState.Status != SyncStatusCanceled {
			t.Fatalf("old migration-cleaned status = %q, want %q", oldState.Status, SyncStatusCanceled)
		}
		if oldState.ErrorKind != SyncErrorKindStaleReclaimed {
			t.Fatalf("old migration-cleaned error_kind = %q, want %q", oldState.ErrorKind, SyncErrorKindStaleReclaimed)
		}
		if !oldState.FinishedAt.Valid || !oldState.FinishedAt.Time.Equal(oldState.StartedAt) {
			t.Fatalf("old migration-cleaned finished_at = %+v, want started_at %v", oldState.FinishedAt, oldState.StartedAt)
		}

		recentState := fetchSyncRunState(t, ctx, pool, recentRunID)
		if recentState.Status != "running" {
			t.Fatalf("recent status = %q, want running", recentState.Status)
		}
		if recentState.FinishedAt.Valid {
			t.Fatalf("recent finished_at = %+v, want NULL", recentState.FinishedAt)
		}
	})
}

type syncRunSeed struct {
	SourceKind string
	SourceName string
	Status     string
	StartedAt  time.Time
	FinishedAt *time.Time
	Message    string
	ErrorKind  string
}

type syncRunState struct {
	Status     string
	StartedAt  time.Time
	FinishedAt pgtype.Timestamptz
	Message    string
	ErrorKind  string
}

func withSyncRunsTestDB(t *testing.T, fn func(context.Context, *pgxpool.Pool, *gen.Queries, *migrate.Migrate)) {
	t.Helper()

	baseURL := strings.TrimSpace(os.Getenv("OPENSSPM_TEST_DATABASE_URL"))
	if baseURL == "" {
		t.Skip("OPENSSPM_TEST_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	adminURL, err := testDatabaseAdminURL(baseURL)
	if err != nil {
		t.Fatalf("testDatabaseAdminURL() err = %v", err)
	}

	adminConn, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatalf("pgx.Connect(admin) err = %v", err)
	}
	defer adminConn.Close(ctx)

	dbName := "opensspm_syncruns_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := adminConn.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{dbName}.Sanitize()); err != nil {
		t.Fatalf("CREATE DATABASE err = %v", err)
	}
	defer func() {
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer dropCancel()
		_, _ = adminConn.Exec(dropCtx, "DROP DATABASE IF EXISTS "+pgx.Identifier{dbName}.Sanitize()+" WITH (FORCE)")
	}()

	testURL, err := testDatabaseURLWithName(baseURL, dbName)
	if err != nil {
		t.Fatalf("testDatabaseURLWithName() err = %v", err)
	}

	migrator, err := migrate.New("file://"+testMigrationsDir(t), testURL)
	if err != nil {
		t.Fatalf("migrate.New() err = %v", err)
	}
	defer func() {
		_, _ = migrator.Close()
	}()

	pool, err := pgxpool.New(ctx, testURL)
	if err != nil {
		t.Fatalf("pgxpool.New() err = %v", err)
	}
	defer pool.Close()

	fn(ctx, pool, gen.New(pool), migrator)
}

func migrateUp(t *testing.T, migrator *migrate.Migrate) {
	t.Helper()

	if err := migrator.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		t.Fatalf("migrate up: %v", err)
	}
}

func migrateToVersion(t *testing.T, migrator *migrate.Migrate, version uint) {
	t.Helper()

	if err := migrator.Migrate(version); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		t.Fatalf("migrate to version %d: %v", version, err)
	}
}

func insertSyncRunRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, seed syncRunSeed) int64 {
	t.Helper()

	seed.SourceKind = strings.TrimSpace(seed.SourceKind)
	seed.SourceName = strings.TrimSpace(seed.SourceName)
	seed.Status = strings.TrimSpace(seed.Status)
	seed.Message = strings.TrimSpace(seed.Message)
	seed.ErrorKind = strings.TrimSpace(seed.ErrorKind)

	var (
		id         int64
		finishedAt pgtype.Timestamptz
	)
	if seed.FinishedAt != nil {
		finishedAt = pgtype.Timestamptz{Time: seed.FinishedAt.UTC(), Valid: true}
	}

	err := pool.QueryRow(ctx, `
		INSERT INTO sync_runs (source_kind, source_name, status, started_at, finished_at, message, stats, error_kind)
		VALUES ($1, $2, $3, $4, $5, $6, '{}'::jsonb, $7)
		RETURNING id
	`, seed.SourceKind, seed.SourceName, seed.Status, seed.StartedAt.UTC(), finishedAt, seed.Message, seed.ErrorKind).Scan(&id)
	if err != nil {
		t.Fatalf("insert sync run %s/%s: %v", seed.SourceKind, seed.SourceName, err)
	}
	return id
}

func fetchSyncRunState(t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID int64) syncRunState {
	t.Helper()

	var state syncRunState
	err := pool.QueryRow(ctx, `
		SELECT status, started_at, finished_at, message, error_kind
		FROM sync_runs
		WHERE id = $1
	`, runID).Scan(&state.Status, &state.StartedAt, &state.FinishedAt, &state.Message, &state.ErrorKind)
	if err != nil {
		t.Fatalf("fetch sync run %d: %v", runID, err)
	}
	return state
}

func testMigrationsDir(t *testing.T) string {
	t.Helper()

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "..", "db", "migrations")
}

func testDatabaseAdminURL(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	parsed.Path = "/postgres"
	return parsed.String(), nil
}

func testDatabaseURLWithName(raw, dbName string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	parsed.Path = "/" + dbName
	return parsed.String(), nil
}
