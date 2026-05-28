package registry

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/readmodels"
	"github.com/open-sspm/open-sspm/internal/testdb"
)

const syncRunsTestConnectorSecretKey = "0123456789abcdef0123456789abcdef"

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
			RunModes:    []string{string(RunModeFull)},
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
		existingMode RunMode
		startSource  string
		startMode    RunMode
		sourceName   string
	}{
		{
			name:         "starting full reclaims discovery",
			existingKind: "okta",
			existingMode: RunModeDiscovery,
			startSource:  "okta",
			startMode:    RunModeFull,
			sourceName:   "acme",
		},
		{
			name:         "starting discovery reclaims full",
			existingKind: "okta",
			existingMode: RunModeFull,
			startSource:  "okta",
			startMode:    RunModeDiscovery,
			sourceName:   "acme",
		},
		{
			name:         "starting tail reclaims full",
			existingKind: "okta",
			existingMode: RunModeFull,
			startSource:  "okta",
			startMode:    RunModeTail,
			sourceName:   "acme",
		},
		{
			name:         "starting full reclaims tail",
			existingKind: "okta",
			existingMode: RunModeTail,
			startSource:  "okta",
			startMode:    RunModeFull,
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
					RunMode:    tt.existingMode,
					Status:     "running",
					StartedAt:  time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Microsecond),
				})

				newRunID, err := StartSyncRunWithMode(ctx, q, tt.startSource, tt.sourceName, tt.startMode)
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

				scopeKinds := []string{strings.ToLower(strings.TrimSpace(tt.startSource))}
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

func TestFinalizeAppRunRollsBackSuccessWhenReadModelRefreshFails(t *testing.T) {
	t.Parallel()

	withSyncRunsTestDB(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, migrator *migrate.Migrate) {
		migrateUp(t, migrator)

		runID, err := StartSyncRun(ctx, q, "github", "acme")
		if err != nil {
			t.Fatalf("StartSyncRun() err = %v", err)
		}

		if _, err := pool.Exec(ctx, `
			UPDATE connector_configs
			SET config = '"broken"'::jsonb
			WHERE kind = 'okta'
		`); err != nil {
			t.Fatalf("update connector_configs: %v", err)
		}

		finalizeCtx := readmodels.WithRefreshConfig(ctx, readmodels.RefreshConfig{})
		err = FinalizeAppRun(finalizeCtx, q, pool, runID, "github", "acme", 2*time.Second, false)
		if err == nil {
			t.Fatalf("FinalizeAppRun() error = nil, want non-nil")
		}

		state := fetchSyncRunState(t, ctx, pool, runID)
		if state.Status != "running" {
			t.Fatalf("status after failed finalize = %q, want running", state.Status)
		}

		failedErr := FailSyncRun(ctx, q, runID, err, SyncErrorKindDB)
		if failedErr == nil {
			t.Fatalf("FailSyncRun() error = nil, want original error")
		}

		state = fetchSyncRunState(t, ctx, pool, runID)
		if state.Status != SyncStatusError {
			t.Fatalf("status after FailSyncRun = %q, want %q", state.Status, SyncStatusError)
		}
	})
}

func TestFinalizeAppRunRefreshesNonHumanFreshnessFromCurrentSuccess(t *testing.T) {
	t.Parallel()

	withSyncRunsTestDB(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, migrator *migrate.Migrate) {
		migrateUp(t, migrator)

		upsertSyncRunsTestConnectorConfig(t, ctx, pool, configstore.KindGitHub, true, configstore.GitHubConfig{Org: "acme"})

		runID, err := StartSyncRun(ctx, q, configstore.KindGitHub, "acme")
		if err != nil {
			t.Fatalf("StartSyncRun() err = %v", err)
		}

		appAssetID := insertSyncRunsTestAppAsset(t, ctx, q, runID, configstore.KindGitHub, "acme", "github_app", "github-actions", "GitHub Actions")

		finalizeCtx := readmodels.WithRefreshConfig(ctx, readmodels.RefreshConfig{SyncGitHubInterval: time.Hour})
		if err := FinalizeAppRun(finalizeCtx, q, pool, runID, configstore.KindGitHub, "acme", 2*time.Second, false); err != nil {
			t.Fatalf("FinalizeAppRun() err = %v", err)
		}

		var (
			freshnessState   string
			hasStaleEvidence bool
		)
		if err := pool.QueryRow(ctx, `
			SELECT freshness_state, has_stale_evidence
			FROM non_human_principals
			WHERE principal_ref = $1
		`, "app-asset-"+strconv.FormatInt(appAssetID, 10)).Scan(&freshnessState, &hasStaleEvidence); err != nil {
			t.Fatalf("select non_human_principals freshness: %v", err)
		}
		if freshnessState != "current" {
			t.Fatalf("freshness_state = %q, want current", freshnessState)
		}
		if hasStaleEvidence {
			t.Fatalf("has_stale_evidence = true, want false")
		}
	})
}

type syncRunSeed struct {
	SourceKind string
	SourceName string
	RunMode    RunMode
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

	testdb.WithDatabase(t, testdb.Options{NamePrefix: "opensspm_syncruns"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		fn(ctx, pool, gen.New(pool), migrator)
	})
}

func migrateUp(t *testing.T, migrator *migrate.Migrate) {
	t.Helper()

	testdb.MigrateUp(t, migrator)
}

func insertSyncRunRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, seed syncRunSeed) int64 {
	t.Helper()

	seed.SourceKind = strings.TrimSpace(seed.SourceKind)
	seed.SourceName = strings.TrimSpace(seed.SourceName)
	seed.RunMode = seed.RunMode.Normalize()
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
		INSERT INTO sync_runs (source_kind, source_name, run_mode, status, started_at, finished_at, message, stats, error_kind)
		VALUES ($1, $2, $3, $4, $5, $6, $7, '{}'::jsonb, $8)
		RETURNING id
	`, seed.SourceKind, seed.SourceName, string(seed.RunMode), seed.Status, seed.StartedAt.UTC(), finishedAt, seed.Message, seed.ErrorKind).Scan(&id)
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

func upsertSyncRunsTestConnectorConfig(t *testing.T, ctx context.Context, pool *pgxpool.Pool, kind string, enabled bool, cfg any) {
	t.Helper()

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin connector config tx %s: %v", kind, err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	qtx := gen.New(pool).WithTx(tx)
	if _, err := qtx.UpdateConnectorConfigEnabled(ctx, gen.UpdateConnectorConfigEnabledParams{
		Kind:    kind,
		Enabled: enabled,
	}); err != nil {
		t.Fatalf("UpdateConnectorConfigEnabled(%s): %v", kind, err)
	}

	store := configstore.NewStore(nil, qtx, []byte(syncRunsTestConnectorSecretKey))
	if err := store.SaveConnectorConfigTx(ctx, qtx, kind, cfg); err != nil {
		t.Fatalf("SaveConnectorConfigTx(%s): %v", kind, err)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit connector config %s: %v", kind, err)
	}
}

func insertSyncRunsTestAppAsset(t *testing.T, ctx context.Context, q *gen.Queries, runID int64, sourceKind, sourceName, assetKind, externalID, displayName string) int64 {
	t.Helper()

	now := pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true}
	if _, err := q.UpsertAppAssetsBulkBySource(ctx, gen.UpsertAppAssetsBulkBySourceParams{
		SourceKind:        sourceKind,
		SourceName:        sourceName,
		SeenInRunID:       runID,
		AssetKinds:        []string{assetKind},
		ExternalIds:       []string{externalID},
		ParentExternalIds: []string{""},
		DisplayNames:      []string{displayName},
		Statuses:          []string{"active"},
		CreatedAtSources:  []pgtype.Timestamptz{now},
		UpdatedAtSources:  []pgtype.Timestamptz{now},
		RawJsons:          [][]byte{[]byte(`{}`)},
	}); err != nil {
		t.Fatalf("UpsertAppAssetsBulkBySource(%s/%s): %v", sourceKind, externalID, err)
	}

	appAsset, err := q.GetAppAssetBySourceAndKindAndExternalID(ctx, gen.GetAppAssetBySourceAndKindAndExternalIDParams{
		SourceKind: sourceKind,
		SourceName: sourceName,
		AssetKind:  assetKind,
		ExternalID: externalID,
	})
	if err != nil {
		t.Fatalf("GetAppAssetBySourceAndKindAndExternalID(%s/%s): %v", sourceKind, externalID, err)
	}

	return appAsset.ID
}
