package readmodels

import (
	"context"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

const readModelsTestConnectorSecretKey = "0123456789abcdef0123456789abcdef"

func TestProjectorRefreshConnectorSourceStateUsesLatestFullRunForFreshness(t *testing.T) {
	t.Parallel()

	withReadModelsTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, migrator *migrate.Migrate) {
		migrateUpReadModels(t, migrator)

		insertReadModelsConnectorConfig(t, ctx, pool, configstore.KindEntra, true, configstore.EntraConfig{
			TenantID:         "11111111-1111-1111-1111-111111111111",
			ClientID:         "22222222-2222-2222-2222-222222222222",
			ClientSecret:     "secret",
			DiscoveryEnabled: true,
		})

		now := time.Now().UTC().Truncate(time.Second)
		fullFinishedAt := now.Add(-4 * time.Hour)
		discoveryFinishedAt := now.Add(-5 * time.Minute)

		insertReadModelsSyncRun(t, ctx, pool, "entra", "11111111-1111-1111-1111-111111111111", fullFinishedAt)
		insertReadModelsSyncRun(t, ctx, pool, "entra_discovery", "11111111-1111-1111-1111-111111111111", discoveryFinishedAt)

		projector := NewProjector(pool, nil, RefreshConfig{SyncEntraInterval: time.Hour})
		if err := projector.RefreshConnectorSourceState(ctx); err != nil {
			t.Fatalf("RefreshConnectorSourceState(): %v", err)
		}

		lastSuccessAt, freshUntilAt := fetchConnectorSourceStateTimes(t, ctx, pool, "entra", "11111111-1111-1111-1111-111111111111")
		if !lastSuccessAt.Equal(fullFinishedAt) {
			t.Fatalf("last_success_at = %s, want %s", lastSuccessAt, fullFinishedAt)
		}

		wantFreshUntil := fullFinishedAt.Add(2 * time.Hour)
		if !freshUntilAt.Equal(wantFreshUntil) {
			t.Fatalf("fresh_until_at = %s, want %s", freshUntilAt, wantFreshUntil)
		}
	})
}

func TestProjectorRefreshConnectorSourceStateKeepsCurrentFullRunCurrent(t *testing.T) {
	t.Parallel()

	withReadModelsTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, migrator *migrate.Migrate) {
		migrateUpReadModels(t, migrator)

		insertReadModelsConnectorConfig(t, ctx, pool, configstore.KindEntra, true, configstore.EntraConfig{
			TenantID:         "11111111-1111-1111-1111-111111111111",
			ClientID:         "22222222-2222-2222-2222-222222222222",
			ClientSecret:     "secret",
			DiscoveryEnabled: true,
		})

		now := time.Now().UTC().Truncate(time.Second)
		fullFinishedAt := now.Add(-30 * time.Minute)
		discoveryFinishedAt := now.Add(-5 * time.Minute)

		insertReadModelsSyncRun(t, ctx, pool, "entra", "11111111-1111-1111-1111-111111111111", fullFinishedAt)
		insertReadModelsSyncRun(t, ctx, pool, "entra_discovery", "11111111-1111-1111-1111-111111111111", discoveryFinishedAt)

		projector := NewProjector(pool, nil, RefreshConfig{SyncEntraInterval: time.Hour})
		if err := projector.RefreshConnectorSourceState(ctx); err != nil {
			t.Fatalf("RefreshConnectorSourceState(): %v", err)
		}

		lastSuccessAt, freshUntilAt := fetchConnectorSourceStateTimes(t, ctx, pool, "entra", "11111111-1111-1111-1111-111111111111")
		if !lastSuccessAt.Equal(fullFinishedAt) {
			t.Fatalf("last_success_at = %s, want %s", lastSuccessAt, fullFinishedAt)
		}
		if !freshUntilAt.After(now) {
			t.Fatalf("fresh_until_at = %s, want a current freshness window after %s", freshUntilAt, now)
		}
	})
}

func TestProjectorRefreshConnectorSourceStateDoesNotLetDiscoveryFreshnessKeepNonHumanCurrent(t *testing.T) {
	t.Parallel()

	withReadModelsTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, migrator *migrate.Migrate) {
		migrateUpReadModels(t, migrator)

		sourceName := "11111111-1111-1111-1111-111111111111"
		insertReadModelsConnectorConfig(t, ctx, pool, configstore.KindEntra, true, configstore.EntraConfig{
			TenantID:         sourceName,
			ClientID:         "22222222-2222-2222-2222-222222222222",
			ClientSecret:     "secret",
			DiscoveryEnabled: true,
		})

		now := time.Now().UTC().Truncate(time.Second)
		fullFinishedAt := now.Add(-4 * time.Hour)
		discoveryFinishedAt := now.Add(-5 * time.Minute)
		fullRunID := insertReadModelsSyncRun(t, ctx, pool, "entra", sourceName, fullFinishedAt)
		insertReadModelsSyncRun(t, ctx, pool, "entra_discovery", sourceName, discoveryFinishedAt)

		appAssetID := upsertReadModelsAppAsset(t, ctx, q, fullRunID, "entra", sourceName, "entra_service_principal", "svc-123", "Azure Service Principal")

		if _, err := q.RefreshAllAppAssetReadModels(ctx); err != nil {
			t.Fatalf("RefreshAllAppAssetReadModels(): %v", err)
		}

		projector := NewProjector(pool, nil, RefreshConfig{SyncEntraInterval: time.Hour})
		if err := projector.RefreshConnectorSourceState(ctx); err != nil {
			t.Fatalf("RefreshConnectorSourceState(): %v", err)
		}

		if _, err := q.RefreshAllNonHumanPrincipalReadModelsSafely(ctx); err != nil {
			t.Fatalf("RefreshAllNonHumanPrincipalReadModelsSafely(): %v", err)
		}

		principal, err := q.GetNonHumanPrincipalByRef(ctx, "app-asset-"+int64String(appAssetID))
		if err != nil {
			t.Fatalf("GetNonHumanPrincipalByRef(): %v", err)
		}
		if principal.FreshnessState != "stale" {
			t.Fatalf("freshness_state = %q, want %q", principal.FreshnessState, "stale")
		}
		if !principal.HasStaleEvidence {
			t.Fatalf("has_stale_evidence = false, want true")
		}
	})
}

func withReadModelsTestDatabase(t *testing.T, fn func(context.Context, *pgxpool.Pool, *gen.Queries, *migrate.Migrate)) {
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

	dbName := "opensspm_readmodels_" + strings.ReplaceAll(uuid.NewString(), "-", "")
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

	migrator, err := migrate.New("file://"+readModelsMigrationsDir(t), testURL)
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

func migrateUpReadModels(t *testing.T, migrator *migrate.Migrate) {
	t.Helper()

	if err := migrator.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		t.Fatalf("migrate up: %v", err)
	}
}

func insertReadModelsConnectorConfig(t *testing.T, ctx context.Context, pool *pgxpool.Pool, kind string, enabled bool, cfg any) {
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

	store := configstore.NewStore(nil, qtx, []byte(readModelsTestConnectorSecretKey))
	if err := store.SaveConnectorConfigTx(ctx, qtx, kind, cfg); err != nil {
		t.Fatalf("SaveConnectorConfigTx(%s): %v", kind, err)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit connector config %s: %v", kind, err)
	}
}

func insertReadModelsSyncRun(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sourceKind, sourceName string, finishedAt time.Time) int64 {
	t.Helper()

	var id int64
	err := pool.QueryRow(ctx, `
		INSERT INTO sync_runs (source_kind, source_name, status, started_at, finished_at, message)
		VALUES ($1, $2, 'success', $3, $4, '')
		RETURNING id
	`, sourceKind, sourceName, finishedAt.Add(-time.Minute), finishedAt).Scan(&id)
	if err != nil {
		t.Fatalf("insert sync run %s/%s: %v", sourceKind, sourceName, err)
	}
	return id
}

func upsertReadModelsAppAsset(t *testing.T, ctx context.Context, q *gen.Queries, runID int64, sourceKind, sourceName, assetKind, externalID, displayName string) int64 {
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
		t.Fatalf("UpsertAppAssetsBulkBySource(): %v", err)
	}

	appAsset, err := q.GetAppAssetBySourceAndKindAndExternalID(ctx, gen.GetAppAssetBySourceAndKindAndExternalIDParams{
		SourceKind: sourceKind,
		SourceName: sourceName,
		AssetKind:  assetKind,
		ExternalID: externalID,
	})
	if err != nil {
		t.Fatalf("GetAppAssetBySourceAndKindAndExternalID(): %v", err)
	}
	return appAsset.ID
}

func fetchConnectorSourceStateTimes(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sourceKind, sourceName string) (time.Time, time.Time) {
	t.Helper()

	var lastSuccessAt, freshUntilAt time.Time
	if err := pool.QueryRow(ctx, `
		SELECT last_success_at, fresh_until_at
		FROM connector_source_state
		WHERE source_kind = $1
		  AND source_name = $2
	`, sourceKind, sourceName).Scan(&lastSuccessAt, &freshUntilAt); err != nil {
		t.Fatalf("select connector_source_state: %v", err)
	}
	return lastSuccessAt.UTC(), freshUntilAt.UTC()
}

func int64String(v int64) string {
	return strconv.FormatInt(v, 10)
}

func readModelsMigrationsDir(t *testing.T) string {
	t.Helper()

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "db", "migrations"))
}

func testDatabaseAdminURL(baseURL string) (string, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}
	parsed.Path = "/postgres"
	return parsed.String(), nil
}

func testDatabaseURLWithName(baseURL, dbName string) (string, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}
	parsed.Path = "/" + dbName
	return parsed.String(), nil
}
