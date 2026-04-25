package readmodels

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/testdb"
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

func TestProjectorRefreshSaaSAppRiskReadModelsBySourceEvaluatesAndStoresPolicy(t *testing.T) {
	t.Parallel()

	withReadModelsTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, migrator *migrate.Migrate) {
		migrateUpReadModels(t, migrator)

		sourceKind := "google_workspace"
		sourceName := "C0123"
		now := time.Now().UTC().Truncate(time.Second)
		runID := insertReadModelsSyncRun(t, ctx, pool, sourceKind, sourceName, now)
		otherRunID := insertReadModelsSyncRun(t, ctx, pool, "okta", "acme.okta.com", now)

		appID := upsertReadModelsSaaSApp(t, ctx, pool, q, "projector-risk-app", "Projector Risk App", "projector.example.com", "Example", now)
		otherAppID := upsertReadModelsSaaSApp(t, ctx, pool, q, "other-risk-app", "Other Risk App", "other.example.com", "Example", now)
		upsertReadModelsSaaSAppSource(t, ctx, q, runID, sourceKind, sourceName, "projector-risk-app", "projector-risk-app", "Projector Risk App", "projector.example.com", now)
		upsertReadModelsSaaSAppSource(t, ctx, q, otherRunID, "okta", "acme.okta.com", "other-risk-app", "other-risk-app", "Other Risk App", "other.example.com", now)
		upsertReadModelsPrivilegedSaaSAppEvent(t, ctx, q, runID, sourceKind, sourceName, "projector-risk-app", "projector-risk-app", "Projector Risk App", "projector.example.com", now)

		projector := NewProjector(pool, nil, RefreshConfig{})
		if err := projector.RefreshDiscoverySource(ctx, sourceKind, sourceName); err != nil {
			t.Fatalf("RefreshDiscoverySource(): %v", err)
		}
		if err := projector.RefreshSaaSAppRiskReadModelsBySource(ctx, sourceKind, sourceName); err != nil {
			t.Fatalf("RefreshSaaSAppRiskReadModelsBySource(): %v", err)
		}

		row, err := q.GetSaaSAppByID(ctx, appID)
		if err != nil {
			t.Fatalf("GetSaaSAppByID(): %v", err)
		}
		if row.RiskScore != 95 {
			t.Fatalf("risk_score = %d, want 95", row.RiskScore)
		}
		if row.RiskLevel != "critical" {
			t.Fatalf("risk_level = %q, want critical", row.RiskLevel)
		}
		if row.SuggestedBusinessCriticality != "high" {
			t.Fatalf("suggested_business_criticality = %q, want high", row.SuggestedBusinessCriticality)
		}
		if row.SuggestedDataClassification != "restricted" {
			t.Fatalf("suggested_data_classification = %q, want restricted", row.SuggestedDataClassification)
		}

		var effectiveBusinessCriticality, effectiveDataClassification string
		var policyPackCount int
		if err := pool.QueryRow(ctx, `
			SELECT
				effective_business_criticality,
				effective_data_classification,
				jsonb_array_length(policy_packs_json)
			FROM saas_app_risk_read_models
			WHERE saas_app_id = $1
		`, appID).Scan(&effectiveBusinessCriticality, &effectiveDataClassification, &policyPackCount); err != nil {
			t.Fatalf("select saas_app_risk_read_models: %v", err)
		}
		if effectiveBusinessCriticality != "high" {
			t.Fatalf("effective_business_criticality = %q, want high", effectiveBusinessCriticality)
		}
		if effectiveDataClassification != "restricted" {
			t.Fatalf("effective_data_classification = %q, want restricted", effectiveDataClassification)
		}
		if policyPackCount == 0 {
			t.Fatalf("policy_packs_json length = 0, want policy metadata")
		}

		var otherRiskRows int
		if err := pool.QueryRow(ctx, `
			SELECT count(*)
			FROM saas_app_risk_read_models
			WHERE saas_app_id = $1
		`, otherAppID).Scan(&otherRiskRows); err != nil {
			t.Fatalf("select other saas_app_risk_read_models count: %v", err)
		}
		if otherRiskRows != 0 {
			t.Fatalf("other source risk rows = %d, want 0", otherRiskRows)
		}
	})
}

func withReadModelsTestDatabase(t *testing.T, fn func(context.Context, *pgxpool.Pool, *gen.Queries, *migrate.Migrate)) {
	t.Helper()

	testdb.WithDatabase(t, testdb.Options{NamePrefix: "opensspm_readmodels"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		fn(ctx, pool, gen.New(pool), migrator)
	})
}

func migrateUpReadModels(t *testing.T, migrator *migrate.Migrate) {
	t.Helper()

	testdb.MigrateUp(t, migrator)
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

func upsertReadModelsSaaSApp(t *testing.T, ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, canonicalKey, displayName, primaryDomain, vendorName string, observedAt time.Time) int64 {
	t.Helper()

	observed := pgtype.Timestamptz{Time: observedAt.UTC(), Valid: true}
	if _, err := q.UpsertSaaSAppsBulk(ctx, gen.UpsertSaaSAppsBulkParams{
		CanonicalKeys:  []string{canonicalKey},
		DisplayNames:   []string{displayName},
		PrimaryDomains: []string{primaryDomain},
		VendorNames:    []string{vendorName},
		FirstSeenAts:   []pgtype.Timestamptz{observed},
		LastSeenAts:    []pgtype.Timestamptz{observed},
	}); err != nil {
		t.Fatalf("UpsertSaaSAppsBulk(%s): %v", canonicalKey, err)
	}

	var id int64
	if err := pool.QueryRow(ctx, `
		SELECT id
		FROM saas_apps
		WHERE canonical_key = $1
	`, canonicalKey).Scan(&id); err != nil {
		t.Fatalf("select saas app %s: %v", canonicalKey, err)
	}
	return id
}

func upsertReadModelsSaaSAppSource(t *testing.T, ctx context.Context, q *gen.Queries, runID int64, sourceKind, sourceName, canonicalKey, sourceAppID, sourceAppName, sourceAppDomain string, observedAt time.Time) {
	t.Helper()

	observed := pgtype.Timestamptz{Time: observedAt.UTC(), Valid: true}
	if _, err := q.UpsertSaaSAppSourcesBulkBySource(ctx, gen.UpsertSaaSAppSourcesBulkBySourceParams{
		SeenInRunID:      runID,
		SourceKind:       sourceKind,
		SourceName:       sourceName,
		CanonicalKeys:    []string{canonicalKey},
		SourceAppIds:     []string{sourceAppID},
		SourceAppNames:   []string{sourceAppName},
		SourceAppDomains: []string{sourceAppDomain},
		SeenAts:          []pgtype.Timestamptz{observed},
	}); err != nil {
		t.Fatalf("UpsertSaaSAppSourcesBulkBySource(%s): %v", canonicalKey, err)
	}
	if _, err := q.PromoteSaaSAppSourcesSeenInRunBySource(ctx, gen.PromoteSaaSAppSourcesSeenInRunBySourceParams{
		LastObservedRunID: runID,
		SourceKind:        sourceKind,
		SourceName:        sourceName,
	}); err != nil {
		t.Fatalf("PromoteSaaSAppSourcesSeenInRunBySource(%s): %v", canonicalKey, err)
	}
}

func upsertReadModelsPrivilegedSaaSAppEvent(t *testing.T, ctx context.Context, q *gen.Queries, runID int64, sourceKind, sourceName, canonicalKey, sourceAppID, sourceAppName, sourceAppDomain string, observedAt time.Time) {
	t.Helper()

	observed := pgtype.Timestamptz{Time: observedAt.UTC(), Valid: true}
	if _, err := q.UpsertSaaSAppEventsBulkBySource(ctx, gen.UpsertSaaSAppEventsBulkBySourceParams{
		SeenInRunID:       runID,
		SourceKind:        sourceKind,
		SourceName:        sourceName,
		CanonicalKeys:     []string{canonicalKey},
		SignalKinds:       []string{"oauth_grant"},
		EventExternalIds:  []string{sourceAppID + ":grant"},
		SourceAppIds:      []string{sourceAppID},
		SourceAppNames:    []string{sourceAppName},
		SourceAppDomains:  []string{sourceAppDomain},
		ActorExternalIds:  []string{"actor-1"},
		ActorEmails:       []string{"actor@example.com"},
		ActorDisplayNames: []string{"Actor Example"},
		ObservedAts:       []pgtype.Timestamptz{observed},
		ScopesJsons:       [][]byte{[]byte(`["files.readwrite.all"]`)},
		RawJsons:          [][]byte{[]byte(`{}`)},
	}); err != nil {
		t.Fatalf("UpsertSaaSAppEventsBulkBySource(%s): %v", canonicalKey, err)
	}
	if _, err := q.PromoteSaaSAppEventsSeenInRunBySource(ctx, gen.PromoteSaaSAppEventsSeenInRunBySourceParams{
		LastObservedRunID: runID,
		SourceKind:        sourceKind,
		SourceName:        sourceName,
	}); err != nil {
		t.Fatalf("PromoteSaaSAppEventsSeenInRunBySource(%s): %v", canonicalKey, err)
	}
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
