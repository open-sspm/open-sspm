package gen

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestNonHumanAccessReadModelsAndMetrics(t *testing.T) {
	t.Parallel()

	withEntityCategoryTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *Queries, migrator *migrate.Migrate) {
		migrateUp(t, migrator)

		now := time.Now().UTC().Truncate(time.Second)
		ownerID := insertIdentity(t, ctx, pool, "human", "owner@example.com", "Owner Example")

		entraRunID := insertSyncRun(t, ctx, pool, "entra", "tenant-1")
		githubRunID := insertSyncRun(t, ctx, pool, "github", "acme")

		insertIdentitySourceSetting(t, ctx, pool, "entra", "tenant-1", true)
		upsertConfiguredSourceState(t, ctx, pool, "entra", "tenant-1", now.Add(-4*time.Hour), now.Add(-90*time.Minute))
		upsertConfiguredSourceState(t, ctx, pool, "github", "acme", now.Add(-30*time.Minute), now.Add(2*time.Hour))

		serviceAccountID := insertAccount(t, ctx, pool, entraRunID, accountSeed{
			SourceKind:     "entra",
			SourceName:     "tenant-1",
			ExternalID:     "sp:svc-123",
			Email:          "service.principal@example.com",
			DisplayName:    "Azure Service Principal",
			Status:         "active",
			AccountKind:    "service",
			EntityCategory: "service_principal",
			RawJSON:        `{"status":"active"}`,
		})
		serviceIdentityID := insertIdentity(t, ctx, pool, "service", "service.principal@example.com", "Azure Service Principal")
		insertIdentityAccountLink(t, ctx, pool, serviceIdentityID, serviceAccountID)

		serviceAssetID := insertAppAsset(t, ctx, pool, entraRunID, appAssetSeed{
			SourceKind:  "entra",
			SourceName:  "tenant-1",
			AssetKind:   "entra_service_principal",
			ExternalID:  "svc-123",
			DisplayName: "Azure Service Principal",
		})
		if _, err := q.UpsertAppAssetGovernance(ctx, UpsertAppAssetGovernanceParams{
			AppAssetID:      serviceAssetID,
			GovernanceState: "approved",
			OwnerIdentityID: pgtype.Int8{Int64: ownerID, Valid: true},
		}); err != nil {
			t.Fatalf("UpsertAppAssetGovernance(service): %v", err)
		}
		insertCredentialArtifact(t, ctx, pool, entraRunID, credentialArtifactSeed{
			SourceKind:          "entra",
			SourceName:          "tenant-1",
			AssetRefKind:        "app_asset",
			AssetRefExternalID:  "entra_service_principal:svc-123",
			CredentialKind:      "entra_client_secret",
			ExternalID:          "secret-owned",
			DisplayName:         "Owned Secret",
			Status:              "active",
			ExpiresAtSource:     now.Add(-24 * time.Hour),
			LastUsedAtSource:    now.Add(-120 * 24 * time.Hour),
			CreatedByExternalID: "owner@example.com",
		})

		githubAssetID := insertAppAsset(t, ctx, pool, githubRunID, appAssetSeed{
			SourceKind:  "github",
			SourceName:  "acme",
			AssetKind:   "github_app",
			ExternalID:  "github-actions",
			DisplayName: "GitHub Actions",
		})
		insertCredentialArtifact(t, ctx, pool, githubRunID, credentialArtifactSeed{
			SourceKind:         "github",
			SourceName:         "acme",
			AssetRefKind:       "app_asset",
			AssetRefExternalID: "github_app:github-actions",
			CredentialKind:     "github_pat_fine_grained",
			ExternalID:         "pat-unowned",
			DisplayName:        "Unowned PAT",
			Status:             "active",
			LastUsedAtSource:   now.Add(-120 * 24 * time.Hour),
		})

		if _, err := q.RefreshAllAppAssetReadModels(ctx); err != nil {
			t.Fatalf("RefreshAllAppAssetReadModels(): %v", err)
		}
		if _, err := q.RefreshAllNonHumanPrincipalReadModels(ctx); err != nil {
			t.Fatalf("RefreshAllNonHumanPrincipalReadModels(): %v", err)
		}

		servicePrincipal, err := q.GetNonHumanPrincipalByRef(ctx, "identity-"+int64String(serviceIdentityID))
		if err != nil {
			t.Fatalf("GetNonHumanPrincipalByRef(service): %v", err)
		}
		if servicePrincipal.FreshnessState != "stale" || !servicePrincipal.HasStaleEvidence {
			t.Fatalf("service freshness = %q stale=%v", servicePrincipal.FreshnessState, servicePrincipal.HasStaleEvidence)
		}
		if servicePrincipal.OwnerPresence != "owned" || servicePrincipal.GovernanceState != "approved" {
			t.Fatalf("service owner/governance = %q/%q", servicePrincipal.OwnerPresence, servicePrincipal.GovernanceState)
		}

		githubPrincipal, err := q.GetNonHumanPrincipalByRef(ctx, "app-asset-"+int64String(githubAssetID))
		if err != nil {
			t.Fatalf("GetNonHumanPrincipalByRef(github): %v", err)
		}
		if githubPrincipal.OwnerPresence != "unknown" {
			t.Fatalf("github owner_presence = %q want unknown", githubPrincipal.OwnerPresence)
		}
		if githubPrincipal.FreshnessState != "current" {
			t.Fatalf("github freshness = %q want current", githubPrincipal.FreshnessState)
		}

		staleRows, err := q.ListNonHumanPrincipalsPageByFilters(ctx, ListNonHumanPrincipalsPageByFiltersParams{
			FreshnessState:        "stale",
			ConfiguredSourceKinds: []string{"entra", "github"},
			ConfiguredSourceNames: []string{"tenant-1", "acme"},
			PageLimit:             10,
		})
		if err != nil {
			t.Fatalf("ListNonHumanPrincipalsPageByFilters(stale): %v", err)
		}
		if len(staleRows) != 1 || staleRows[0].PrincipalRef != "identity-"+int64String(serviceIdentityID) {
			t.Fatalf("stale rows = %#v", staleRows)
		}

		ownerCoverage, err := q.CountConfiguredNonHumanPrincipalOwnerCoverage(ctx)
		if err != nil {
			t.Fatalf("CountConfiguredNonHumanPrincipalOwnerCoverage(): %v", err)
		}
		if ownerCoverage.PrincipalCount != 2 || ownerCoverage.WithOwnerCount != 1 {
			t.Fatalf("owner coverage = %+v", ownerCoverage)
		}

		credentialCoverage, err := q.CountConfiguredNonHumanHighRiskCredentialAttribution(ctx)
		if err != nil {
			t.Fatalf("CountConfiguredNonHumanHighRiskCredentialAttribution(): %v", err)
		}
		if credentialCoverage.HighRiskCredentialCount != 2 || credentialCoverage.HighRiskWithAttributionCount != 1 {
			t.Fatalf("credential coverage = %+v", credentialCoverage)
		}

		adminUser := createNonHumanAccessAuthUser(t, ctx, q, "admin@example.com", "admin")
		viewerUser := createNonHumanAccessAuthUser(t, ctx, q, "viewer@example.com", "viewer")

		if err := q.InsertNonHumanAccessEvent(ctx, InsertNonHumanAccessEventParams{
			AuthUserID:   adminUser,
			AuthUserRole: "admin",
			EventKind:    "inventory_view",
			OccurredAt:   validTimestamptz(now.Add(-24 * time.Hour)),
		}); err != nil {
			t.Fatalf("InsertNonHumanAccessEvent(admin recent): %v", err)
		}
		if err := q.InsertNonHumanAccessEvent(ctx, InsertNonHumanAccessEventParams{
			AuthUserID:   adminUser,
			AuthUserRole: "admin",
			EventKind:    "detail_open",
			OccurredAt:   validTimestamptz(now.Add(-2 * time.Hour)),
		}); err != nil {
			t.Fatalf("InsertNonHumanAccessEvent(admin second event): %v", err)
		}
		if err := q.InsertNonHumanAccessEvent(ctx, InsertNonHumanAccessEventParams{
			AuthUserID:   viewerUser,
			AuthUserRole: "viewer",
			EventKind:    "inventory_view",
			OccurredAt:   validTimestamptz(now.Add(-2 * time.Hour)),
		}); err != nil {
			t.Fatalf("InsertNonHumanAccessEvent(viewer): %v", err)
		}
		if err := q.InsertNonHumanAccessEvent(ctx, InsertNonHumanAccessEventParams{
			AuthUserID:   adminUser,
			AuthUserRole: "admin",
			EventKind:    "inventory_view",
			OccurredAt:   validTimestamptz(now.Add(-9 * 24 * time.Hour)),
		}); err != nil {
			t.Fatalf("InsertNonHumanAccessEvent(admin old): %v", err)
		}

		weeklySessions, err := q.CountNonHumanAccessWeeklyAdminReviewSessions(ctx, validTimestamptz(now.Add(-7*24*time.Hour)))
		if err != nil {
			t.Fatalf("CountNonHumanAccessWeeklyAdminReviewSessions(): %v", err)
		}
		if weeklySessions != 1 {
			t.Fatalf("weekly sessions = %d want 1", weeklySessions)
		}
	})
}

func TestRefreshNonHumanPrincipalReadModelsBySourceKeepsUnrelatedRowsUntouched(t *testing.T) {
	t.Parallel()

	withEntityCategoryTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *Queries, migrator *migrate.Migrate) {
		migrateUp(t, migrator)

		now := time.Now().UTC().Truncate(time.Second)
		githubRunID := insertSyncRun(t, ctx, pool, "github", "acme")
		datadogRunID := insertSyncRun(t, ctx, pool, "datadog", "us5.datadoghq.com")

		upsertConfiguredSourceState(t, ctx, pool, "github", "acme", now.Add(-10*time.Minute), now.Add(time.Hour))
		upsertConfiguredSourceState(t, ctx, pool, "datadog", "us5.datadoghq.com", now.Add(-10*time.Minute), now.Add(time.Hour))

		githubAssetID := insertAppAsset(t, ctx, pool, githubRunID, appAssetSeed{
			SourceKind:  "github",
			SourceName:  "acme",
			AssetKind:   "github_app",
			ExternalID:  "github-actions",
			DisplayName: "GitHub Actions",
		})
		datadogAssetID := insertAppAsset(t, ctx, pool, datadogRunID, appAssetSeed{
			SourceKind:  "datadog",
			SourceName:  "us5.datadoghq.com",
			AssetKind:   "datadog_app_key",
			ExternalID:  "datadog-app-key",
			DisplayName: "Datadog App Key",
		})

		if _, err := q.RefreshAllAppAssetReadModels(ctx); err != nil {
			t.Fatalf("RefreshAllAppAssetReadModels(): %v", err)
		}
		if _, err := q.RefreshAllNonHumanPrincipalReadModels(ctx); err != nil {
			t.Fatalf("RefreshAllNonHumanPrincipalReadModels(): %v", err)
		}

		affectedRef := "app-asset-" + int64String(githubAssetID)
		unaffectedRef := "app-asset-" + int64String(datadogAssetID)
		affectedSentinel := now.Add(-48 * time.Hour)
		unaffectedSentinel := now.Add(-24 * time.Hour)

		if _, err := pool.Exec(ctx, `
			UPDATE non_human_principals
			SET projection_refreshed_at = CASE principal_ref
				WHEN $1 THEN $2
				WHEN $3 THEN $4
				ELSE projection_refreshed_at
			END
			WHERE principal_ref IN ($1, $3)
		`, affectedRef, affectedSentinel, unaffectedRef, unaffectedSentinel); err != nil {
			t.Fatalf("seed non_human_principals projection_refreshed_at: %v", err)
		}

		if _, err := q.RefreshNonHumanPrincipalReadModelsBySource(ctx, RefreshNonHumanPrincipalReadModelsBySourceParams{
			SourceKind: "github",
			SourceName: "acme",
		}); err != nil {
			t.Fatalf("RefreshNonHumanPrincipalReadModelsBySource(): %v", err)
		}

		var (
			affectedRefreshedAt   pgtype.Timestamptz
			unaffectedRefreshedAt pgtype.Timestamptz
		)
		if err := pool.QueryRow(ctx, `
			SELECT projection_refreshed_at
			FROM non_human_principals
			WHERE principal_ref = $1
		`, affectedRef).Scan(&affectedRefreshedAt); err != nil {
			t.Fatalf("select affected projection_refreshed_at: %v", err)
		}
		if err := pool.QueryRow(ctx, `
			SELECT projection_refreshed_at
			FROM non_human_principals
			WHERE principal_ref = $1
		`, unaffectedRef).Scan(&unaffectedRefreshedAt); err != nil {
			t.Fatalf("select unaffected projection_refreshed_at: %v", err)
		}

		if !affectedRefreshedAt.Valid || affectedRefreshedAt.Time.Equal(affectedSentinel) {
			t.Fatalf("affected projection_refreshed_at = %+v, want refreshed timestamp", affectedRefreshedAt)
		}
		if !unaffectedRefreshedAt.Valid || !unaffectedRefreshedAt.Time.Equal(unaffectedSentinel) {
			t.Fatalf("unaffected projection_refreshed_at = %+v, want %v", unaffectedRefreshedAt, unaffectedSentinel)
		}
	})
}

func upsertConfiguredSourceState(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sourceKind, sourceName string, lastSuccessAt, freshUntilAt time.Time) {
	t.Helper()

	if _, err := pool.Exec(ctx, `
		INSERT INTO connector_source_state (
			source_kind,
			source_name,
			enabled,
			configured,
			discovery_enabled,
			last_success_at,
			fresh_until_at,
			updated_at
		)
		VALUES ($1, $2, true, true, true, $3, $4, now())
		ON CONFLICT (source_kind, source_name) DO UPDATE SET
			enabled = EXCLUDED.enabled,
			configured = EXCLUDED.configured,
			discovery_enabled = EXCLUDED.discovery_enabled,
			last_success_at = EXCLUDED.last_success_at,
			fresh_until_at = EXCLUDED.fresh_until_at,
			updated_at = now()
	`, sourceKind, sourceName, lastSuccessAt.UTC(), freshUntilAt.UTC()); err != nil {
		t.Fatalf("upsert connector_source_state %s/%s: %v", sourceKind, sourceName, err)
	}
}

func createNonHumanAccessAuthUser(t *testing.T, ctx context.Context, q *Queries, email, role string) int64 {
	t.Helper()

	user, err := q.CreateAuthUser(ctx, CreateAuthUserParams{
		Email:        email,
		PasswordHash: "test-password-hash",
		Role:         role,
		IsActive:     true,
	})
	if err != nil {
		t.Fatalf("CreateAuthUser(%q): %v", email, err)
	}
	return user.ID
}

func int64String(value int64) string {
	return strconv.FormatInt(value, 10)
}
