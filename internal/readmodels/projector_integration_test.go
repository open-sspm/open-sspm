package readmodels

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/riskpolicy"
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

		if err := projector.RefreshAllNonHumanPrincipalReadModels(ctx); err != nil {
			t.Fatalf("RefreshAllNonHumanPrincipalReadModels(): %v", err)
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

func TestProjectorRefreshAllNonHumanPrincipalReadModelsEvaluatesAndStoresPolicy(t *testing.T) {
	t.Parallel()

	withReadModelsTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, migrator *migrate.Migrate) {
		migrateUpReadModels(t, migrator)

		sourceKind := "entra"
		sourceName := "11111111-1111-1111-1111-111111111111"
		now := time.Now().UTC().Truncate(time.Second)
		runID := insertReadModelsSyncRun(t, ctx, pool, sourceKind, sourceName, now.Add(-4*time.Hour))
		upsertConfiguredSourceState(t, ctx, pool, sourceKind, sourceName, now.Add(-4*time.Hour), now.Add(-90*time.Minute))

		appAssetID := upsertReadModelsAppAsset(t, ctx, q, runID, sourceKind, sourceName, "entra_service_principal", "svc-policy", "Policy Service Principal")

		if _, err := q.RefreshAllAppAssetReadModels(ctx); err != nil {
			t.Fatalf("RefreshAllAppAssetReadModels(): %v", err)
		}

		projector := NewProjector(pool, nil, RefreshConfig{})
		if err := projector.RefreshAllNonHumanPrincipalReadModels(ctx); err != nil {
			t.Fatalf("RefreshAllNonHumanPrincipalReadModels(): %v", err)
		}

		principal, err := q.GetNonHumanPrincipalByRef(ctx, "app-asset-"+int64String(appAssetID))
		if err != nil {
			t.Fatalf("GetNonHumanPrincipalByRef(): %v", err)
		}
		expected, err := riskpolicy.EvaluateIdentity(riskpolicy.IdentityInput{
			IdentityID:             principal.IdentityID,
			PrincipalRef:           principal.PrincipalRef,
			PrincipalType:          principal.PrincipalType,
			SourceKind:             principal.SourceKind,
			SourceName:             principal.SourceName,
			DisplayName:            principal.DisplayName,
			PrimaryEmail:           principal.SecondaryName,
			LastSeenAt:             timestamptzPtr(principal.LastSeenAt),
			OwnerPresence:          principal.OwnerPresence,
			GovernanceState:        principal.GovernanceState,
			LinkedAssetsCount:      principal.LinkedAssetsCount,
			LinkedCredentialsCount: principal.LinkedCredentialsCount,
			HasCriticalCredential:  principal.HasCriticalCredential,
			HasHighRiskCredential:  principal.HasHighRiskCredential,
			HasExpiredCredential:   principal.HasExpiredCredential,
			HasExpiringCredential:  principal.HasExpiringCredential,
			HasUnusedCredential:    principal.HasUnusedCredential,
			HasStaleEvidence:       principal.HasStaleEvidence,
		})
		if err != nil {
			t.Fatalf("EvaluateIdentity(): %v", err)
		}
		if principal.RiskLevel != expected.RiskLevel {
			t.Fatalf("stored risk_level = %q, policy risk_level = %q", principal.RiskLevel, expected.RiskLevel)
		}
		if principal.RiskReasonCount != int32(expected.RiskReasonCount) {
			t.Fatalf("stored risk_reason_count = %d, policy risk_reason_count = %d", principal.RiskReasonCount, expected.RiskReasonCount)
		}
		if principal.RiskLevel != "high" || principal.RiskReasonCount != 3 {
			t.Fatalf("stored policy risk = %q/%d, want high/3", principal.RiskLevel, principal.RiskReasonCount)
		}

		var signals []riskpolicy.RiskSignal
		if err := json.Unmarshal(principal.RiskSignalsJson, &signals); err != nil {
			t.Fatalf("unmarshal risk_signals_json: %v", err)
		}
		if len(signals) != expected.RiskReasonCount {
			t.Fatalf("risk_signals_json length = %d, want %d", len(signals), expected.RiskReasonCount)
		}
		var packs []riskpolicy.PolicyPackRef
		if err := json.Unmarshal(principal.PolicyPacksJson, &packs); err != nil {
			t.Fatalf("unmarshal policy_packs_json: %v", err)
		}
		if len(packs) != len(expected.PolicyPacks) || packs[0].ID != expected.PolicyPacks[0].ID {
			t.Fatalf("policy_packs_json = %+v, want %+v", packs, expected.PolicyPacks)
		}
	})
}

func TestProjectorRefreshNonHumanPrincipalSourceReadModelsKeepsUnrelatedRowsUntouched(t *testing.T) {
	t.Parallel()

	withReadModelsTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, migrator *migrate.Migrate) {
		migrateUpReadModels(t, migrator)

		now := time.Now().UTC().Truncate(time.Second)
		githubRunID := insertReadModelsSyncRun(t, ctx, pool, "github", "acme", now)
		datadogRunID := insertReadModelsSyncRun(t, ctx, pool, "datadog", "us5.datadoghq.com", now)

		upsertConfiguredSourceState(t, ctx, pool, "github", "acme", now.Add(-10*time.Minute), now.Add(time.Hour))
		upsertConfiguredSourceState(t, ctx, pool, "datadog", "us5.datadoghq.com", now.Add(-10*time.Minute), now.Add(time.Hour))

		githubAssetID := upsertReadModelsAppAsset(t, ctx, q, githubRunID, "github", "acme", "github_app", "github-actions", "GitHub Actions")
		datadogAssetID := upsertReadModelsAppAsset(t, ctx, q, datadogRunID, "datadog", "us5.datadoghq.com", "datadog_app_key", "datadog-app-key", "Datadog App Key")

		if _, err := q.RefreshAllAppAssetReadModels(ctx); err != nil {
			t.Fatalf("RefreshAllAppAssetReadModels(): %v", err)
		}

		projector := NewProjector(pool, nil, RefreshConfig{})
		if err := projector.RefreshAllNonHumanPrincipalReadModels(ctx); err != nil {
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

		if err := projector.RefreshNonHumanPrincipalSourceReadModels(ctx, "github", "acme"); err != nil {
			t.Fatalf("RefreshNonHumanPrincipalSourceReadModels(): %v", err)
		}

		affectedRefreshedAt := fetchNonHumanProjectionRefreshedAt(t, ctx, pool, affectedRef)
		unaffectedRefreshedAt := fetchNonHumanProjectionRefreshedAt(t, ctx, pool, unaffectedRef)
		if !affectedRefreshedAt.Valid || affectedRefreshedAt.Time.Equal(affectedSentinel) {
			t.Fatalf("affected projection_refreshed_at = %+v, want refreshed timestamp", affectedRefreshedAt)
		}
		if !unaffectedRefreshedAt.Valid || !unaffectedRefreshedAt.Time.Equal(unaffectedSentinel) {
			t.Fatalf("unaffected projection_refreshed_at = %+v, want %v", unaffectedRefreshedAt, unaffectedSentinel)
		}

		affected, err := q.GetNonHumanPrincipalByRef(ctx, affectedRef)
		if err != nil {
			t.Fatalf("GetNonHumanPrincipalByRef(affected): %v", err)
		}
		if affected.RiskLevel != "high" || affected.RiskReasonCount != 1 {
			t.Fatalf("affected policy risk = %q/%d, want high/1", affected.RiskLevel, affected.RiskReasonCount)
		}
	})
}

func TestProjectorNonHumanPolicyRiskFeedsFiltersSortAndMetrics(t *testing.T) {
	t.Parallel()

	withReadModelsTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, migrator *migrate.Migrate) {
		migrateUpReadModels(t, migrator)

		now := time.Now().UTC().Truncate(time.Second)
		ownerID := insertReadModelsIdentity(t, ctx, pool, "human", "owner@example.com", "Owner Example")
		githubRunID := insertReadModelsSyncRun(t, ctx, pool, "github", "acme", now)
		entraRunID := insertReadModelsSyncRun(t, ctx, pool, "entra", "tenant-1", now)
		datadogRunID := insertReadModelsSyncRun(t, ctx, pool, "datadog", "rogue", now)

		upsertConfiguredSourceState(t, ctx, pool, "github", "acme", now.Add(-15*time.Minute), now.Add(time.Hour))
		upsertConfiguredSourceState(t, ctx, pool, "entra", "tenant-1", now.Add(-15*time.Minute), now.Add(time.Hour))

		criticalAssetID := upsertReadModelsAppAsset(t, ctx, q, githubRunID, "github", "acme", "github_app", "critical-app", "Critical GitHub App")
		highAssetID := upsertReadModelsAppAsset(t, ctx, q, githubRunID, "github", "acme", "github_app", "high-app", "High GitHub App")
		mediumAssetID := upsertReadModelsAppAsset(t, ctx, q, entraRunID, "entra", "tenant-1", "entra_service_principal", "medium-sp", "Medium Service Principal")
		lowAssetID := upsertReadModelsAppAsset(t, ctx, q, entraRunID, "entra", "tenant-1", "entra_service_principal", "low-sp", "Low Service Principal")
		rogueAssetID := upsertReadModelsAppAsset(t, ctx, q, datadogRunID, "datadog", "rogue", "datadog_app_key", "rogue-key", "Rogue Datadog Key")

		for _, appAssetID := range []int64{mediumAssetID, lowAssetID} {
			if _, err := q.UpsertAppAssetGovernance(ctx, gen.UpsertAppAssetGovernanceParams{
				AppAssetID:      appAssetID,
				GovernanceState: "approved",
				OwnerIdentityID: pgtype.Int8{Int64: ownerID, Valid: true},
			}); err != nil {
				t.Fatalf("UpsertAppAssetGovernance(%d): %v", appAssetID, err)
			}
		}

		criticalCredential := readModelsCredentialArtifactSeed{
			SourceKind:           "github",
			SourceName:           "acme",
			AssetRefKind:         "app_asset",
			AssetRefExternalID:   "github_app:critical-app",
			CredentialKind:       "generic_api_key",
			ExternalID:           "critical-expired",
			DisplayName:          "Critical Expired Credential",
			Status:               "active",
			ExpiresAtSource:      now.Add(-24 * time.Hour),
			CreatedByExternalID:  "owner@example.com",
			CreatedByDisplayName: "Owner Example",
		}
		highCredential := readModelsCredentialArtifactSeed{
			SourceKind:         "github",
			SourceName:         "acme",
			AssetRefKind:       "app_asset",
			AssetRefExternalID: "github_app:high-app",
			CredentialKind:     "generic_api_key",
			ExternalID:         "high-unattributed",
			DisplayName:        "High Unattributed Credential",
			Status:             "active",
			LastUsedAtSource:   now.Add(-120 * 24 * time.Hour),
		}
		mediumCredential := readModelsCredentialArtifactSeed{
			SourceKind:           "entra",
			SourceName:           "tenant-1",
			AssetRefKind:         "app_asset",
			AssetRefExternalID:   "entra_service_principal:medium-sp",
			CredentialKind:       "generic_api_key",
			ExternalID:           "medium-expiring",
			DisplayName:          "Medium Expiring Credential",
			Status:               "active",
			ExpiresAtSource:      now.Add(20 * 24 * time.Hour),
			CreatedByExternalID:  "owner@example.com",
			CreatedByDisplayName: "Owner Example",
		}
		rogueCredential := readModelsCredentialArtifactSeed{
			SourceKind:         "datadog",
			SourceName:         "rogue",
			AssetRefKind:       "app_asset",
			AssetRefExternalID: "datadog_app_key:rogue-key",
			CredentialKind:     "generic_api_key",
			ExternalID:         "rogue-expired",
			DisplayName:        "Rogue Expired Credential",
			Status:             "active",
			ExpiresAtSource:    now.Add(-24 * time.Hour),
		}
		for _, fixture := range []struct {
			runID int64
			seed  readModelsCredentialArtifactSeed
		}{
			{runID: githubRunID, seed: criticalCredential},
			{runID: githubRunID, seed: highCredential},
			{runID: entraRunID, seed: mediumCredential},
			{runID: datadogRunID, seed: rogueCredential},
		} {
			insertReadModelsCredentialArtifact(t, ctx, pool, fixture.runID, fixture.seed)
		}

		if _, err := q.RefreshAllAppAssetReadModels(ctx); err != nil {
			t.Fatalf("RefreshAllAppAssetReadModels(): %v", err)
		}

		projector := NewProjector(pool, nil, RefreshConfig{})
		if err := projector.RefreshAllNonHumanPrincipalReadModels(ctx); err != nil {
			t.Fatalf("RefreshAllNonHumanPrincipalReadModels(): %v", err)
		}

		configuredSourceKinds := []string{"github", "entra"}
		configuredSourceNames := []string{"acme", "tenant-1"}
		criticalRef := "app-asset-" + int64String(criticalAssetID)
		highRef := "app-asset-" + int64String(highAssetID)
		mediumRef := "app-asset-" + int64String(mediumAssetID)
		lowRef := "app-asset-" + int64String(lowAssetID)
		rogueRef := "app-asset-" + int64String(rogueAssetID)

		rows, err := q.ListNonHumanPrincipalsPageByFilters(ctx, gen.ListNonHumanPrincipalsPageByFiltersParams{
			ConfiguredSourceKinds: configuredSourceKinds,
			ConfiguredSourceNames: configuredSourceNames,
			PageLimit:             10,
		})
		if err != nil {
			t.Fatalf("ListNonHumanPrincipalsPageByFilters(all configured): %v", err)
		}
		rowRefs := principalRefsFromRows(rows)
		assertPrincipalRefs(t, rowRefs, []string{criticalRef, highRef, mediumRef, lowRef})
		assertNoPrincipalRef(t, rowRefs, rogueRef)

		highRows, err := q.ListNonHumanPrincipalsPageByFilters(ctx, gen.ListNonHumanPrincipalsPageByFiltersParams{
			ConfiguredSourceKinds: configuredSourceKinds,
			ConfiguredSourceNames: configuredSourceNames,
			RiskLevel:             "high",
			PageLimit:             10,
		})
		if err != nil {
			t.Fatalf("ListNonHumanPrincipalsPageByFilters(high): %v", err)
		}
		assertPrincipalRefs(t, principalRefsFromRows(highRows), []string{highRef})

		githubRows, err := q.ListNonHumanPrincipalsPageByFilters(ctx, gen.ListNonHumanPrincipalsPageByFiltersParams{
			ConfiguredSourceKinds: configuredSourceKinds,
			ConfiguredSourceNames: configuredSourceNames,
			SourceKind:            "github",
			PageLimit:             10,
		})
		if err != nil {
			t.Fatalf("ListNonHumanPrincipalsPageByFilters(github): %v", err)
		}
		assertPrincipalRefs(t, principalRefsFromRows(githubRows), []string{criticalRef, highRef})

		criticalCount, err := q.CountNonHumanPrincipalsByFilters(ctx, gen.CountNonHumanPrincipalsByFiltersParams{
			ConfiguredSourceKinds: configuredSourceKinds,
			ConfiguredSourceNames: configuredSourceNames,
			RiskLevel:             "critical",
		})
		if err != nil {
			t.Fatalf("CountNonHumanPrincipalsByFilters(critical): %v", err)
		}
		if criticalCount != 1 {
			t.Fatalf("critical count = %d, want 1", criticalCount)
		}

		ownerCoverage, err := q.CountConfiguredNonHumanPrincipalOwnerCoverage(ctx)
		if err != nil {
			t.Fatalf("CountConfiguredNonHumanPrincipalOwnerCoverage(): %v", err)
		}
		if ownerCoverage.PrincipalCount != 4 || ownerCoverage.WithOwnerCount != 2 {
			t.Fatalf("owner coverage = %+v, want 4 principals and 2 with owner", ownerCoverage)
		}

		wantHighRiskCredentials, wantHighRiskCredentialsWithAttribution := expectedHighRiskCredentialAttributionFromPolicy(t, now, []readModelsCredentialMetricCase{
			{seed: criticalCredential, hasAttribution: true},
			{seed: highCredential, hasAttribution: false},
			{seed: mediumCredential, hasAttribution: true},
		})
		credentialCoverage, err := q.CountConfiguredNonHumanHighRiskCredentialAttribution(ctx)
		if err != nil {
			t.Fatalf("CountConfiguredNonHumanHighRiskCredentialAttribution(): %v", err)
		}
		if credentialCoverage.HighRiskCredentialCount != wantHighRiskCredentials ||
			credentialCoverage.HighRiskWithAttributionCount != wantHighRiskCredentialsWithAttribution {
			t.Fatalf("credential coverage = %+v, want %d/%d", credentialCoverage, wantHighRiskCredentials, wantHighRiskCredentialsWithAttribution)
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

func insertReadModelsIdentity(t *testing.T, ctx context.Context, pool *pgxpool.Pool, kind, email, displayName string) int64 {
	t.Helper()

	var id int64
	err := pool.QueryRow(ctx, `
		INSERT INTO identities (kind, display_name, primary_email, created_at, updated_at)
		VALUES ($1, $2, $3, now(), now())
		RETURNING id
	`, kind, displayName, email).Scan(&id)
	if err != nil {
		t.Fatalf("insert identity %s: %v", email, err)
	}
	return id
}

type readModelsCredentialArtifactSeed struct {
	SourceKind            string
	SourceName            string
	AssetRefKind          string
	AssetRefExternalID    string
	CredentialKind        string
	ExternalID            string
	DisplayName           string
	Status                string
	ExpiresAtSource       time.Time
	LastUsedAtSource      time.Time
	CreatedByExternalID   string
	CreatedByDisplayName  string
	ApprovedByExternalID  string
	ApprovedByDisplayName string
}

func insertReadModelsCredentialArtifact(t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID int64, seed readModelsCredentialArtifactSeed) int64 {
	t.Helper()

	var id int64
	err := pool.QueryRow(ctx, `
		INSERT INTO credential_artifacts (
			source_kind,
			source_name,
			asset_ref_kind,
			asset_ref_external_id,
			credential_kind,
			external_id,
			display_name,
			scope_json,
			raw_json,
			status,
			seen_in_run_id,
			seen_at,
			last_observed_run_id,
			last_observed_at,
			expires_at_source,
			last_used_at_source,
			created_by_kind,
			created_by_external_id,
			created_by_display_name,
			approved_by_kind,
			approved_by_external_id,
			approved_by_display_name,
			updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, '{}'::jsonb, '{}'::jsonb, $8, $9, now(), $9, now(), $10, $11, 'user', $12, $13, 'user', $14, $15, now())
		RETURNING id
	`, seed.SourceKind, seed.SourceName, seed.AssetRefKind, seed.AssetRefExternalID, seed.CredentialKind, seed.ExternalID, seed.DisplayName, seed.Status, runID, readModelsNullableTime(seed.ExpiresAtSource), readModelsNullableTime(seed.LastUsedAtSource), seed.CreatedByExternalID, seed.CreatedByDisplayName, seed.ApprovedByExternalID, seed.ApprovedByDisplayName).Scan(&id)
	if err != nil {
		t.Fatalf("insert credential artifact %s/%s: %v", seed.SourceKind, seed.ExternalID, err)
	}
	return id
}

func readModelsNullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UTC()
}

type readModelsCredentialMetricCase struct {
	seed           readModelsCredentialArtifactSeed
	hasAttribution bool
}

func expectedHighRiskCredentialAttributionFromPolicy(t *testing.T, evaluatedAt time.Time, cases []readModelsCredentialMetricCase) (int64, int64) {
	t.Helper()

	var highRiskCount, highRiskWithAttributionCount int64
	for _, tc := range cases {
		result, err := riskpolicy.EvaluateCredential(readModelsCredentialPolicyInput(tc.seed, evaluatedAt))
		if err != nil {
			t.Fatalf("EvaluateCredential(%s): %v", tc.seed.ExternalID, err)
		}
		if result.RiskRank < riskpolicy.SeverityRank(riskpolicy.SeverityHigh) {
			continue
		}
		highRiskCount++
		if tc.hasAttribution {
			highRiskWithAttributionCount++
		}
	}
	return highRiskCount, highRiskWithAttributionCount
}

func readModelsCredentialPolicyInput(seed readModelsCredentialArtifactSeed, evaluatedAt time.Time) riskpolicy.CredentialInput {
	return riskpolicy.CredentialInput{
		SourceKind:            seed.SourceKind,
		SourceName:            seed.SourceName,
		CredentialKind:        seed.CredentialKind,
		Status:                seed.Status,
		ExpiresAt:             readModelsTimePtr(seed.ExpiresAtSource),
		LastUsedAt:            readModelsTimePtr(seed.LastUsedAtSource),
		CreatedByExternalID:   seed.CreatedByExternalID,
		CreatedByDisplayName:  seed.CreatedByDisplayName,
		ApprovedByExternalID:  seed.ApprovedByExternalID,
		ApprovedByDisplayName: seed.ApprovedByDisplayName,
		AssetRefKind:          seed.AssetRefKind,
		AssetRefExternalID:    seed.AssetRefExternalID,
		EvaluatedAt:           evaluatedAt,
	}
}

func readModelsTimePtr(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	normalized := value.UTC()
	return &normalized
}

func principalRefsFromRows(rows []gen.ListNonHumanPrincipalsPageByFiltersRow) []string {
	refs := make([]string, 0, len(rows))
	for _, row := range rows {
		refs = append(refs, row.PrincipalRef)
	}
	return refs
}

func assertPrincipalRefs(t *testing.T, got, want []string) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("principal refs = %#v, want %#v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("principal refs = %#v, want %#v", got, want)
		}
	}
}

func assertNoPrincipalRef(t *testing.T, got []string, unwanted string) {
	t.Helper()

	for _, ref := range got {
		if ref == unwanted {
			t.Fatalf("principal refs = %#v, did not expect %q", got, unwanted)
		}
	}
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

func fetchNonHumanProjectionRefreshedAt(t *testing.T, ctx context.Context, pool *pgxpool.Pool, principalRef string) pgtype.Timestamptz {
	t.Helper()

	var refreshedAt pgtype.Timestamptz
	if err := pool.QueryRow(ctx, `
		SELECT projection_refreshed_at
		FROM non_human_principals
		WHERE principal_ref = $1
	`, principalRef).Scan(&refreshedAt); err != nil {
		t.Fatalf("select non_human_principals projection_refreshed_at: %v", err)
	}
	return refreshedAt
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
