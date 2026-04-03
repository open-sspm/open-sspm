package gen

import (
	"context"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestListAppAssetsPageBySourcesAndQueryAndKindPaginatesGlobally(t *testing.T) {
	t.Parallel()

	withEntityCategoryTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *Queries, migrator *migrate.Migrate) {
		migrateUp(t, migrator)

		githubRunID := insertSyncRun(t, ctx, pool, "github", "acme")
		vaultRunID := insertSyncRun(t, ctx, pool, "vault", "prod")
		excludedRunID := insertSyncRun(t, ctx, pool, "entra", "tenant-1")

		insertAppAsset(t, ctx, pool, githubRunID, appAssetSeed{
			SourceKind:  "github",
			SourceName:  "acme",
			AssetKind:   "repository",
			ExternalID:  "repo-alpha",
			DisplayName: "Alpha",
		})
		insertAppAsset(t, ctx, pool, vaultRunID, appAssetSeed{
			SourceKind:  "vault",
			SourceName:  "prod",
			AssetKind:   "secret_engine",
			ExternalID:  "engine-alpha",
			DisplayName: "Alpha",
		})
		insertAppAsset(t, ctx, pool, githubRunID, appAssetSeed{
			SourceKind:  "github",
			SourceName:  "acme",
			AssetKind:   "repository",
			ExternalID:  "repo-beta",
			DisplayName: "Beta",
		})
		insertAppAsset(t, ctx, pool, excludedRunID, appAssetSeed{
			SourceKind:  "entra",
			SourceName:  "tenant-1",
			AssetKind:   "service_principal",
			ExternalID:  "sp-zeta",
			DisplayName: "Aardvark",
		})

		params := CountAppAssetsBySourcesAndQueryAndKindParams{
			ConfiguredSourceKinds: []string{"github", "vault"},
			ConfiguredSourceNames: []string{"acme", "prod"},
		}
		count, err := q.CountAppAssetsBySourcesAndQueryAndKind(ctx, params)
		if err != nil {
			t.Fatalf("CountAppAssetsBySourcesAndQueryAndKind(): %v", err)
		}
		if count != 3 {
			t.Fatalf("CountAppAssetsBySourcesAndQueryAndKind()=%d want 3", count)
		}

		firstPage, err := q.ListAppAssetsPageBySourcesAndQueryAndKind(ctx, ListAppAssetsPageBySourcesAndQueryAndKindParams{
			ConfiguredSourceKinds: params.ConfiguredSourceKinds,
			ConfiguredSourceNames: params.ConfiguredSourceNames,
			PageLimit:             2,
		})
		if err != nil {
			t.Fatalf("ListAppAssetsPageBySourcesAndQueryAndKind(first page): %v", err)
		}
		if len(firstPage) != 2 {
			t.Fatalf("first page len=%d want 2", len(firstPage))
		}
		if firstPage[0].SourceKind != "github" || firstPage[0].ExternalID != "repo-alpha" {
			t.Fatalf("first page[0]=%s/%s want github/repo-alpha", firstPage[0].SourceKind, firstPage[0].ExternalID)
		}
		if firstPage[1].SourceKind != "vault" || firstPage[1].ExternalID != "engine-alpha" {
			t.Fatalf("first page[1]=%s/%s want vault/engine-alpha", firstPage[1].SourceKind, firstPage[1].ExternalID)
		}

		secondPage, err := q.ListAppAssetsPageBySourcesAndQueryAndKind(ctx, ListAppAssetsPageBySourcesAndQueryAndKindParams{
			ConfiguredSourceKinds: params.ConfiguredSourceKinds,
			ConfiguredSourceNames: params.ConfiguredSourceNames,
			PageLimit:             2,
			PageOffset:            2,
		})
		if err != nil {
			t.Fatalf("ListAppAssetsPageBySourcesAndQueryAndKind(second page): %v", err)
		}
		if len(secondPage) != 1 {
			t.Fatalf("second page len=%d want 1", len(secondPage))
		}
		if secondPage[0].SourceKind != "github" || secondPage[0].ExternalID != "repo-beta" {
			t.Fatalf("second page[0]=%s/%s want github/repo-beta", secondPage[0].SourceKind, secondPage[0].ExternalID)
		}
	})
}

func TestListCredentialArtifactsPageBySourcesAndQueryAndFiltersPaginatesGlobally(t *testing.T) {
	t.Parallel()

	withEntityCategoryTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *Queries, migrator *migrate.Migrate) {
		migrateUp(t, migrator)

		now := time.Now().UTC().Truncate(time.Second)
		githubRunID := insertSyncRun(t, ctx, pool, "github", "acme")
		vaultRunID := insertSyncRun(t, ctx, pool, "vault", "prod")
		excludedRunID := insertSyncRun(t, ctx, pool, "entra", "tenant-1")

		insertCredentialArtifact(t, ctx, pool, githubRunID, credentialArtifactSeed{
			SourceKind:         "github",
			SourceName:         "acme",
			AssetRefKind:       "repository",
			AssetRefExternalID: "repo-1",
			CredentialKind:     "github_pat_fine_grained",
			ExternalID:         "pat-early",
			DisplayName:        "Token Early",
			ExpiresAtSource:    now.Add(24 * time.Hour),
		})
		insertCredentialArtifact(t, ctx, pool, githubRunID, credentialArtifactSeed{
			SourceKind:         "github",
			SourceName:         "acme",
			AssetRefKind:       "repository",
			AssetRefExternalID: "repo-2",
			CredentialKind:     "github_pat_fine_grained",
			ExternalID:         "pat-alpha",
			DisplayName:        "Token Shared",
			ExpiresAtSource:    now.Add(48 * time.Hour),
		})
		insertCredentialArtifact(t, ctx, pool, vaultRunID, credentialArtifactSeed{
			SourceKind:         "vault",
			SourceName:         "prod",
			AssetRefKind:       "secret_engine",
			AssetRefExternalID: "engine-1",
			CredentialKind:     "vault_token",
			ExternalID:         "vault-alpha",
			DisplayName:        "Token Shared",
			ExpiresAtSource:    now.Add(48 * time.Hour),
		})
		insertCredentialArtifact(t, ctx, pool, excludedRunID, credentialArtifactSeed{
			SourceKind:         "entra",
			SourceName:         "tenant-1",
			AssetRefKind:       "application",
			AssetRefExternalID: "app-1",
			CredentialKind:     "entra_client_secret",
			ExternalID:         "secret-excluded",
			DisplayName:        "Token Excluded",
			ExpiresAtSource:    now.Add(12 * time.Hour),
		})

		params := CountCredentialArtifactsBySourcesAndQueryAndFiltersParams{
			ConfiguredSourceKinds: []string{"github", "vault"},
			ConfiguredSourceNames: []string{"acme", "prod"},
		}
		count, err := q.CountCredentialArtifactsBySourcesAndQueryAndFilters(ctx, params)
		if err != nil {
			t.Fatalf("CountCredentialArtifactsBySourcesAndQueryAndFilters(): %v", err)
		}
		if count != 3 {
			t.Fatalf("CountCredentialArtifactsBySourcesAndQueryAndFilters()=%d want 3", count)
		}

		firstPage, err := q.ListCredentialArtifactsPageBySourcesAndQueryAndFilters(ctx, ListCredentialArtifactsPageBySourcesAndQueryAndFiltersParams{
			ConfiguredSourceKinds: params.ConfiguredSourceKinds,
			ConfiguredSourceNames: params.ConfiguredSourceNames,
			PageLimit:             2,
		})
		if err != nil {
			t.Fatalf("ListCredentialArtifactsPageBySourcesAndQueryAndFilters(first page): %v", err)
		}
		if len(firstPage) != 2 {
			t.Fatalf("first page len=%d want 2", len(firstPage))
		}
		if firstPage[0].SourceKind != "github" || firstPage[0].ExternalID != "pat-early" {
			t.Fatalf("first page[0]=%s/%s want github/pat-early", firstPage[0].SourceKind, firstPage[0].ExternalID)
		}
		if firstPage[1].SourceKind != "github" || firstPage[1].ExternalID != "pat-alpha" {
			t.Fatalf("first page[1]=%s/%s want github/pat-alpha", firstPage[1].SourceKind, firstPage[1].ExternalID)
		}

		secondPage, err := q.ListCredentialArtifactsPageBySourcesAndQueryAndFilters(ctx, ListCredentialArtifactsPageBySourcesAndQueryAndFiltersParams{
			ConfiguredSourceKinds: params.ConfiguredSourceKinds,
			ConfiguredSourceNames: params.ConfiguredSourceNames,
			PageLimit:             2,
			PageOffset:            2,
		})
		if err != nil {
			t.Fatalf("ListCredentialArtifactsPageBySourcesAndQueryAndFilters(second page): %v", err)
		}
		if len(secondPage) != 1 {
			t.Fatalf("second page len=%d want 1", len(secondPage))
		}
		if secondPage[0].SourceKind != "vault" || secondPage[0].ExternalID != "vault-alpha" {
			t.Fatalf("second page[0]=%s/%s want vault/vault-alpha", secondPage[0].SourceKind, secondPage[0].ExternalID)
		}
	})
}

type appAssetSeed struct {
	SourceKind  string
	SourceName  string
	AssetKind   string
	ExternalID  string
	DisplayName string
}

func insertAppAsset(t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID int64, seed appAssetSeed) int64 {
	t.Helper()

	var id int64
	err := pool.QueryRow(ctx, `
		INSERT INTO app_assets (
			source_kind,
			source_name,
			asset_kind,
			external_id,
			display_name,
			raw_json,
			seen_in_run_id,
			seen_at,
			last_observed_run_id,
			last_observed_at,
			updated_at
		)
		VALUES ($1, $2, $3, $4, $5, '{}'::jsonb, $6, now(), $6, now(), now())
		RETURNING id
	`, seed.SourceKind, seed.SourceName, seed.AssetKind, seed.ExternalID, seed.DisplayName, runID).Scan(&id)
	if err != nil {
		t.Fatalf("insert app asset %s/%s: %v", seed.SourceKind, seed.ExternalID, err)
	}
	return id
}

type credentialArtifactSeed struct {
	SourceKind         string
	SourceName         string
	AssetRefKind       string
	AssetRefExternalID string
	CredentialKind     string
	ExternalID         string
	DisplayName        string
	ExpiresAtSource    time.Time
}

func insertCredentialArtifact(t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID int64, seed credentialArtifactSeed) int64 {
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
			seen_in_run_id,
			seen_at,
			last_observed_run_id,
			last_observed_at,
			expires_at_source,
			updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, '{}'::jsonb, '{}'::jsonb, $8, now(), $8, now(), $9, now())
		RETURNING id
	`, seed.SourceKind, seed.SourceName, seed.AssetRefKind, seed.AssetRefExternalID, seed.CredentialKind, seed.ExternalID, seed.DisplayName, runID, seed.ExpiresAtSource).Scan(&id)
	if err != nil {
		t.Fatalf("insert credential artifact %s/%s: %v", seed.SourceKind, seed.ExternalID, err)
	}
	return id
}
