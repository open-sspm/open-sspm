package gen

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgtype"
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
			EvaluatedAt:           validTimestamptz(now),
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
			EvaluatedAt:           params.EvaluatedAt,
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
			EvaluatedAt:           params.EvaluatedAt,
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

func TestCredentialArtifactRiskLevelConsistentAcrossQueries(t *testing.T) {
	t.Parallel()

	withEntityCategoryTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *Queries, migrator *migrate.Migrate) {
		migrateUp(t, migrator)

		evaluatedAtTime := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
		evaluatedAt := validTimestamptz(evaluatedAtTime)

		cases := []struct {
			name string
			seed credentialArtifactSeed
			want string
		}{
			{
				name: "critical when expired and active",
				seed: credentialArtifactSeed{
					CredentialKind:      "entra_certificate",
					Status:              "active",
					CreatedByExternalID: "owner@example.com",
					ExpiresAtSource:     evaluatedAtTime.Add(-1 * time.Hour),
				},
				want: "critical",
			},
			{
				name: "critical when expired and status is blank",
				seed: credentialArtifactSeed{
					CredentialKind:      "entra_certificate",
					CreatedByExternalID: "owner@example.com",
					ExpiresAtSource:     evaluatedAtTime.Add(-1 * time.Hour),
				},
				want: "critical",
			},
			{
				name: "high when expired and inactive",
				seed: credentialArtifactSeed{
					CredentialKind:      "entra_certificate",
					Status:              "revoked",
					CreatedByExternalID: "owner@example.com",
					ExpiresAtSource:     evaluatedAtTime.Add(-1 * time.Hour),
				},
				want: "high",
			},
			{
				name: "critical when high privilege has no attribution",
				seed: credentialArtifactSeed{
					CredentialKind: "github_pat_fine_grained",
					Status:         "active",
				},
				want: "critical",
			},
			{
				name: "high when expiring within seven days",
				seed: credentialArtifactSeed{
					CredentialKind:      "entra_certificate",
					Status:              "active",
					CreatedByExternalID: "owner@example.com",
					ExpiresAtSource:     evaluatedAtTime.Add(3 * 24 * time.Hour),
				},
				want: "high",
			},
			{
				name: "high when creator attribution is missing",
				seed: credentialArtifactSeed{
					CredentialKind:       "vault_token",
					Status:               "active",
					ApprovedByExternalID: "approver@example.com",
					ExpiresAtSource:      evaluatedAtTime.Add(45 * 24 * time.Hour),
				},
				want: "high",
			},
			{
				name: "high when last used is stale",
				seed: credentialArtifactSeed{
					CredentialKind:      "vault_token",
					Status:              "active",
					CreatedByExternalID: "owner@example.com",
					ExpiresAtSource:     evaluatedAtTime.Add(45 * 24 * time.Hour),
					LastUsedAtSource:    evaluatedAtTime.Add(-120 * 24 * time.Hour),
				},
				want: "high",
			},
			{
				name: "medium when expiring within thirty days",
				seed: credentialArtifactSeed{
					CredentialKind:      "entra_certificate",
					Status:              "active",
					CreatedByExternalID: "owner@example.com",
					ExpiresAtSource:     evaluatedAtTime.Add(20 * 24 * time.Hour),
				},
				want: "medium",
			},
			{
				name: "low when healthy",
				seed: credentialArtifactSeed{
					CredentialKind:       "entra_certificate",
					Status:               "active",
					CreatedByExternalID:  "owner@example.com",
					ApprovedByExternalID: "approver@example.com",
					ExpiresAtSource:      evaluatedAtTime.Add(60 * 24 * time.Hour),
					LastUsedAtSource:     evaluatedAtTime.Add(-10 * 24 * time.Hour),
				},
				want: "low",
			},
		}

		for i, tc := range cases {
			sourceName := fmt.Sprintf("risk-source-%d", i)
			runID := insertSyncRun(t, ctx, pool, "github", sourceName)
			credentialID := insertCredentialArtifact(t, ctx, pool, runID, credentialArtifactSeed{
				SourceKind:           "github",
				SourceName:           sourceName,
				AssetRefKind:         "repository",
				AssetRefExternalID:   fmt.Sprintf("repo-%d", i),
				ExternalID:           fmt.Sprintf("credential-%d", i),
				DisplayName:          tc.name,
				CredentialKind:       tc.seed.CredentialKind,
				Status:               tc.seed.Status,
				ExpiresAtSource:      tc.seed.ExpiresAtSource,
				LastUsedAtSource:     tc.seed.LastUsedAtSource,
				CreatedByExternalID:  tc.seed.CreatedByExternalID,
				ApprovedByExternalID: tc.seed.ApprovedByExternalID,
			})

			gotByID, err := q.GetCredentialArtifactByID(ctx, GetCredentialArtifactByIDParams{
				EvaluatedAt: evaluatedAt,
				ID:          credentialID,
			})
			if err != nil {
				t.Fatalf("%s: GetCredentialArtifactByID(): %v", tc.name, err)
			}
			if gotByID.RiskLevel != tc.want {
				t.Fatalf("%s: GetCredentialArtifactByID() risk=%q want %q", tc.name, gotByID.RiskLevel, tc.want)
			}

			assetRows, err := q.ListCredentialArtifactsForAssetRef(ctx, ListCredentialArtifactsForAssetRefParams{
				EvaluatedAt:        evaluatedAt,
				SourceKind:         "github",
				SourceName:         sourceName,
				AssetRefKind:       "repository",
				AssetRefExternalID: fmt.Sprintf("repo-%d", i),
			})
			if err != nil {
				t.Fatalf("%s: ListCredentialArtifactsForAssetRef(): %v", tc.name, err)
			}
			if len(assetRows) != 1 {
				t.Fatalf("%s: ListCredentialArtifactsForAssetRef() len=%d want 1", tc.name, len(assetRows))
			}
			if assetRows[0].RiskLevel != tc.want {
				t.Fatalf("%s: ListCredentialArtifactsForAssetRef() risk=%q want %q", tc.name, assetRows[0].RiskLevel, tc.want)
			}

			singleSourceCount, err := q.CountCredentialArtifactsBySourceAndQueryAndFilters(ctx, CountCredentialArtifactsBySourceAndQueryAndFiltersParams{
				EvaluatedAt: evaluatedAt,
				SourceKind:  "github",
				SourceName:  sourceName,
				RiskLevel:   tc.want,
			})
			if err != nil {
				t.Fatalf("%s: CountCredentialArtifactsBySourceAndQueryAndFilters(): %v", tc.name, err)
			}
			if singleSourceCount != 1 {
				t.Fatalf("%s: CountCredentialArtifactsBySourceAndQueryAndFilters()=%d want 1", tc.name, singleSourceCount)
			}

			singleSourcePage, err := q.ListCredentialArtifactsPageBySourceAndQueryAndFilters(ctx, ListCredentialArtifactsPageBySourceAndQueryAndFiltersParams{
				EvaluatedAt: evaluatedAt,
				SourceKind:  "github",
				SourceName:  sourceName,
				RiskLevel:   tc.want,
				PageLimit:   10,
			})
			if err != nil {
				t.Fatalf("%s: ListCredentialArtifactsPageBySourceAndQueryAndFilters(): %v", tc.name, err)
			}
			if len(singleSourcePage) != 1 {
				t.Fatalf("%s: ListCredentialArtifactsPageBySourceAndQueryAndFilters() len=%d want 1", tc.name, len(singleSourcePage))
			}
			if singleSourcePage[0].ID != credentialID || singleSourcePage[0].RiskLevel != tc.want {
				t.Fatalf("%s: single-source page returned id=%d risk=%q want id=%d risk=%q", tc.name, singleSourcePage[0].ID, singleSourcePage[0].RiskLevel, credentialID, tc.want)
			}

			configuredSourceCount, err := q.CountCredentialArtifactsBySourcesAndQueryAndFilters(ctx, CountCredentialArtifactsBySourcesAndQueryAndFiltersParams{
				EvaluatedAt:           evaluatedAt,
				ConfiguredSourceKinds: []string{"github"},
				ConfiguredSourceNames: []string{sourceName},
				RiskLevel:             tc.want,
			})
			if err != nil {
				t.Fatalf("%s: CountCredentialArtifactsBySourcesAndQueryAndFilters(): %v", tc.name, err)
			}
			if configuredSourceCount != 1 {
				t.Fatalf("%s: CountCredentialArtifactsBySourcesAndQueryAndFilters()=%d want 1", tc.name, configuredSourceCount)
			}

			configuredSourcePage, err := q.ListCredentialArtifactsPageBySourcesAndQueryAndFilters(ctx, ListCredentialArtifactsPageBySourcesAndQueryAndFiltersParams{
				EvaluatedAt:           evaluatedAt,
				ConfiguredSourceKinds: []string{"github"},
				ConfiguredSourceNames: []string{sourceName},
				RiskLevel:             tc.want,
				PageLimit:             10,
			})
			if err != nil {
				t.Fatalf("%s: ListCredentialArtifactsPageBySourcesAndQueryAndFilters(): %v", tc.name, err)
			}
			if len(configuredSourcePage) != 1 {
				t.Fatalf("%s: ListCredentialArtifactsPageBySourcesAndQueryAndFilters() len=%d want 1", tc.name, len(configuredSourcePage))
			}
			if configuredSourcePage[0].ID != credentialID || configuredSourcePage[0].RiskLevel != tc.want {
				t.Fatalf("%s: configured-sources page returned id=%d risk=%q want id=%d risk=%q", tc.name, configuredSourcePage[0].ID, configuredSourcePage[0].RiskLevel, credentialID, tc.want)
			}
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
	SourceKind           string
	SourceName           string
	AssetRefKind         string
	AssetRefExternalID   string
	CredentialKind       string
	ExternalID           string
	DisplayName          string
	Status               string
	ExpiresAtSource      time.Time
	LastUsedAtSource     time.Time
	CreatedByExternalID  string
	ApprovedByExternalID string
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
			status,
			seen_in_run_id,
			seen_at,
			last_observed_run_id,
			last_observed_at,
			expires_at_source,
			last_used_at_source,
			created_by_external_id,
			approved_by_external_id,
			updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, '{}'::jsonb, '{}'::jsonb, $8, $9, now(), $9, now(), $10, $11, $12, $13, now())
		RETURNING id
	`, seed.SourceKind, seed.SourceName, seed.AssetRefKind, seed.AssetRefExternalID, seed.CredentialKind, seed.ExternalID, seed.DisplayName, seed.Status, runID, nullableTime(seed.ExpiresAtSource), nullableTime(seed.LastUsedAtSource), seed.CreatedByExternalID, seed.ApprovedByExternalID).Scan(&id)
	if err != nil {
		t.Fatalf("insert credential artifact %s/%s: %v", seed.SourceKind, seed.ExternalID, err)
	}
	return id
}

func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UTC()
}

func validTimestamptz(value time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: value.UTC(), Valid: true}
}
