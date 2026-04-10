package gen

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5"
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

func TestGovernanceSubjectOverridesMigrationBackfillsAndDropsLegacySchema(t *testing.T) {
	t.Parallel()

	withEntityCategoryTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *Queries, migrator *migrate.Migrate) {
		migrateToVersion(t, migrator, 37)

		runID := insertSyncRun(t, ctx, pool, "google_workspace", "C0123")
		ownerID := insertIdentity(t, ctx, pool, "human", "owner@example.com", "Owner Example")
		appAssetID := insertAppAsset(t, ctx, pool, runID, appAssetSeed{
			SourceKind:  "google_workspace",
			SourceName:  "C0123",
			AssetKind:   "google_oauth_client",
			ExternalID:  "client-123.apps.googleusercontent.com",
			DisplayName: "OAuth Client",
		})
		saasAppID := insertSaaSApp(t, ctx, pool, "shadow-app", "Shadow App", "shadow.example.com", "Example", time.Now().UTC(), time.Now().UTC())

		if _, err := pool.Exec(ctx, `
			INSERT INTO connected_app_governance (app_asset_id, review_state, owner_identity_id, ticket_ref, notes, updated_at)
			VALUES ($1, 'needs_revocation', $2, 'SEC-123', 'Contain access', now())
		`, appAssetID, ownerID); err != nil {
			t.Fatalf("insert connected_app_governance: %v", err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO saas_app_governance_overrides (
				saas_app_id,
				owner_identity_id,
				business_criticality,
				data_classification,
				notes,
				updated_at
			)
			VALUES ($1, $2, 'high', 'confidential', 'Assigned for review', now())
		`, saasAppID, ownerID); err != nil {
			t.Fatalf("insert saas_app_governance_overrides: %v", err)
		}

		migrateUp(t, migrator)

		var appAssetState, appAssetTicket, appAssetNotes string
		var appAssetOwnerID int64
		if err := pool.QueryRow(ctx, `
			SELECT governance_state, owner_identity_id, ticket_ref, notes
			FROM governance_subject_overrides
			WHERE subject_kind = 'app_asset' AND subject_id = $1
		`, appAssetID).Scan(&appAssetState, &appAssetOwnerID, &appAssetTicket, &appAssetNotes); err != nil {
			t.Fatalf("select migrated app-asset override: %v", err)
		}
		if appAssetState != "action_required" {
			t.Fatalf("app-asset governance_state=%q want action_required", appAssetState)
		}
		if appAssetOwnerID != ownerID {
			t.Fatalf("app-asset owner_identity_id=%d want %d", appAssetOwnerID, ownerID)
		}
		if appAssetTicket != "SEC-123" || appAssetNotes != "Contain access" {
			t.Fatalf("app-asset ticket/notes=%q/%q want SEC-123/Contain access", appAssetTicket, appAssetNotes)
		}

		var saasState, saasBusinessCriticality, saasDataClassification, saasNotes string
		var saasOwnerID int64
		if err := pool.QueryRow(ctx, `
			SELECT governance_state, owner_identity_id, business_criticality, data_classification, notes
			FROM governance_subject_overrides
			WHERE subject_kind = 'saas_app' AND subject_id = $1
		`, saasAppID).Scan(&saasState, &saasOwnerID, &saasBusinessCriticality, &saasDataClassification, &saasNotes); err != nil {
			t.Fatalf("select migrated saas-app override: %v", err)
		}
		if saasState != "unreviewed" {
			t.Fatalf("saas-app governance_state=%q want unreviewed", saasState)
		}
		if saasOwnerID != ownerID {
			t.Fatalf("saas-app owner_identity_id=%d want %d", saasOwnerID, ownerID)
		}
		if saasBusinessCriticality != "high" || saasDataClassification != "confidential" || saasNotes != "Assigned for review" {
			t.Fatalf("saas-app override=%q/%q/%q want high/confidential/Assigned for review", saasBusinessCriticality, saasDataClassification, saasNotes)
		}

		var legacyTableCount int
		if err := pool.QueryRow(ctx, `
			SELECT count(*)
			FROM information_schema.tables
			WHERE table_schema = 'public'
			  AND table_name IN ('connected_app_governance', 'saas_app_governance_overrides')
		`).Scan(&legacyTableCount); err != nil {
			t.Fatalf("count legacy tables: %v", err)
		}
		if legacyTableCount != 0 {
			t.Fatalf("legacy governance tables still present: %d", legacyTableCount)
		}

		var legacyViewCount int
		if err := pool.QueryRow(ctx, `
			SELECT count(*)
			FROM information_schema.views
			WHERE table_schema = 'public'
			  AND table_name = 'connected_app_summaries_v'
		`).Scan(&legacyViewCount); err != nil {
			t.Fatalf("count legacy views: %v", err)
		}
		if legacyViewCount != 0 {
			t.Fatalf("legacy connected-app posture view still present: %d", legacyViewCount)
		}

		var postureColumnCount int
		if err := pool.QueryRow(ctx, `
			SELECT count(*)
			FROM information_schema.columns
			WHERE table_schema = 'public'
			  AND table_name = 'app_assets'
			  AND column_name IN (
			    'governance_state',
			    'ticket_ref',
			    'notes',
			    'evidence_freshness',
			    'evidence_confidence',
			    'evidence_confidence_reason'
			  )
		`).Scan(&postureColumnCount); err != nil {
			t.Fatalf("count app_assets posture columns: %v", err)
		}
		if postureColumnCount != 0 {
			t.Fatalf("app_assets still exposes %d stored posture columns", postureColumnCount)
		}
	})
}

func TestConnectedAppReadModelDerivesEvidenceBuckets(t *testing.T) {
	t.Parallel()

	withEntityCategoryTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *Queries, migrator *migrate.Migrate) {
		migrateUp(t, migrator)

		evaluatedAtTime := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
		runID := insertSyncRun(t, ctx, pool, "google_workspace", "C0123")
		ownerID := insertIdentity(t, ctx, pool, "human", "owner@example.com", "Owner Example")

		freshLowID := insertAppAsset(t, ctx, pool, runID, appAssetSeed{
			SourceKind:  "google_workspace",
			SourceName:  "C0123",
			AssetKind:   "google_oauth_client",
			ExternalID:  "fresh-low.apps.googleusercontent.com",
			DisplayName: "Fresh Low",
		})
		setAppAssetObservedAt(t, ctx, pool, freshLowID, evaluatedAtTime.Add(-24*time.Hour))

		agingMediumID := insertAppAsset(t, ctx, pool, runID, appAssetSeed{
			SourceKind:  "google_workspace",
			SourceName:  "C0123",
			AssetKind:   "google_oauth_client",
			ExternalID:  "aging-medium.apps.googleusercontent.com",
			DisplayName: "Aging Medium",
		})
		setAppAssetObservedAt(t, ctx, pool, agingMediumID, evaluatedAtTime.Add(-10*24*time.Hour))
		if _, err := q.UpsertAppAssetGovernance(ctx, UpsertAppAssetGovernanceParams{
			AppAssetID:      agingMediumID,
			GovernanceState: "in_review",
			OwnerIdentityID: pgtype.Int8{Int64: ownerID, Valid: true},
		}); err != nil {
			t.Fatalf("UpsertAppAssetGovernance(aging medium): %v", err)
		}

		staleHighID := insertAppAsset(t, ctx, pool, runID, appAssetSeed{
			SourceKind:  "google_workspace",
			SourceName:  "C0123",
			AssetKind:   "google_oauth_client",
			ExternalID:  "stale-high.apps.googleusercontent.com",
			DisplayName: "Stale High",
		})
		staleObservedAt := evaluatedAtTime.Add(-40 * 24 * time.Hour)
		setAppAssetObservedAt(t, ctx, pool, staleHighID, staleObservedAt)
		if _, err := q.UpsertAppAssetGovernance(ctx, UpsertAppAssetGovernanceParams{
			AppAssetID:      staleHighID,
			GovernanceState: "approved",
			OwnerIdentityID: pgtype.Int8{Int64: ownerID, Valid: true},
		}); err != nil {
			t.Fatalf("UpsertAppAssetGovernance(stale high): %v", err)
		}
		discoveryAppID := insertSaaSApp(t, ctx, pool, "stale-high", "Stale High", "stale.example.com", "Example", staleObservedAt, staleObservedAt)
		insertSaaSAppSource(t, ctx, pool, discoveryAppID, runID, "google_workspace", "C0123", "stale-high.apps.googleusercontent.com", "Stale High", "stale.example.com", staleObservedAt)

		grantsOnlyID := insertAppAsset(t, ctx, pool, runID, appAssetSeed{
			SourceKind:  "google_workspace",
			SourceName:  "C0123",
			AssetKind:   "google_oauth_client",
			ExternalID:  "grants-only.apps.googleusercontent.com",
			DisplayName: "Grants Only",
		})
		setAppAssetObservedAt(t, ctx, pool, grantsOnlyID, evaluatedAtTime.Add(-48*time.Hour))
		insertCredentialArtifact(t, ctx, pool, runID, credentialArtifactSeed{
			SourceKind:         "google_workspace",
			SourceName:         "C0123",
			AssetRefKind:       "google_oauth_client",
			AssetRefExternalID: "google_oauth_client:grants-only.apps.googleusercontent.com",
			CredentialKind:     "google_oauth_grant",
			ExternalID:         "grant-1",
			DisplayName:        "Grant 1",
			Status:             "active",
		})

		if _, err := q.RefreshAppAssetReadModelsBySource(ctx, RefreshAppAssetReadModelsBySourceParams{
			SourceKind: "google_workspace",
			SourceName: "C0123",
		}); err != nil {
			t.Fatalf("RefreshAppAssetReadModelsBySource(): %v", err)
		}

		cases := []struct {
			name               string
			id                 int64
			wantFreshness      string
			wantConfidence     string
			wantReason         string
			wantGovernance     string
			wantGrantCount     int64
			wantDiscoveryCount int64
		}{
			{
				name:               "fresh low",
				id:                 freshLowID,
				wantFreshness:      "fresh",
				wantConfidence:     "low",
				wantReason:         "This record currently relies on a single evidence path.",
				wantGovernance:     "unreviewed",
				wantGrantCount:     0,
				wantDiscoveryCount: 0,
			},
			{
				name:               "aging medium",
				id:                 agingMediumID,
				wantFreshness:      "aging",
				wantConfidence:     "medium",
				wantReason:         "Multiple evidence paths are available, but attribution is still partial.",
				wantGovernance:     "in_review",
				wantGrantCount:     0,
				wantDiscoveryCount: 0,
			},
			{
				name:               "stale high",
				id:                 staleHighID,
				wantFreshness:      "stale",
				wantConfidence:     "high",
				wantReason:         "Inventory, ownership, and discovery evidence all line up.",
				wantGovernance:     "approved",
				wantGrantCount:     0,
				wantDiscoveryCount: 1,
			},
			{
				name:               "grants do not increase confidence",
				id:                 grantsOnlyID,
				wantFreshness:      "fresh",
				wantConfidence:     "low",
				wantReason:         "This record currently relies on a single evidence path.",
				wantGovernance:     "unreviewed",
				wantGrantCount:     1,
				wantDiscoveryCount: 0,
			},
		}

		for _, tc := range cases {
			row, err := q.GetAppAssetPostureByID(ctx, tc.id)
			if err != nil {
				t.Fatalf("%s: GetAppAssetPostureByID(): %v", tc.name, err)
			}
			if row.EvidenceFreshness != tc.wantFreshness {
				t.Fatalf("%s: evidence_freshness=%q want %q", tc.name, row.EvidenceFreshness, tc.wantFreshness)
			}
			if row.EvidenceConfidence != tc.wantConfidence {
				t.Fatalf("%s: evidence_confidence=%q want %q", tc.name, row.EvidenceConfidence, tc.wantConfidence)
			}
			if row.EvidenceConfidenceReason != tc.wantReason {
				t.Fatalf("%s: evidence_confidence_reason=%q want %q", tc.name, row.EvidenceConfidenceReason, tc.wantReason)
			}
			if row.GovernanceState != tc.wantGovernance {
				t.Fatalf("%s: governance_state=%q want %q", tc.name, row.GovernanceState, tc.wantGovernance)
			}
			if row.GrantCount != tc.wantGrantCount {
				t.Fatalf("%s: grant_count=%d want %d", tc.name, row.GrantCount, tc.wantGrantCount)
			}
			if row.DiscoverySourceCount != tc.wantDiscoveryCount {
				t.Fatalf("%s: discovery_source_count=%d want %d", tc.name, row.DiscoverySourceCount, tc.wantDiscoveryCount)
			}
		}
	})
}

func TestDiscoveryAppReadModelReadsReviewGovernance(t *testing.T) {
	t.Parallel()

	withEntityCategoryTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *Queries, migrator *migrate.Migrate) {
		migrateUp(t, migrator)

		evaluatedAt := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
		ownerID := insertIdentity(t, ctx, pool, "human", "owner@example.com", "Owner Example")
		reviewOwnerID := insertIdentity(t, ctx, pool, "human", "reviewer@example.com", "Reviewer Example")
		saasAppID := insertSaaSApp(t, ctx, pool, "replace-shadow-app", "Replace Shadow App", "shadow.example.com", "Example", evaluatedAt.Add(-48*time.Hour), evaluatedAt.Add(-24*time.Hour))
		replacementID := insertSaaSApp(t, ctx, pool, "approved-app", "Approved App", "approved.example.com", "Example", evaluatedAt.Add(-72*time.Hour), evaluatedAt.Add(-2*time.Hour))

		if _, err := q.UpsertSaaSAppReviewGovernance(ctx, UpsertSaaSAppReviewGovernanceParams{
			SaasAppID:             saasAppID,
			OwnerIdentityID:       pgtype.Int8{Int64: ownerID, Valid: true},
			TicketRef:             "SEC-456",
			Notes:                 "Needs follow-up",
			ReviewDisposition:     "replace",
			ReviewOwnerIdentityID: pgtype.Int8{Int64: reviewOwnerID, Valid: true},
			ReplacementSaasAppID:  pgtype.Int8{Int64: replacementID, Valid: true},
		}); err != nil {
			t.Fatalf("UpsertSaaSAppReviewGovernance(): %v", err)
		}

		if _, err := q.RefreshAllSaaSAppReadModels(ctx); err != nil {
			t.Fatalf("RefreshAllSaaSAppReadModels(): %v", err)
		}

		row, err := q.GetSaaSAppByID(ctx, saasAppID)
		if err != nil {
			t.Fatalf("GetSaaSAppByID(): %v", err)
		}

		if row.ReviewDisposition != "replace" {
			t.Fatalf("review_disposition=%q want replace", row.ReviewDisposition)
		}
		if row.OwnerPrimaryEmail != "owner@example.com" {
			t.Fatalf("owner_primary_email=%q want owner@example.com", row.OwnerPrimaryEmail)
		}
		if row.ReviewOwnerPrimaryEmail != "reviewer@example.com" {
			t.Fatalf("review_owner_primary_email=%q want reviewer@example.com", row.ReviewOwnerPrimaryEmail)
		}
		if row.TicketRef != "SEC-456" || row.Notes != "Needs follow-up" {
			t.Fatalf("ticket_ref/notes=%q/%q want SEC-456/Needs follow-up", row.TicketRef, row.Notes)
		}
		if row.ReplacementSaasAppID != replacementID || row.ReplacementDisplayName != "Approved App" {
			t.Fatalf("replacement=%d/%q want %d/Approved App", row.ReplacementSaasAppID, row.ReplacementDisplayName, replacementID)
		}
		if row.ManagedState != "unmanaged" {
			t.Fatalf("managed_state=%q want unmanaged", row.ManagedState)
		}
		if row.RiskLevel != "high" {
			t.Fatalf("risk_level=%q want high", row.RiskLevel)
		}
	})
}

func TestUpsertSaaSAppReviewGovernanceSyncsGovernanceState(t *testing.T) {
	t.Parallel()

	withEntityCategoryTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *Queries, migrator *migrate.Migrate) {
		migrateUp(t, migrator)

		evaluatedAt := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
		tests := []struct {
			name                string
			reviewDisposition   string
			wantGovernanceState string
		}{
			{name: "unreviewed", reviewDisposition: "unreviewed", wantGovernanceState: "unreviewed"},
			{name: "under review", reviewDisposition: "under_review", wantGovernanceState: "in_review"},
			{name: "sanctioned", reviewDisposition: "sanctioned", wantGovernanceState: "approved"},
			{name: "tolerated", reviewDisposition: "tolerated", wantGovernanceState: "approved"},
			{name: "replace", reviewDisposition: "replace", wantGovernanceState: "action_required"},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				saasAppID := insertSaaSApp(t, ctx, pool, "sync-state-"+tc.reviewDisposition, "Sync State "+tc.reviewDisposition, tc.reviewDisposition+".example.com", "Example", evaluatedAt.Add(-48*time.Hour), evaluatedAt.Add(-24*time.Hour))

				if _, err := q.UpsertSaaSAppReviewGovernance(ctx, UpsertSaaSAppReviewGovernanceParams{
					SaasAppID:         saasAppID,
					ReviewDisposition: tc.reviewDisposition,
				}); err != nil {
					t.Fatalf("UpsertSaaSAppReviewGovernance(): %v", err)
				}

				var governanceState string
				if err := pool.QueryRow(ctx, `
					SELECT governance_state
					FROM governance_subject_overrides
					WHERE subject_kind = 'saas_app' AND subject_id = $1
				`, saasAppID).Scan(&governanceState); err != nil {
					t.Fatalf("select governance_state: %v", err)
				}
				if governanceState != tc.wantGovernanceState {
					t.Fatalf("governance_state=%q want %q", governanceState, tc.wantGovernanceState)
				}
			})
		}
	})
}

func TestDiscoveryAppReadModelDefaultsDiscoveryReviewWorkflowFields(t *testing.T) {
	t.Parallel()

	withEntityCategoryTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *Queries, migrator *migrate.Migrate) {
		migrateUp(t, migrator)

		evaluatedAt := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
		saasAppID := insertSaaSApp(t, ctx, pool, "default-shadow-app", "Default Shadow App", "default.example.com", "Example", evaluatedAt.Add(-48*time.Hour), evaluatedAt.Add(-24*time.Hour))

		if _, err := q.RefreshAllSaaSAppReadModels(ctx); err != nil {
			t.Fatalf("RefreshAllSaaSAppReadModels(): %v", err)
		}

		row, err := q.GetSaaSAppByID(ctx, saasAppID)
		if err != nil {
			t.Fatalf("GetSaaSAppByID(): %v", err)
		}

		if row.ReviewDisposition != "unreviewed" {
			t.Fatalf("review_disposition=%q want unreviewed", row.ReviewDisposition)
		}
		if row.ReviewOwnerPrimaryEmail != "" {
			t.Fatalf("review_owner_primary_email=%q want empty", row.ReviewOwnerPrimaryEmail)
		}
		if row.FollowUpDueDate.Valid {
			t.Fatalf("follow_up_due_date valid=%t want false", row.FollowUpDueDate.Valid)
		}
		if row.IsFollowUpOverdue {
			t.Fatalf("is_follow_up_overdue=true want false")
		}
		if row.ReplacementSaasAppID != 0 {
			t.Fatalf("replacement_saas_app_id=%d want 0", row.ReplacementSaasAppID)
		}
		if row.ReplacementDisplayName != "" {
			t.Fatalf("replacement_display_name=%q want empty", row.ReplacementDisplayName)
		}
	})
}

func TestSearchManagedReplacementSaaSAppsFiltersManagedAlternatives(t *testing.T) {
	t.Parallel()

	withEntityCategoryTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *Queries, migrator *migrate.Migrate) {
		migrateUp(t, migrator)

		evaluatedAt := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
		runID := insertSyncRun(t, ctx, pool, "github", "acme")

		currentID := insertSaaSApp(t, ctx, pool, "current-shadow-app", "Current Shadow App", "current.example.com", "Example", evaluatedAt.Add(-48*time.Hour), evaluatedAt.Add(-24*time.Hour))
		managedID := insertSaaSApp(t, ctx, pool, "approved-app", "Approved App", "approved.example.com", "Example", evaluatedAt.Add(-72*time.Hour), evaluatedAt.Add(-2*time.Hour))
		unmanagedID := insertSaaSApp(t, ctx, pool, "legacy-app", "Legacy App", "legacy.example.com", "Example", evaluatedAt.Add(-96*time.Hour), evaluatedAt.Add(-4*time.Hour))

		insertSaaSAppSource(t, ctx, pool, currentID, runID, "github", "acme", "current-shadow-app", "Current Shadow App", "current.example.com", evaluatedAt.Add(-24*time.Hour))
		insertSaaSAppSource(t, ctx, pool, managedID, runID, "github", "acme", "approved-app", "Approved App", "approved.example.com", evaluatedAt.Add(-2*time.Hour))
		insertSaaSAppSource(t, ctx, pool, unmanagedID, runID, "github", "acme", "legacy-app", "Legacy App", "legacy.example.com", evaluatedAt.Add(-4*time.Hour))

		if err := q.UpsertSaaSAppBinding(ctx, UpsertSaaSAppBindingParams{
			SaasAppID:           currentID,
			ConnectorKind:       "github",
			ConnectorSourceName: "acme",
			BindingSource:       "seed",
			Confidence:          1,
			IsPrimary:           true,
			CreatedByAuthUserID: pgtype.Int8{},
		}); err != nil {
			t.Fatalf("UpsertSaaSAppBinding(current): %v", err)
		}
		if err := q.UpsertSaaSAppBinding(ctx, UpsertSaaSAppBindingParams{
			SaasAppID:           managedID,
			ConnectorKind:       "github",
			ConnectorSourceName: "acme",
			BindingSource:       "seed",
			Confidence:          1,
			IsPrimary:           true,
			CreatedByAuthUserID: pgtype.Int8{},
		}); err != nil {
			t.Fatalf("UpsertSaaSAppBinding(managed): %v", err)
		}

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
		`, "github", "acme", evaluatedAt.Add(-30*time.Minute), evaluatedAt.Add(2*time.Hour)); err != nil {
			t.Fatalf("insert connector_source_state: %v", err)
		}

		if _, err := q.RefreshAllSaaSAppReadModels(ctx); err != nil {
			t.Fatalf("RefreshAllSaaSAppReadModels(): %v", err)
		}

		rows, err := q.SearchManagedReplacementSaaSApps(ctx, SearchManagedReplacementSaaSAppsParams{
			ExcludeID: currentID,
			Query:     "app",
			LimitRows: 10,
		})
		if err != nil {
			t.Fatalf("SearchManagedReplacementSaaSApps(): %v", err)
		}
		if len(rows) != 1 {
			t.Fatalf("rows len=%d want 1", len(rows))
		}
		if rows[0].ID != managedID {
			t.Fatalf("replacement id=%d want %d", rows[0].ID, managedID)
		}
		if rows[0].DisplayName != "Approved App" {
			t.Fatalf("replacement display_name=%q want Approved App", rows[0].DisplayName)
		}
	})
}

func TestGetSaaSAppReplacementCandidateByIDFiltersManagedDiscoveryScopedAlternatives(t *testing.T) {
	t.Parallel()

	withEntityCategoryTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *Queries, migrator *migrate.Migrate) {
		migrateUp(t, migrator)

		evaluatedAt := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
		runID := insertSyncRun(t, ctx, pool, "github", "acme")
		hiddenRunID := insertSyncRun(t, ctx, pool, "datadog", "datadoghq.com")

		managedID := insertSaaSApp(t, ctx, pool, "approved-app", "Approved App", "approved.example.com", "Example", evaluatedAt.Add(-72*time.Hour), evaluatedAt.Add(-2*time.Hour))
		unmanagedID := insertSaaSApp(t, ctx, pool, "legacy-app", "Legacy App", "legacy.example.com", "Example", evaluatedAt.Add(-96*time.Hour), evaluatedAt.Add(-4*time.Hour))
		hiddenManagedID := insertSaaSApp(t, ctx, pool, "hidden-app", "Hidden App", "hidden.example.com", "Example", evaluatedAt.Add(-48*time.Hour), evaluatedAt.Add(-90*time.Minute))

		insertSaaSAppSource(t, ctx, pool, managedID, runID, "github", "acme", "approved-app", "Approved App", "approved.example.com", evaluatedAt.Add(-2*time.Hour))
		insertSaaSAppSource(t, ctx, pool, unmanagedID, runID, "github", "acme", "legacy-app", "Legacy App", "legacy.example.com", evaluatedAt.Add(-4*time.Hour))
		insertSaaSAppSource(t, ctx, pool, hiddenManagedID, hiddenRunID, "datadog", "datadoghq.com", "hidden-app", "Hidden App", "hidden.example.com", evaluatedAt.Add(-90*time.Minute))

		if err := q.UpsertSaaSAppBinding(ctx, UpsertSaaSAppBindingParams{
			SaasAppID:           managedID,
			ConnectorKind:       "github",
			ConnectorSourceName: "acme",
			BindingSource:       "seed",
			Confidence:          1,
			IsPrimary:           true,
			CreatedByAuthUserID: pgtype.Int8{},
		}); err != nil {
			t.Fatalf("UpsertSaaSAppBinding(managed): %v", err)
		}
		if err := q.UpsertSaaSAppBinding(ctx, UpsertSaaSAppBindingParams{
			SaasAppID:           hiddenManagedID,
			ConnectorKind:       "datadog",
			ConnectorSourceName: "datadoghq.com",
			BindingSource:       "seed",
			Confidence:          1,
			IsPrimary:           true,
			CreatedByAuthUserID: pgtype.Int8{},
		}); err != nil {
			t.Fatalf("UpsertSaaSAppBinding(hidden managed): %v", err)
		}

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
		`, "github", "acme", evaluatedAt.Add(-30*time.Minute), evaluatedAt.Add(2*time.Hour)); err != nil {
			t.Fatalf("insert connector_source_state: %v", err)
		}
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
			VALUES ($1, $2, true, true, false, $3, $4, now())
			ON CONFLICT (source_kind, source_name) DO UPDATE SET
				enabled = EXCLUDED.enabled,
				configured = EXCLUDED.configured,
				discovery_enabled = EXCLUDED.discovery_enabled,
				last_success_at = EXCLUDED.last_success_at,
				fresh_until_at = EXCLUDED.fresh_until_at,
				updated_at = now()
		`, "datadog", "datadoghq.com", evaluatedAt.Add(-30*time.Minute), evaluatedAt.Add(2*time.Hour)); err != nil {
			t.Fatalf("insert hidden connector_source_state: %v", err)
		}

		if _, err := q.RefreshAllSaaSAppReadModels(ctx); err != nil {
			t.Fatalf("RefreshAllSaaSAppReadModels(): %v", err)
		}

		row, err := q.GetSaaSAppReplacementCandidateByID(ctx, managedID)
		if err != nil {
			t.Fatalf("GetSaaSAppReplacementCandidateByID(managed): %v", err)
		}
		if row.ID != managedID {
			t.Fatalf("replacement id=%d want %d", row.ID, managedID)
		}

		_, err = q.GetSaaSAppReplacementCandidateByID(ctx, unmanagedID)
		if !errors.Is(err, pgx.ErrNoRows) {
			t.Fatalf("GetSaaSAppReplacementCandidateByID(unmanaged) err=%v want %v", err, pgx.ErrNoRows)
		}

		_, err = q.GetSaaSAppReplacementCandidateByID(ctx, hiddenManagedID)
		if !errors.Is(err, pgx.ErrNoRows) {
			t.Fatalf("GetSaaSAppReplacementCandidateByID(hidden managed) err=%v want %v", err, pgx.ErrNoRows)
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

func setAppAssetObservedAt(t *testing.T, ctx context.Context, pool *pgxpool.Pool, appAssetID int64, observedAt time.Time) {
	t.Helper()

	if _, err := pool.Exec(ctx, `
		UPDATE app_assets
		SET seen_at = $2,
		    last_observed_at = $2,
		    updated_at = now()
		WHERE id = $1
	`, appAssetID, observedAt.UTC()); err != nil {
		t.Fatalf("update app asset observed_at %d: %v", appAssetID, err)
	}
}

func insertSaaSApp(t *testing.T, ctx context.Context, pool *pgxpool.Pool, canonicalKey, displayName, primaryDomain, vendorName string, firstSeenAt, lastSeenAt time.Time) int64 {
	t.Helper()

	var id int64
	err := pool.QueryRow(ctx, `
		INSERT INTO saas_apps (
			canonical_key,
			display_name,
			primary_domain,
			vendor_name,
			first_seen_at,
			last_seen_at,
			created_at,
			updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, now(), now())
		RETURNING id
	`, canonicalKey, displayName, primaryDomain, vendorName, firstSeenAt.UTC(), lastSeenAt.UTC()).Scan(&id)
	if err != nil {
		t.Fatalf("insert saas app %s: %v", canonicalKey, err)
	}
	return id
}

func insertSaaSAppSource(t *testing.T, ctx context.Context, pool *pgxpool.Pool, saasAppID, runID int64, sourceKind, sourceName, sourceAppID, sourceAppName, sourceAppDomain string, observedAt time.Time) {
	t.Helper()

	if _, err := pool.Exec(ctx, `
		INSERT INTO saas_app_sources (
			saas_app_id,
			source_kind,
			source_name,
			source_app_id,
			source_app_name,
			source_app_domain,
			seen_in_run_id,
			seen_at,
			last_observed_run_id,
			last_observed_at,
			created_at,
			updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $7, $8, now(), now())
	`, saasAppID, sourceKind, sourceName, sourceAppID, sourceAppName, sourceAppDomain, runID, observedAt.UTC()); err != nil {
		t.Fatalf("insert saas app source %s/%s: %v", sourceKind, sourceAppID, err)
	}
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
