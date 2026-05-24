package gen

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestSummarizeIdentitiesInventoryByFiltersCountsOnlyStaleInStaleChip(t *testing.T) {
	t.Parallel()

	withEntityCategoryTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *Queries, migrator *migrate.Migrate) {
		migrateUp(t, migrator)

		runID := insertSyncRun(t, ctx, pool, "entra", "tenant-1")

		staleAccountID := insertAccount(t, ctx, pool, runID, accountSeed{
			SourceKind:     "entra",
			SourceName:     "tenant-1",
			ExternalID:     "user-stale",
			Email:          "stale@example.com",
			DisplayName:    "Stale User",
			Status:         "active",
			AccountKind:    "human",
			EntityCategory: "user",
			RawJSON:        `{"status":"active"}`,
		})
		staleIdentityID := insertIdentity(t, ctx, pool, "human", "stale@example.com", "Stale User")
		insertIdentityAccountLink(t, ctx, pool, staleIdentityID, staleAccountID)
		if _, err := pool.Exec(ctx, `
			UPDATE accounts
			SET last_observed_at = $1
			WHERE id = $2
		`, time.Now().UTC().Add(-120*24*time.Hour), staleAccountID); err != nil {
			t.Fatalf("update stale account last_observed_at: %v", err)
		}

		neverSeenAccountID := insertAccount(t, ctx, pool, runID, accountSeed{
			SourceKind:     "entra",
			SourceName:     "tenant-1",
			ExternalID:     "user-never",
			Email:          "never@example.com",
			DisplayName:    "Never Seen User",
			Status:         "active",
			AccountKind:    "human",
			EntityCategory: "user",
			RawJSON:        `{"status":"active"}`,
		})
		neverSeenIdentityID := insertIdentity(t, ctx, pool, "human", "never@example.com", "Never Seen User")
		insertIdentityAccountLink(t, ctx, pool, neverSeenIdentityID, neverSeenAccountID)
		if _, err := pool.Exec(ctx, `
			UPDATE accounts
			SET last_observed_at = NULL
			WHERE id = $1
		`, neverSeenAccountID); err != nil {
			t.Fatalf("update never-seen account last_observed_at: %v", err)
		}

		summary, err := q.SummarizeIdentitiesInventoryByFilters(ctx, SummarizeIdentitiesInventoryByFiltersParams{
			ConfiguredSourceKinds: []string{"entra"},
			ConfiguredSourceNames: []string{"tenant-1"},
		})
		if err != nil {
			t.Fatalf("SummarizeIdentitiesInventoryByFilters(): %v", err)
		}
		if summary.TotalCount != 2 {
			t.Fatalf("total_count = %d, want 2", summary.TotalCount)
		}
		if summary.StaleCount != 1 {
			t.Fatalf("stale_count = %d, want 1", summary.StaleCount)
		}
	})
}

func TestIdentityAuthoritativePostureUsesConfiguredSourceScope(t *testing.T) {
	t.Parallel()

	withEntityCategoryTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *Queries, migrator *migrate.Migrate) {
		migrateUp(t, migrator)

		runID := insertSyncRun(t, ctx, pool, "seed", "identity-scope")
		identityID := insertIdentity(t, ctx, pool, "human", "owner@example.com", "Owner")
		oktaAccountID := insertAccount(t, ctx, pool, runID, accountSeed{
			SourceKind:     "okta",
			SourceName:     "retired.okta.com",
			ExternalID:     "owner-okta",
			Email:          "owner@example.com",
			DisplayName:    "Owner Okta",
			Status:         "active",
			AccountKind:    "human",
			EntityCategory: "user",
			RawJSON:        `{"status":"active"}`,
		})
		githubAccountID := insertAccount(t, ctx, pool, runID, accountSeed{
			SourceKind:     "github",
			SourceName:     "acme",
			ExternalID:     "owner-gh",
			Email:          "owner@example.com",
			DisplayName:    "Owner GitHub",
			Status:         "active",
			AccountKind:    "human",
			EntityCategory: "user",
			RawJSON:        `{"status":"active"}`,
		})
		insertIdentityAccountLink(t, ctx, pool, identityID, oktaAccountID)
		insertIdentityAccountLink(t, ctx, pool, identityID, githubAccountID)
		insertIdentitySourceSetting(t, ctx, pool, "okta", "retired.okta.com", true)

		summary, err := q.GetIdentitySummaryByID(ctx, GetIdentitySummaryByIDParams{
			ID:                    identityID,
			ConfiguredSourceKinds: []string{"github"},
			ConfiguredSourceNames: []string{"acme"},
		})
		if err != nil {
			t.Fatalf("GetIdentitySummaryByID(github scope): %v", err)
		}
		if summary.AnchorState != "missing_anchor" {
			t.Fatalf("github-scoped anchor_state = %q, want missing_anchor", summary.AnchorState)
		}

		summary, err = q.GetIdentitySummaryByID(ctx, GetIdentitySummaryByIDParams{
			ID:                    identityID,
			ConfiguredSourceKinds: []string{"github", "okta"},
			ConfiguredSourceNames: []string{"acme", "retired.okta.com"},
		})
		if err != nil {
			t.Fatalf("GetIdentitySummaryByID(github+okta scope): %v", err)
		}
		if summary.AnchorState != "anchored" {
			t.Fatalf("github+okta-scoped anchor_state = %q, want anchored", summary.AnchorState)
		}
	})
}

func TestFindUnambiguousIdentityByPrimaryEmailScopesAuthoritativeTieBreaker(t *testing.T) {
	t.Parallel()

	withEntityCategoryTestDatabase(t, func(ctx context.Context, pool *pgxpool.Pool, q *Queries, migrator *migrate.Migrate) {
		migrateUp(t, migrator)

		runID := insertSyncRun(t, ctx, pool, "seed", "identity-scope")
		retiredIdentityID := insertIdentity(t, ctx, pool, "human", "team@example.com", "Retired Anchor")
		currentIdentityID := insertIdentity(t, ctx, pool, "human", "team@example.com", "Current Identity")
		oktaAccountID := insertAccount(t, ctx, pool, runID, accountSeed{
			SourceKind:     "okta",
			SourceName:     "retired.okta.com",
			ExternalID:     "team-okta",
			Email:          "team@example.com",
			DisplayName:    "Team Okta",
			Status:         "active",
			AccountKind:    "human",
			EntityCategory: "user",
			RawJSON:        `{"status":"active"}`,
		})
		githubAccountID := insertAccount(t, ctx, pool, runID, accountSeed{
			SourceKind:     "github",
			SourceName:     "acme",
			ExternalID:     "team-gh",
			Email:          "team@example.com",
			DisplayName:    "Team GitHub",
			Status:         "active",
			AccountKind:    "human",
			EntityCategory: "user",
			RawJSON:        `{"status":"active"}`,
		})
		insertIdentityAccountLink(t, ctx, pool, retiredIdentityID, oktaAccountID)
		insertIdentityAccountLink(t, ctx, pool, currentIdentityID, githubAccountID)
		insertIdentitySourceSetting(t, ctx, pool, "okta", "retired.okta.com", true)

		_, err := q.FindUnambiguousIdentityByPrimaryEmail(ctx, FindUnambiguousIdentityByPrimaryEmailParams{
			ConfiguredSourceKinds: []string{"github"},
			ConfiguredSourceNames: []string{"acme"},
			PrimaryEmail:          "team@example.com",
		})
		if !errors.Is(err, pgx.ErrNoRows) {
			t.Fatalf("FindUnambiguousIdentityByPrimaryEmail(github scope) err = %v, want ErrNoRows", err)
		}

		identity, err := q.FindUnambiguousIdentityByPrimaryEmail(ctx, FindUnambiguousIdentityByPrimaryEmailParams{
			ConfiguredSourceKinds: []string{"github", "okta"},
			ConfiguredSourceNames: []string{"acme", "retired.okta.com"},
			PrimaryEmail:          "team@example.com",
		})
		if err != nil {
			t.Fatalf("FindUnambiguousIdentityByPrimaryEmail(github+okta scope): %v", err)
		}
		if identity.ID != retiredIdentityID {
			t.Fatalf("identity ID = %d, want retired authoritative identity %d", identity.ID, retiredIdentityID)
		}
	})
}
