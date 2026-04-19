package gen

import (
	"context"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
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
