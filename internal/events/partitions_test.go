package events

import (
	"context"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/testdb"
)

func TestPartitionManagerMaintainCreatesFutureAndDropsExpired(t *testing.T) {
	testdb.WithDatabase(t, testdb.Options{NamePrefix: "event_partitions"}, func(ctx context.Context, pool *pgxpool.Pool, migrator *migrate.Migrate) {
		testdb.MigrateUp(t, migrator)

		manager := NewPartitionManager(pool)
		oldDay := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		now := time.Date(2026, 5, 16, 10, 0, 0, 0, time.UTC)
		if err := manager.EnsureDailyPartitions(ctx, oldDay, oldDay); err != nil {
			t.Fatalf("EnsureDailyPartitions(oldDay) error = %v", err)
		}

		result, err := manager.MaintainDailyPartitions(ctx, PartitionMaintenanceConfig{
			Now:           func() time.Time { return now },
			FutureDays:    2,
			RetentionDays: 30,
		})
		if err != nil {
			t.Fatalf("MaintainDailyPartitions() error = %v", err)
		}
		if result.Dropped != 2 {
			t.Fatalf("Dropped = %d, want old events and event_targets partitions", result.Dropped)
		}
		assertPartitionExists(t, ctx, pool, "events_20260516")
		assertPartitionExists(t, ctx, pool, "event_targets_20260518")
		assertPartitionMissing(t, ctx, pool, "events_20260101")
		assertPartitionMissing(t, ctx, pool, "event_targets_20260101")
	})
}

func assertPartitionExists(t *testing.T, ctx context.Context, pool *pgxpool.Pool, name string) {
	t.Helper()
	var exists bool
	if err := pool.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", name).Scan(&exists); err != nil {
		t.Fatalf("query partition %s: %v", name, err)
	}
	if !exists {
		t.Fatalf("partition %s does not exist", name)
	}
}

func assertPartitionMissing(t *testing.T, ctx context.Context, pool *pgxpool.Pool, name string) {
	t.Helper()
	var exists bool
	if err := pool.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", name).Scan(&exists); err != nil {
		t.Fatalf("query partition %s: %v", name, err)
	}
	if exists {
		t.Fatalf("partition %s exists, want missing", name)
	}
}
