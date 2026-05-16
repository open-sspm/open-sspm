package events

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

type PartitionManager struct {
	db gen.DBTX
}

type PartitionMaintenanceConfig struct {
	Now           func() time.Time
	FutureDays    int32
	RetentionDays int32
}

type PartitionMaintenanceResult struct {
	EnsuredStart time.Time
	EnsuredEnd   time.Time
	Dropped      int
}

func NewPartitionManager(db gen.DBTX) *PartitionManager {
	return &PartitionManager{db: db}
}

func (m *PartitionManager) MaintainDailyPartitions(ctx context.Context, cfg PartitionMaintenanceConfig) (PartitionMaintenanceResult, error) {
	if m == nil || m.db == nil {
		return PartitionMaintenanceResult{}, fmt.Errorf("event partition manager is not configured")
	}
	now := time.Now
	if cfg.Now != nil {
		now = cfg.Now
	}
	current := now()
	if current.IsZero() {
		current = time.Now()
	}
	if cfg.FutureDays <= 0 {
		cfg.FutureDays = 7
	}
	if cfg.RetentionDays <= 0 {
		cfg.RetentionDays = 90
	}

	start := utcDay(current.AddDate(0, 0, -1))
	end := utcDay(current.AddDate(0, 0, int(cfg.FutureDays)))
	if err := m.EnsureDailyPartitions(ctx, start, end); err != nil {
		return PartitionMaintenanceResult{}, err
	}
	dropped, err := m.DropExpiredDailyPartitions(ctx, current.AddDate(0, 0, -int(cfg.RetentionDays)))
	if err != nil {
		return PartitionMaintenanceResult{}, err
	}
	return PartitionMaintenanceResult{
		EnsuredStart: start,
		EnsuredEnd:   end,
		Dropped:      dropped,
	}, nil
}

func (m *PartitionManager) EnsureDailyPartitions(ctx context.Context, start, end time.Time) error {
	if m == nil || m.db == nil {
		return fmt.Errorf("event partition manager is not configured")
	}
	start = utcDay(start)
	end = utcDay(end)
	if end.Before(start) {
		return fmt.Errorf("partition end %s is before start %s", end.Format(time.DateOnly), start.Format(time.DateOnly))
	}
	for day := start; !day.After(end); day = day.AddDate(0, 0, 1) {
		next := day.AddDate(0, 0, 1)
		if err := m.createDailyPartition(ctx, "events", day, next); err != nil {
			return err
		}
		if err := m.createDailyPartition(ctx, "event_targets", day, next); err != nil {
			return err
		}
	}
	return nil
}

func (m *PartitionManager) DropExpiredDailyPartitions(ctx context.Context, cutoff time.Time) (int, error) {
	if m == nil || m.db == nil {
		return 0, fmt.Errorf("event partition manager is not configured")
	}
	cutoff = utcDay(cutoff)
	droppedTargets, err := m.dropExpiredDailyPartitionsForParent(ctx, "event_targets", cutoff)
	if err != nil {
		return droppedTargets, err
	}
	droppedEvents, err := m.dropExpiredDailyPartitionsForParent(ctx, "events", cutoff)
	if err != nil {
		return droppedTargets + droppedEvents, err
	}
	return droppedTargets + droppedEvents, nil
}

func (m *PartitionManager) createDailyPartition(ctx context.Context, parent string, start, end time.Time) error {
	name := fmt.Sprintf("%s_%s", parent, start.Format("20060102"))
	stmt := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s PARTITION OF %s FOR VALUES FROM (TIMESTAMPTZ %s) TO (TIMESTAMPTZ %s)",
		pgx.Identifier{name}.Sanitize(),
		pgx.Identifier{parent}.Sanitize(),
		quoteSQLString(start.Format("2006-01-02 15:04:05-07")),
		quoteSQLString(end.Format("2006-01-02 15:04:05-07")),
	)
	if _, err := m.db.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("create %s partition for %s: %w", parent, start.Format(time.DateOnly), err)
	}
	return nil
}

func (m *PartitionManager) dropExpiredDailyPartitionsForParent(ctx context.Context, parent string, cutoff time.Time) (int, error) {
	rows, err := m.db.Query(ctx, `
SELECT child_ns.nspname, child.relname
FROM pg_inherits
JOIN pg_class parent ON parent.oid = pg_inherits.inhparent
JOIN pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace
JOIN pg_class child ON child.oid = pg_inherits.inhrelid
JOIN pg_namespace child_ns ON child_ns.oid = child.relnamespace
WHERE parent.relname = $1
  AND child.relname LIKE $2
  AND parent_ns.nspname = child_ns.nspname
`, parent, parent+`_%%%%%%%%`)
	if err != nil {
		return 0, fmt.Errorf("list %s partitions: %w", parent, err)
	}
	defer rows.Close()

	type partitionRef struct {
		schema string
		name   string
	}
	var expired []partitionRef
	for rows.Next() {
		var ref partitionRef
		if err := rows.Scan(&ref.schema, &ref.name); err != nil {
			return 0, fmt.Errorf("scan %s partition: %w", parent, err)
		}
		day, ok := parseDailyPartitionDay(parent, ref.name)
		if !ok || !day.Before(cutoff) {
			continue
		}
		expired = append(expired, ref)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate %s partitions: %w", parent, err)
	}

	for _, ref := range expired {
		stmt := fmt.Sprintf("DROP TABLE IF EXISTS %s", pgx.Identifier{ref.schema, ref.name}.Sanitize())
		if _, err := m.db.Exec(ctx, stmt); err != nil {
			return 0, fmt.Errorf("drop expired %s partition %s: %w", parent, ref.name, err)
		}
	}
	return len(expired), nil
}

func parseDailyPartitionDay(parent, name string) (time.Time, bool) {
	prefix := parent + "_"
	if !strings.HasPrefix(name, prefix) {
		return time.Time{}, false
	}
	suffix := strings.TrimPrefix(name, prefix)
	if len(suffix) != len("20060102") {
		return time.Time{}, false
	}
	day, err := time.ParseInLocation("20060102", suffix, time.UTC)
	if err != nil {
		return time.Time{}, false
	}
	return utcDay(day), true
}

func utcDay(t time.Time) time.Time {
	if t.IsZero() {
		t = time.Now()
	}
	year, month, day := t.UTC().Date()
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

func quoteSQLString(value string) string {
	return "'" + value + "'"
}
