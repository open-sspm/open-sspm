package inbox

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/metrics"
)

const (
	defaultBatchSize               int32 = 500
	defaultPollInterval                  = 5 * time.Second
	defaultCleanupInterval               = time.Hour
	defaultRetryDelay                    = 30 * time.Second
	defaultRetryDelayMax                 = 15 * time.Minute
	defaultLeaseTTL                      = 5 * time.Minute
	defaultMaxAttempts             int32 = 10
	defaultProcessedRetentionDays  int32 = 30
	defaultDeadLetterRetentionDays int32 = 90
)

type Config struct {
	BatchSize               int32
	PollInterval            time.Duration
	CleanupInterval         time.Duration
	RetryDelay              time.Duration
	RetryDelayMax           time.Duration
	LeaseTTL                time.Duration
	MaxAttempts             int32
	ProcessedRetentionDays  int32
	DeadLetterRetentionDays int32
	ClaimedBy               string
	OnLoopTick              func()
	OnClaimAttempt          func()
}

func DefaultConfig() Config {
	return Config{
		BatchSize:               defaultBatchSize,
		PollInterval:            defaultPollInterval,
		CleanupInterval:         defaultCleanupInterval,
		RetryDelay:              defaultRetryDelay,
		RetryDelayMax:           defaultRetryDelayMax,
		LeaseTTL:                defaultLeaseTTL,
		MaxAttempts:             defaultMaxAttempts,
		ProcessedRetentionDays:  defaultProcessedRetentionDays,
		DeadLetterRetentionDays: defaultDeadLetterRetentionDays,
	}
}

func RunLoop(ctx context.Context, q *gen.Queries, handler Handler, cfg Config) error {
	cfg = cfg.normalized()
	slog.Info("starting event inbox processor", "interval", cfg.PollInterval, "batch_size", cfg.BatchSize)

	runEventInboxIteration(ctx, q, handler, cfg)

	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()

	staleTicker := time.NewTicker(max(cfg.LeaseTTL/2, cfg.PollInterval))
	defer staleTicker.Stop()

	cleanupTicker := time.NewTicker(cfg.CleanupInterval)
	defer cleanupTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			runEventInboxIteration(ctx, q, handler, cfg)
		case <-staleTicker.C:
			if err := requeueExpiredLeases(ctx, q); err != nil {
				slog.Warn("event inbox stale-row recovery failed", "error", err)
			}
		case <-cleanupTicker.C:
			if err := cleanupDeliveries(ctx, q, cfg); err != nil {
				slog.Warn("event inbox cleanup failed", "error", err)
			}
			if err := RefreshMetrics(ctx, q); err != nil {
				slog.Warn("event inbox metrics refresh failed", "error", err)
			}
		}
	}
}

func ProcessQueued(ctx context.Context, q *gen.Queries, handler Handler, limit int32, cfg Config) (RunOnceResult, error) {
	cfg = cfg.normalized()
	if limit > 0 {
		cfg.BatchSize = limit
	}

	store := NewStore(q)
	processor := NewProcessor(store, handler, ProcessorConfig{
		BatchSize:     cfg.BatchSize,
		LeaseOwner:    cfg.ClaimedBy,
		LeaseTTL:      cfg.LeaseTTL,
		RetryDelay:    cfg.RetryDelay,
		MaxRetryDelay: cfg.RetryDelayMax,
		MaxAttempts:   cfg.MaxAttempts,
	})
	cfg.observeClaimAttempt()
	result, err := processor.RunOnce(ctx)
	cfg.observeLoopTick()
	return result, err
}

func runEventInboxIteration(ctx context.Context, q *gen.Queries, handler Handler, cfg Config) {
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		result, err := ProcessQueued(ctx, q, handler, cfg.BatchSize, cfg)
		if err != nil {
			slog.Warn("event inbox processing failed", "error", err, "claimed", result.Claimed, "processed", result.Processed, "ignored", result.Ignored, "dead", result.Dead, "retried", result.Retried)
		}
		if result.Claimed == 0 || int32(result.Claimed) < cfg.BatchSize {
			return
		}
	}
}

func requeueExpiredLeases(ctx context.Context, q *gen.Queries) error {
	if q == nil {
		return nil
	}
	_, err := q.RequeueExpiredEventInboxLeases(ctx)
	return err
}

func cleanupDeliveries(ctx context.Context, q *gen.Queries, cfg Config) error {
	_, err := q.DeleteOldEventInboxDeliveries(ctx, gen.DeleteOldEventInboxDeliveriesParams{
		ProcessedRetentionDays:  cfg.ProcessedRetentionDays,
		DeadLetterRetentionDays: cfg.DeadLetterRetentionDays,
	})
	return err
}

func RefreshMetrics(ctx context.Context, q *gen.Queries) error {
	rows, err := q.ListEventInboxMetricsBySourceChannel(ctx)
	if err != nil {
		return err
	}
	seen := make(map[sourceChannelKey]struct{}, len(rows))
	for _, row := range rows {
		key := sourceChannelKey{sourceKind: row.SourceKind, sourceName: row.SourceName, channel: row.Channel}
		seen[key] = struct{}{}
		metrics.EventInboxQueueDepth.WithLabelValues(row.SourceKind, row.SourceName, row.Channel).Set(float64(row.QueuedCount))
		metrics.EventInboxDeadLetterRows.WithLabelValues(row.SourceKind, row.SourceName, row.Channel).Set(float64(row.DeadLetterCount))
		if row.LastReceivedAt.Valid {
			metrics.EventInboxLastReceivedTimestamp.WithLabelValues(row.SourceKind, row.SourceName, row.Channel).Set(float64(row.LastReceivedAt.Time.Unix()))
		}
		if row.LastProcessedAt.Valid {
			metrics.EventInboxLastProcessedTimestamp.WithLabelValues(row.SourceKind, row.SourceName, row.Channel).Set(float64(row.LastProcessedAt.Time.Unix()))
		}
	}
	pruneStaleMetrics(seen)
	return nil
}

var (
	metricLabelsMu sync.Mutex
	metricLabels   = map[sourceChannelKey]struct{}{}
)

func pruneStaleMetrics(seen map[sourceChannelKey]struct{}) {
	metricLabelsMu.Lock()
	defer metricLabelsMu.Unlock()
	for key := range metricLabels {
		if _, ok := seen[key]; ok {
			continue
		}
		metrics.EventInboxQueueDepth.DeleteLabelValues(key.sourceKind, key.sourceName, key.channel)
		metrics.EventInboxDeadLetterRows.DeleteLabelValues(key.sourceKind, key.sourceName, key.channel)
		metrics.EventInboxLastReceivedTimestamp.DeleteLabelValues(key.sourceKind, key.sourceName, key.channel)
		metrics.EventInboxLastProcessedTimestamp.DeleteLabelValues(key.sourceKind, key.sourceName, key.channel)
	}
	metricLabels = seen
}

func (cfg Config) normalized() Config {
	defaults := DefaultConfig()
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaults.BatchSize
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaults.PollInterval
	}
	if cfg.CleanupInterval <= 0 {
		cfg.CleanupInterval = defaults.CleanupInterval
	}
	if cfg.RetryDelay <= 0 {
		cfg.RetryDelay = defaults.RetryDelay
	}
	if cfg.RetryDelayMax <= 0 {
		cfg.RetryDelayMax = defaults.RetryDelayMax
	}
	if cfg.RetryDelayMax < cfg.RetryDelay {
		cfg.RetryDelayMax = cfg.RetryDelay
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = defaults.LeaseTTL
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = defaults.MaxAttempts
	}
	if cfg.ProcessedRetentionDays <= 0 {
		cfg.ProcessedRetentionDays = defaults.ProcessedRetentionDays
	}
	if cfg.DeadLetterRetentionDays <= 0 {
		cfg.DeadLetterRetentionDays = defaults.DeadLetterRetentionDays
	}
	cfg.ClaimedBy = strings.TrimSpace(cfg.ClaimedBy)
	if cfg.ClaimedBy == "" {
		cfg.ClaimedBy = "event-inbox/" + uuid.NewString()
	}
	return cfg
}

func (cfg Config) observeLoopTick() {
	if cfg.OnLoopTick != nil {
		cfg.OnLoopTick()
	}
}

func (cfg Config) observeClaimAttempt() {
	if cfg.OnClaimAttempt != nil {
		cfg.OnClaimAttempt()
	}
}

type sourceChannelKey struct {
	sourceKind string
	sourceName string
	channel    string
}
