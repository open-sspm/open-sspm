package oktaingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	oktaconnector "github.com/open-sspm/open-sspm/internal/connectors/okta"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/discovery"
	"github.com/open-sspm/open-sspm/internal/metrics"
	osspmsync "github.com/open-sspm/open-sspm/internal/sync"
)

const (
	SourceKindOktaPush = "okta_push"

	statusProcessed  = "processed"
	statusIgnored    = "ignored"
	statusDeadLetter = "dead_letter"

	defaultBatchSize               int32 = 500
	defaultPollInterval                  = 5 * time.Second
	defaultCleanupInterval               = time.Hour
	defaultRetryDelay                    = 30 * time.Second
	defaultRetryDelayMax                 = 15 * time.Minute
	defaultStaleProcessingAfter          = 5 * time.Minute
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
	StaleProcessingAfter    time.Duration
	MaxAttempts             int32
	ProcessedRetentionDays  int32
	DeadLetterRetentionDays int32
}

type ProcessResult struct {
	Claimed    int
	Processed  int
	Ignored    int
	DeadLetter int
}

func DefaultConfig() Config {
	return Config{
		BatchSize:               defaultBatchSize,
		PollInterval:            defaultPollInterval,
		CleanupInterval:         defaultCleanupInterval,
		RetryDelay:              defaultRetryDelay,
		RetryDelayMax:           defaultRetryDelayMax,
		StaleProcessingAfter:    defaultStaleProcessingAfter,
		MaxAttempts:             defaultMaxAttempts,
		ProcessedRetentionDays:  defaultProcessedRetentionDays,
		DeadLetterRetentionDays: defaultDeadLetterRetentionDays,
	}
}

func RunLoop(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, cfg Config) error {
	cfg = cfg.normalized()
	slog.Info("starting Okta push inbox processor", "interval", cfg.PollInterval, "batch_size", cfg.BatchSize)

	runProcessorIteration(ctx, q, pool, cfg)

	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()

	staleInterval := max(cfg.StaleProcessingAfter/2, cfg.PollInterval)
	staleTicker := time.NewTicker(staleInterval)
	defer staleTicker.Stop()

	cleanupTicker := time.NewTicker(cfg.CleanupInterval)
	defer cleanupTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			runProcessorIteration(ctx, q, pool, cfg)
		case <-staleTicker.C:
			if err := requeueStaleProcessingRows(ctx, q, cfg); err != nil {
				slog.Warn("Okta push inbox stale-row recovery failed", "error", err)
			}
		case <-cleanupTicker.C:
			if err := cleanup(ctx, q, cfg); err != nil {
				slog.Warn("Okta push inbox cleanup failed", "error", err)
			}
			if err := RefreshMetrics(ctx, q); err != nil {
				slog.Warn("Okta push inbox metrics refresh failed", "error", err)
			}
		}
	}
}

func ProcessQueued(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, limit int32) (ProcessResult, error) {
	return ProcessQueuedWithConfig(ctx, q, pool, limit, DefaultConfig())
}

func ProcessQueuedWithConfig(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, limit int32, cfg Config) (ProcessResult, error) {
	cfg = cfg.normalized()
	if limit <= 0 {
		limit = cfg.BatchSize
	}
	rows, err := q.ClaimQueuedOktaPushInboxEvents(ctx, limit)
	if err != nil {
		return ProcessResult{}, fmt.Errorf("claim okta push inbox rows: %w", err)
	}
	result := ProcessResult{Claimed: len(rows)}
	if len(rows) == 0 {
		return result, nil
	}

	groups := groupInboxRowsBySource(rows)
	var errs []error
	for sourceName, group := range groups {
		groupResult, err := processSourceRows(ctx, q, pool, sourceName, group, cfg)
		result.Processed += groupResult.Processed
		result.Ignored += groupResult.Ignored
		result.DeadLetter += groupResult.DeadLetter
		if err != nil {
			errs = append(errs, err)
		}
	}
	return result, errors.Join(errs...)
}

type parsedInboxEvent struct {
	row   gen.OktaPushInbox
	event oktaconnector.SystemLogEvent
}

func processSourceRows(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, sourceName string, rows []gen.OktaPushInbox, cfg Config) (ProcessResult, error) {
	var result ProcessResult
	started := time.Now()
	defer observeProcessingDuration(sourceName, rows, started)

	parsed := make([]parsedInboxEvent, 0, len(rows))
	ignoredIDs := make([]int64, 0)
	deadLetterIDs := make([]int64, 0)
	maxAttemptIDs := make([]int64, 0)

	for _, row := range rows {
		if cfg.MaxAttempts > 0 && row.Attempts > cfg.MaxAttempts {
			maxAttemptIDs = append(maxAttemptIDs, row.ID)
			continue
		}
		event, err := oktaconnector.MapSystemLogEventJSON(row.RawJson)
		if err != nil {
			deadLetterIDs = append(deadLetterIDs, row.ID)
			continue
		}
		if !oktaconnector.ShouldIngestPushEvent(event) {
			ignoredIDs = append(ignoredIDs, row.ID)
			continue
		}
		parsed = append(parsed, parsedInboxEvent{row: row, event: event})
	}

	if err := flushDeadLetter(ctx, q, sourceName, rows, maxAttemptIDs, "max processing attempts exceeded", "mark max-attempt okta push inbox dead-letter", &result); err != nil {
		return result, err
	}
	if err := flushDeadLetter(ctx, q, sourceName, rows, deadLetterIDs, "invalid Okta System Log event JSON", "mark okta push inbox dead-letter", &result); err != nil {
		return result, err
	}
	if err := flushIgnored(ctx, q, sourceName, rows, ignoredIDs, "event is not discovery or state-refresh evidence", &result); err != nil {
		return result, err
	}
	if len(parsed) == 0 {
		return result, nil
	}

	discoveryEvents := make([]oktaconnector.SystemLogEvent, 0, len(parsed))
	processedIDs := make([]int64, 0, len(parsed))
	refreshCounts := make(map[string]int64)
	for _, item := range parsed {
		if _, ok := oktaconnector.DiscoverySignalKind(item.event); ok {
			discoveryEvents = append(discoveryEvents, item.event)
		}
		if kind, ok := oktaconnector.StateRefreshSignalKind(item.event); ok {
			refreshCounts[kind]++
		}
		processedIDs = append(processedIDs, item.row.ID)
	}

	sources, normalizedEvents := oktaconnector.NormalizeDiscoveryEvents(discoveryEvents, sourceName, time.Now().UTC())
	hasDiscoveryRows := len(sources) > 0 || len(normalizedEvents) > 0
	hasStateRefresh := len(refreshCounts) > 0
	if !hasDiscoveryRows && !hasStateRefresh {
		if _, err := q.MarkOktaPushInboxIgnored(ctx, gen.MarkOktaPushInboxIgnoredParams{
			ProcessedRunID: pgtype.Int8{},
			ErrorMessage:   "event did not normalize to discovery or state-refresh evidence",
			Ids:            processedIDs,
		}); err != nil {
			return result, fmt.Errorf("mark okta push inbox ignored: %w", err)
		}
		result.Ignored += len(processedIDs)
		incrementProcessedMetric(sourceName, inboxRowsFromParsed(parsed), statusIgnored)
		return result, nil
	}

	runStarted := time.Now()
	runID, err := startOktaPushSyncRun(ctx, q, sourceName)
	if err != nil {
		_ = retryOrDeadLetterRows(ctx, q, sourceName, inboxRowsFromParsed(parsed), err, cfg)
		return result, err
	}

	if hasDiscoveryRows {
		if err := discovery.WriteRows(ctx, q, discovery.WriteRowsParams{
			SourceKind: "okta",
			SourceName: sourceName,
			RunID:      runID,
			Sources:    sources,
			Events:     normalizedEvents,
		}); err != nil {
			_ = registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
			_ = retryOrDeadLetterRows(ctx, q, sourceName, inboxRowsFromParsed(parsed), err, cfg)
			return result, err
		}
	}

	if hasStateRefresh {
		if err := enqueueOktaFullSync(ctx, pool, sourceName); err != nil {
			_ = registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
			_ = retryOrDeadLetterRows(ctx, q, sourceName, inboxRowsFromParsed(parsed), err, cfg)
			return result, err
		}
	}

	if err := finalizeOktaPushRun(ctx, q, pool, runID, sourceName, time.Since(runStarted), hasDiscoveryRows, refreshCounts); err != nil {
		_ = registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
		_ = retryOrDeadLetterRows(ctx, q, sourceName, inboxRowsFromParsed(parsed), err, cfg)
		return result, err
	}
	if _, err := q.MarkOktaPushInboxProcessed(ctx, gen.MarkOktaPushInboxProcessedParams{
		ProcessedRunID: runID,
		Ids:            processedIDs,
	}); err != nil {
		return result, fmt.Errorf("mark okta push inbox processed: %w", err)
	}
	result.Processed += len(processedIDs)
	incrementProcessedMetric(sourceName, inboxRowsFromParsed(parsed), statusProcessed)
	return result, nil
}

func startOktaPushSyncRun(ctx context.Context, q *gen.Queries, sourceName string) (int64, error) {
	if q == nil {
		return 0, fmt.Errorf("sync run start could not be persisted: queries is nil")
	}
	sourceName = strings.TrimSpace(sourceName)
	if sourceName == "" {
		return 0, fmt.Errorf("sync run start requires source name")
	}
	runID, err := q.CreateSyncRun(ctx, gen.CreateSyncRunParams{
		SourceKind: SourceKindOktaPush,
		SourceName: sourceName,
	})
	if err != nil {
		return 0, fmt.Errorf("create sync run for %s/%s: %w", SourceKindOktaPush, sourceName, err)
	}
	return runID, nil
}

func finalizeOktaPushRun(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, runID int64, sourceName string, duration time.Duration, hasDiscoveryRows bool, refreshCounts map[string]int64) error {
	if hasDiscoveryRows {
		return registry.FinalizeDiscoveryRun(ctx, q, pool, runID, "okta", sourceName, duration)
	}
	counts := map[string]int64{}
	var total int64
	for kind, count := range refreshCounts {
		counts["state_refresh_"+kind] = count
		total += count
	}
	counts["state_refresh_events"] = total
	stats := registry.MarshalJSON(map[string]any{
		"counts":      counts,
		"duration_ms": duration.Milliseconds(),
	})
	return q.MarkSyncRunSuccess(ctx, gen.MarkSyncRunSuccessParams{ID: runID, Stats: stats})
}

func enqueueOktaFullSync(ctx context.Context, pool *pgxpool.Pool, sourceName string) error {
	if pool == nil {
		return fmt.Errorf("queue okta full sync: pool is nil")
	}
	store := osspmsync.NewSyncJobStore(pool)
	runner := osspmsync.NewResyncQueueRunnerWithPlanner(store, nil, registry.RunModeFull)
	err := runner.RunOnce(osspmsync.WithConnectorScope(ctx, configstore.KindOkta, sourceName))
	switch {
	case err == nil:
		return nil
	case errors.Is(err, osspmsync.ErrSyncQueued), errors.Is(err, osspmsync.ErrSyncAlreadyRunning):
		return nil
	default:
		return fmt.Errorf("queue okta full sync for %s: %w", sourceName, err)
	}
}

func flushDeadLetter(ctx context.Context, q *gen.Queries, sourceName string, rows []gen.OktaPushInbox, ids []int64, errMsg, wrapPrefix string, result *ProcessResult) error {
	if len(ids) == 0 {
		return nil
	}
	if _, err := q.MarkOktaPushInboxDeadLetter(ctx, gen.MarkOktaPushInboxDeadLetterParams{
		ErrorMessage: errMsg,
		Ids:          ids,
	}); err != nil {
		return fmt.Errorf("%s: %w", wrapPrefix, err)
	}
	result.DeadLetter += len(ids)
	incrementProcessedMetric(sourceName, rowsByID(rows, ids), statusDeadLetter)
	return nil
}

func flushIgnored(ctx context.Context, q *gen.Queries, sourceName string, rows []gen.OktaPushInbox, ids []int64, errMsg string, result *ProcessResult) error {
	if len(ids) == 0 {
		return nil
	}
	if _, err := q.MarkOktaPushInboxIgnored(ctx, gen.MarkOktaPushInboxIgnoredParams{
		ProcessedRunID: pgtype.Int8{},
		ErrorMessage:   errMsg,
		Ids:            ids,
	}); err != nil {
		return fmt.Errorf("mark okta push inbox ignored: %w", err)
	}
	result.Ignored += len(ids)
	incrementProcessedMetric(sourceName, rowsByID(rows, ids), statusIgnored)
	return nil
}

func groupInboxRowsBySource(rows []gen.OktaPushInbox) map[string][]gen.OktaPushInbox {
	groups := make(map[string][]gen.OktaPushInbox)
	for _, row := range rows {
		sourceName := strings.TrimSpace(row.SourceName)
		if sourceName == "" {
			continue
		}
		groups[sourceName] = append(groups[sourceName], row)
	}
	return groups
}

func retryOrDeadLetterRows(ctx context.Context, q *gen.Queries, sourceName string, rows []gen.OktaPushInbox, cause error, cfg Config) error {
	if len(rows) == 0 {
		return nil
	}
	msg := "processing failed"
	if cause != nil && strings.TrimSpace(cause.Error()) != "" {
		msg = cause.Error()
	}
	retryByDelay := make(map[time.Duration][]int64)
	deadLetterIDs := make([]int64, 0)
	now := time.Now().UTC()
	for _, row := range rows {
		if cfg.MaxAttempts > 0 && row.Attempts >= cfg.MaxAttempts {
			deadLetterIDs = append(deadLetterIDs, row.ID)
			continue
		}
		delay := backoffDelay(row.Attempts, cfg.RetryDelay, cfg.RetryDelayMax)
		retryByDelay[delay] = append(retryByDelay[delay], row.ID)
	}
	if len(deadLetterIDs) > 0 {
		if _, err := q.MarkOktaPushInboxDeadLetter(ctx, gen.MarkOktaPushInboxDeadLetterParams{
			ErrorMessage: msg,
			Ids:          deadLetterIDs,
		}); err != nil {
			return err
		}
		incrementProcessedMetric(sourceName, rowsByID(rows, deadLetterIDs), statusDeadLetter)
	}
	for delay, ids := range retryByDelay {
		if _, err := q.MarkOktaPushInboxRetry(ctx, gen.MarkOktaPushInboxRetryParams{
			NextAttemptAt: pgtype.Timestamptz{Time: now.Add(delay), Valid: true},
			ErrorMessage:  msg,
			Ids:           ids,
		}); err != nil {
			return err
		}
	}
	return nil
}

// backoffDelay returns the delay before the next retry for a row that has
// already been claimed `attempts` times. The first failed attempt waits `base`,
// each subsequent attempt doubles the delay, capped at `max`.
func backoffDelay(attempts int32, base, max time.Duration) time.Duration {
	if base <= 0 {
		base = defaultRetryDelay
	}
	if max <= 0 {
		max = base
	}
	if max < base {
		max = base
	}
	count := int(attempts)
	if count < 1 {
		count = 1
	}
	delay := base
	for idx := 1; idx < count; idx++ {
		if delay >= max {
			return max
		}
		if delay > max/2 {
			return max
		}
		delay *= 2
	}
	if delay > max {
		return max
	}
	return delay
}

func runProcessorIteration(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, cfg Config) {
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		result, err := ProcessQueuedWithConfig(ctx, q, pool, cfg.BatchSize, cfg)
		if err != nil {
			slog.Warn("Okta push inbox processing failed", "error", err, "claimed", result.Claimed, "processed", result.Processed, "ignored", result.Ignored, "dead_letter", result.DeadLetter)
		}
		if result.Claimed == 0 {
			return
		}
		if err := RefreshMetrics(ctx, q); err != nil {
			slog.Warn("Okta push inbox metrics refresh failed", "error", err)
		}
		if int32(result.Claimed) < cfg.BatchSize {
			return
		}
	}
}

func requeueStaleProcessingRows(ctx context.Context, q *gen.Queries, cfg Config) error {
	if cfg.StaleProcessingAfter <= 0 {
		return nil
	}
	_, err := q.RequeueStaleOktaPushInboxProcessingRows(ctx, int32(cfg.StaleProcessingAfter/time.Second))
	return err
}

func cleanup(ctx context.Context, q *gen.Queries, cfg Config) error {
	_, err := q.DeleteOldOktaPushInboxRows(ctx, gen.DeleteOldOktaPushInboxRowsParams{
		ProcessedRetentionDays:  cfg.ProcessedRetentionDays,
		DeadLetterRetentionDays: cfg.DeadLetterRetentionDays,
	})
	return err
}

func RefreshMetrics(ctx context.Context, q *gen.Queries) error {
	rows, err := q.ListOktaPushInboxMetricsBySourceChannel(ctx)
	if err != nil {
		return err
	}
	seen := make(map[sourceChannelKey]struct{}, len(rows))
	for _, row := range rows {
		key := sourceChannelKey{sourceName: row.SourceName, channel: row.Channel}
		seen[key] = struct{}{}
		metrics.OktaPushQueueDepth.WithLabelValues(row.SourceName, row.Channel).Set(float64(row.QueuedCount))
		metrics.OktaPushDeadLetterRows.WithLabelValues(row.SourceName, row.Channel).Set(float64(row.DeadLetterCount))
		if row.LastReceivedAt.Valid {
			metrics.OktaPushLastReceivedTimestamp.WithLabelValues(row.SourceName, row.Channel).Set(float64(row.LastReceivedAt.Time.Unix()))
		}
		if row.LastProcessedAt.Valid {
			metrics.OktaPushLastProcessedTimestamp.WithLabelValues(row.SourceName, row.Channel).Set(float64(row.LastProcessedAt.Time.Unix()))
		}
	}
	pruneStaleOktaPushMetrics(seen)
	return nil
}

var (
	oktaPushMetricLabelsMu sync.Mutex
	oktaPushMetricLabels   = map[sourceChannelKey]struct{}{}
)

// pruneStaleOktaPushMetrics deletes gauge time series whose underlying inbox
// rows have all been removed by retention cleanup. Without this, the last
// observed values would persist forever and dashboards would lie about queue
// state for retired sources or channels.
func pruneStaleOktaPushMetrics(seen map[sourceChannelKey]struct{}) {
	oktaPushMetricLabelsMu.Lock()
	defer oktaPushMetricLabelsMu.Unlock()
	for key := range oktaPushMetricLabels {
		if _, ok := seen[key]; ok {
			continue
		}
		metrics.OktaPushQueueDepth.DeleteLabelValues(key.sourceName, key.channel)
		metrics.OktaPushDeadLetterRows.DeleteLabelValues(key.sourceName, key.channel)
		metrics.OktaPushLastReceivedTimestamp.DeleteLabelValues(key.sourceName, key.channel)
		metrics.OktaPushLastProcessedTimestamp.DeleteLabelValues(key.sourceName, key.channel)
	}
	oktaPushMetricLabels = seen
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
	if cfg.StaleProcessingAfter <= 0 {
		cfg.StaleProcessingAfter = defaults.StaleProcessingAfter
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
	return cfg
}

func inboxRowsFromParsed(parsed []parsedInboxEvent) []gen.OktaPushInbox {
	rows := make([]gen.OktaPushInbox, 0, len(parsed))
	for _, item := range parsed {
		rows = append(rows, item.row)
	}
	return rows
}

func rowsByID(rows []gen.OktaPushInbox, ids []int64) []gen.OktaPushInbox {
	if len(rows) == 0 || len(ids) == 0 {
		return nil
	}
	wanted := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		wanted[id] = struct{}{}
	}
	matched := make([]gen.OktaPushInbox, 0, len(ids))
	for _, row := range rows {
		if _, ok := wanted[row.ID]; ok {
			matched = append(matched, row)
		}
	}
	return matched
}

func incrementProcessedMetric(sourceName string, rows []gen.OktaPushInbox, status string) {
	counts := countRowsBySourceAndChannel(sourceName, rows)
	for key, count := range counts {
		metrics.OktaPushEventsProcessedTotal.WithLabelValues(key.sourceName, key.channel, status).Add(float64(count))
	}
}

func observeProcessingDuration(sourceName string, rows []gen.OktaPushInbox, started time.Time) {
	if len(rows) == 0 || started.IsZero() {
		return
	}
	counts := countRowsBySourceAndChannel(sourceName, rows)
	duration := time.Since(started).Seconds()
	for key := range counts {
		metrics.OktaPushProcessingDuration.WithLabelValues(key.sourceName, key.channel).Observe(duration)
	}
}

type sourceChannelKey struct {
	sourceName string
	channel    string
}

func countRowsBySourceAndChannel(defaultSourceName string, rows []gen.OktaPushInbox) map[sourceChannelKey]int {
	counts := make(map[sourceChannelKey]int)
	for _, row := range rows {
		sourceName := strings.TrimSpace(row.SourceName)
		if sourceName == "" {
			sourceName = strings.TrimSpace(defaultSourceName)
		}
		channel := strings.TrimSpace(row.Channel)
		if channel == "" {
			channel = "unknown"
		}
		if sourceName == "" {
			sourceName = "unknown"
		}
		counts[sourceChannelKey{sourceName: sourceName, channel: channel}]++
	}
	return counts
}
