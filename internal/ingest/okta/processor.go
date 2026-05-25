package oktaingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	oktaconnector "github.com/open-sspm/open-sspm/internal/connectors/okta"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/discovery"
	canonevents "github.com/open-sspm/open-sspm/internal/events"
	"github.com/open-sspm/open-sspm/internal/metrics"
	osspmsync "github.com/open-sspm/open-sspm/internal/sync"
	"github.com/open-sspm/open-sspm/internal/tail"
	"github.com/open-sspm/open-sspm/internal/timing"
)

const (
	SourceKindOktaPush = "okta_push"

	statusProcessed  = "processed"
	statusIgnored    = "ignored"
	statusDeadLetter = "dead_letter"

	defaultBatchSize       int32 = 500
	defaultPollInterval          = 5 * time.Second
	defaultCleanupInterval       = time.Hour
	defaultRetryDelay            = 30 * time.Second
	defaultRetryDelayMax         = 15 * time.Minute
	// Kill -9 recovery is lease-bound: processing rows can be reclaimed only
	// after the lease expires, then on the next stale-requeue tick.
	defaultStaleProcessingAfter          = 5 * time.Minute
	defaultLeaseTTL                      = 5 * time.Minute
	defaultHeartbeatInterval             = time.Minute
	defaultMaxAttempts             int32 = 10
	defaultProcessedRetentionDays  int32 = 30
	defaultDeadLetterRetentionDays int32 = 90
)

var errOktaPushInboxLeaseLost = errors.New("okta push inbox lease lost")

type Config struct {
	BatchSize               int32
	PollInterval            time.Duration
	CleanupInterval         time.Duration
	RetryDelay              time.Duration
	RetryDelayMax           time.Duration
	StaleProcessingAfter    time.Duration
	LeaseTTL                time.Duration
	HeartbeatInterval       time.Duration
	MaxAttempts             int32
	ProcessedRetentionDays  int32
	DeadLetterRetentionDays int32
	ClaimedBy               string
	OnLoopTick              func()
	OnClaimAttempt          func()
	OnLeaseLost             func()
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
		LeaseTTL:                defaultLeaseTTL,
		HeartbeatInterval:       defaultHeartbeatInterval,
		MaxAttempts:             defaultMaxAttempts,
		ProcessedRetentionDays:  defaultProcessedRetentionDays,
		DeadLetterRetentionDays: defaultDeadLetterRetentionDays,
	}
}

type inboxQueueDepthReporter interface {
	Depth(context.Context) (int64, error)
}

func RunLoopWithQueue(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, cfg Config, inboxQueue InboxQueue) error {
	cfg = cfg.normalized()
	queueBackend := "postgres"
	if inboxQueue != nil {
		queueBackend = "redis"
	}
	slog.Info("starting Okta push inbox processor", "interval", cfg.PollInterval, "batch_size", cfg.BatchSize, "queue_backend", queueBackend)

	runProcessorIteration(ctx, q, pool, cfg)

	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()

	staleInterval := max(cfg.StaleProcessingAfter/2, cfg.PollInterval)
	staleTicker := time.NewTicker(staleInterval)
	defer staleTicker.Stop()

	cleanupTicker := time.NewTicker(cfg.CleanupInterval)
	defer cleanupTicker.Stop()

	queueCtx, stopQueue := context.WithCancel(ctx)
	queueCh, waitQueue := startInboxQueueConsumer(queueCtx, inboxQueue, cfg)
	defer func() {
		stopQueue()
		waitQueue()
	}()
	refreshInboxQueueDepth(ctx, inboxQueue)

	for {
		select {
		case <-ctx.Done():
			return nil
		case ids, ok := <-queueCh:
			if !ok {
				queueCh = nil
				continue
			}
			runProcessorIDIteration(ctx, q, pool, ids, cfg)
			refreshInboxQueueDepth(ctx, inboxQueue)
		case <-ticker.C:
			runProcessorIteration(ctx, q, pool, cfg)
			refreshInboxQueueDepth(ctx, inboxQueue)
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
	cfg.observeClaimAttempt()
	rows, err := q.ClaimQueuedOktaPushInboxEvents(ctx, gen.ClaimQueuedOktaPushInboxEventsParams{
		LimitRows:    limit,
		ClaimedBy:    cfg.ClaimedBy,
		ClaimToken:   newClaimToken(),
		LeaseSeconds: durationSecondsCeil(cfg.LeaseTTL),
	})
	if err != nil {
		return ProcessResult{}, fmt.Errorf("claim okta push inbox rows: %w", err)
	}
	cfg.observeLoopTick()
	return processClaimedRows(ctx, q, pool, rows, cfg)
}

func ProcessQueuedIDsWithConfig(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, ids []int64, limit int32, cfg Config) (ProcessResult, error) {
	cfg = cfg.normalized()
	if len(ids) == 0 {
		return ProcessResult{}, nil
	}
	if limit <= 0 {
		limit = cfg.BatchSize
	}
	cfg.observeClaimAttempt()
	rows, err := q.ClaimQueuedOktaPushInboxEventsByIDs(ctx, gen.ClaimQueuedOktaPushInboxEventsByIDsParams{
		Ids:          ids,
		LimitRows:    limit,
		ClaimedBy:    cfg.ClaimedBy,
		ClaimToken:   newClaimToken(),
		LeaseSeconds: durationSecondsCeil(cfg.LeaseTTL),
	})
	if err != nil {
		return ProcessResult{}, fmt.Errorf("claim okta push inbox rows by id: %w", err)
	}
	cfg.observeLoopTick()
	return processClaimedRows(ctx, q, pool, rows, cfg)
}

func processClaimedRows(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, rows []gen.OktaPushInbox, cfg Config) (ProcessResult, error) {
	result := ProcessResult{Claimed: len(rows)}
	if len(rows) == 0 {
		return result, nil
	}

	claim, err := newProcessingClaim(q, rows, cfg)
	if err != nil {
		return result, err
	}
	processCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	stopHeartbeat := startProcessingLeaseHeartbeat(processCtx, claim, cfg, func(err error) {
		if errors.Is(err, errOktaPushInboxLeaseLost) {
			cfg.observeLeaseLost()
		}
		slog.Error("Okta push inbox lease heartbeat failed", "error", err)
		cancel(err)
	})
	defer stopHeartbeat()

	groups := groupInboxRowsBySource(rows)
	var errs []error
	for sourceName, group := range groups {
		groupResult, err := processSourceRows(processCtx, q, pool, sourceName, group, cfg, claim)
		result.Processed += groupResult.Processed
		result.Ignored += groupResult.Ignored
		result.DeadLetter += groupResult.DeadLetter
		if err != nil {
			if errors.Is(err, context.Canceled) {
				if cause := context.Cause(processCtx); cause != nil && !errors.Is(cause, context.Canceled) {
					err = cause
				}
			}
			errs = append(errs, err)
			if errors.Is(err, errOktaPushInboxLeaseLost) || processCtx.Err() != nil {
				break
			}
		}
	}
	return result, errors.Join(errs...)
}

type parsedInboxEvent struct {
	row   gen.OktaPushInbox
	event oktaconnector.SystemLogEvent
}

func processSourceRows(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, sourceName string, rows []gen.OktaPushInbox, cfg Config, claim *processingClaim) (ProcessResult, error) {
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

	if err := flushDeadLetter(ctx, sourceName, rows, maxAttemptIDs, "max processing attempts exceeded", "mark max-attempt okta push inbox dead-letter", claim, &result); err != nil {
		return result, err
	}
	if err := flushDeadLetter(ctx, sourceName, rows, deadLetterIDs, "invalid Okta System Log event JSON", "mark okta push inbox dead-letter", claim, &result); err != nil {
		return result, err
	}
	if err := flushIgnored(ctx, sourceName, rows, ignoredIDs, "event is not discovery or state-refresh evidence", claim, &result); err != nil {
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
		if err := claim.MarkIgnored(ctx, pgtype.Int8{}, "event did not normalize to discovery or state-refresh evidence", processedIDs); err != nil {
			return result, err
		}
		result.Ignored += len(processedIDs)
		incrementProcessedMetric(sourceName, inboxRowsFromParsed(parsed), statusIgnored)
		return result, nil
	}

	runStarted := time.Now()
	if err := claim.RenewIDs(ctx, processedIDs); err != nil {
		return result, err
	}
	runID, err := startOktaPushSyncRun(ctx, q, sourceName)
	if err != nil {
		_ = retryOrDeadLetterRows(ctx, sourceName, inboxRowsFromParsed(parsed), err, cfg, claim)
		return result, err
	}

	if err := writeOktaCanonicalEvents(ctx, pool, sourceName, parsed); err != nil {
		_ = registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
		_ = retryOrDeadLetterRows(ctx, sourceName, inboxRowsFromParsed(parsed), err, cfg, claim)
		return result, err
	}

	if err := enqueueOktaSystemLogTail(ctx, q, sourceName, len(parsed)); err != nil {
		_ = registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
		_ = retryOrDeadLetterRows(ctx, sourceName, inboxRowsFromParsed(parsed), err, cfg, claim)
		return result, err
	}

	if hasDiscoveryRows {
		if err := claim.RenewIDs(ctx, processedIDs); err != nil {
			_ = registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
			return result, err
		}
		if err := discovery.WriteRows(ctx, q, discovery.WriteRowsParams{
			SourceKind: "okta",
			SourceName: sourceName,
			RunID:      runID,
			Sources:    sources,
			Events:     normalizedEvents,
		}); err != nil {
			_ = registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
			_ = retryOrDeadLetterRows(ctx, sourceName, inboxRowsFromParsed(parsed), err, cfg, claim)
			return result, err
		}
	}

	if hasStateRefresh {
		if err := claim.RenewIDs(ctx, processedIDs); err != nil {
			_ = registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
			return result, err
		}
		if err := enqueueOktaFullSync(ctx, pool, sourceName); err != nil {
			_ = registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
			_ = retryOrDeadLetterRows(ctx, sourceName, inboxRowsFromParsed(parsed), err, cfg, claim)
			return result, err
		}
	}

	if err := claim.RenewIDs(ctx, processedIDs); err != nil {
		_ = registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
		return result, err
	}
	if err := finalizeOktaPushRun(ctx, q, pool, runID, sourceName, time.Since(runStarted), hasDiscoveryRows, refreshCounts); err != nil {
		_ = registry.FailSyncRun(ctx, q, runID, err, registry.SyncErrorKindDB)
		_ = retryOrDeadLetterRows(ctx, sourceName, inboxRowsFromParsed(parsed), err, cfg, claim)
		return result, err
	}
	if err := claim.MarkProcessed(ctx, runID, processedIDs); err != nil {
		return result, err
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
	counts := oktaStateRefreshRunCounts(refreshCounts)
	if hasDiscoveryRows {
		return registry.FinalizeDiscoveryRunWithCounts(ctx, q, pool, runID, "okta", sourceName, duration, counts)
	}
	stats := registry.MarshalJSON(map[string]any{
		"counts":      counts,
		"duration_ms": duration.Milliseconds(),
	})
	return q.MarkSyncRunSuccess(ctx, gen.MarkSyncRunSuccessParams{ID: runID, Stats: stats})
}

func oktaStateRefreshRunCounts(refreshCounts map[string]int64) map[string]int64 {
	counts := map[string]int64{}
	var total int64
	for kind, count := range refreshCounts {
		counts["state_refresh_"+kind] = count
		total += count
	}
	if total > 0 {
		counts["state_refresh_events"] = total
	}
	return counts
}

func writeOktaCanonicalEvents(ctx context.Context, pool *pgxpool.Pool, sourceName string, parsed []parsedInboxEvent) error {
	if len(parsed) == 0 {
		return nil
	}
	writer := canonevents.NewWriter(pool)
	for _, item := range parsed {
		record, err := oktaconnector.CanonicalEventRecord(sourceName, item.row.Channel, item.event)
		if err != nil {
			return err
		}
		if _, err := writer.WriteEvent(ctx, record, canonevents.WriteOptions{}); err != nil {
			return fmt.Errorf("write canonical Okta event %s: %w", item.event.ID, err)
		}
	}
	return nil
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

func enqueueOktaSystemLogTail(ctx context.Context, q *gen.Queries, sourceName string, eventCount int) error {
	scheduler := tail.NewScheduler(q)
	_, err := scheduler.Wake(ctx, tail.Wakeup{
		SourceKind: configstore.KindOkta,
		SourceName: sourceName,
		Resource:   oktaconnector.SystemLogTailResource,
		Reason:     "okta_push_wakeup",
		Priority:   10,
		Payload: map[string]any{
			"channel":     "okta_push",
			"event_count": eventCount,
		},
	})
	if err != nil {
		return fmt.Errorf("queue okta system log tail for %s: %w", sourceName, err)
	}
	return nil
}

func flushDeadLetter(ctx context.Context, sourceName string, rows []gen.OktaPushInbox, ids []int64, errMsg, wrapPrefix string, claim *processingClaim, result *ProcessResult) error {
	if len(ids) == 0 {
		return nil
	}
	if err := claim.MarkDeadLetter(ctx, errMsg, ids); err != nil {
		return fmt.Errorf("%s: %w", wrapPrefix, err)
	}
	result.DeadLetter += len(ids)
	incrementProcessedMetric(sourceName, rowsByID(rows, ids), statusDeadLetter)
	return nil
}

func flushIgnored(ctx context.Context, sourceName string, rows []gen.OktaPushInbox, ids []int64, errMsg string, claim *processingClaim, result *ProcessResult) error {
	if len(ids) == 0 {
		return nil
	}
	if err := claim.MarkIgnored(ctx, pgtype.Int8{}, errMsg, ids); err != nil {
		return err
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

func retryOrDeadLetterRows(ctx context.Context, sourceName string, rows []gen.OktaPushInbox, cause error, cfg Config, claim *processingClaim) error {
	if len(rows) == 0 {
		return nil
	}
	if errors.Is(cause, errOktaPushInboxLeaseLost) {
		// The rows are no longer ours to mark; the next claim owner will retry
		// them after requeueing under its own lease.
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
		if err := claim.MarkDeadLetter(ctx, msg, deadLetterIDs); err != nil {
			return err
		}
		incrementProcessedMetric(sourceName, rowsByID(rows, deadLetterIDs), statusDeadLetter)
	}
	for delay, ids := range retryByDelay {
		if err := claim.MarkRetry(ctx, pgtype.Timestamptz{Time: now.Add(delay), Valid: true}, msg, ids); err != nil {
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
	return timing.ExponentialBackoff(int(attempts), base, max)
}

type inboxClaim struct {
	claimedBy  string
	claimToken string
}

type processingClaim struct {
	q            *gen.Queries
	claimedBy    string
	claimToken   string
	leaseSeconds int64
	mu           sync.Mutex
	active       map[int64]struct{}
}

func newProcessingClaim(q *gen.Queries, rows []gen.OktaPushInbox, cfg Config) (*processingClaim, error) {
	claim, err := claimFromRows(rows)
	if err != nil {
		return nil, err
	}
	active := make(map[int64]struct{}, len(rows))
	for _, row := range rows {
		active[row.ID] = struct{}{}
	}
	return &processingClaim{
		q:            q,
		claimedBy:    claim.claimedBy,
		claimToken:   claim.claimToken,
		leaseSeconds: durationSecondsCeil(cfg.LeaseTTL),
		active:       active,
	}, nil
}

func claimFromRows(rows []gen.OktaPushInbox) (inboxClaim, error) {
	if len(rows) == 0 {
		return inboxClaim{}, errors.New("okta push inbox claim requires at least one row")
	}
	claim := inboxClaim{
		claimedBy:  strings.TrimSpace(rows[0].ClaimedBy.String),
		claimToken: strings.TrimSpace(rows[0].ClaimToken.String),
	}
	if !rows[0].ClaimedBy.Valid || claim.claimedBy == "" || !rows[0].ClaimToken.Valid || claim.claimToken == "" {
		return inboxClaim{}, errOktaPushInboxLeaseLost
	}
	for _, row := range rows[1:] {
		claimedBy := strings.TrimSpace(row.ClaimedBy.String)
		claimToken := strings.TrimSpace(row.ClaimToken.String)
		if !row.ClaimedBy.Valid || !row.ClaimToken.Valid || claimedBy != claim.claimedBy || claimToken != claim.claimToken {
			return inboxClaim{}, errOktaPushInboxLeaseLost
		}
	}
	return claim, nil
}

func (c *processingClaim) RenewIDs(ctx context.Context, ids []int64) error {
	ids = uniquePositiveIDs(ids)
	if len(ids) == 0 {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.renewIDsLocked(ctx, ids)
}

func (c *processingClaim) RenewActive(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	ids := c.activeIDsLocked()
	if len(ids) == 0 {
		return nil
	}
	return c.renewIDsLocked(ctx, ids)
}

func (c *processingClaim) MarkProcessed(ctx context.Context, runID int64, ids []int64) error {
	ids = uniquePositiveIDs(ids)
	if len(ids) == 0 {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.renewIDsLocked(ctx, ids); err != nil {
		return err
	}
	marked, err := c.q.MarkOktaPushInboxProcessed(ctx, gen.MarkOktaPushInboxProcessedParams{
		ProcessedRunID: runID,
		ClaimedBy:      c.claimedBy,
		ClaimToken:     c.claimToken,
		Ids:            ids,
	})
	if err != nil {
		return fmt.Errorf("mark okta push inbox processed: %w", err)
	}
	if marked != int64(len(ids)) {
		return errOktaPushInboxLeaseLost
	}
	c.releaseLocked(ids)
	return nil
}

func (c *processingClaim) MarkIgnored(ctx context.Context, processedRunID pgtype.Int8, msg string, ids []int64) error {
	ids = uniquePositiveIDs(ids)
	if len(ids) == 0 {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.renewIDsLocked(ctx, ids); err != nil {
		return err
	}
	marked, err := c.q.MarkOktaPushInboxIgnored(ctx, gen.MarkOktaPushInboxIgnoredParams{
		ProcessedRunID: processedRunID,
		ErrorMessage:   msg,
		ClaimedBy:      c.claimedBy,
		ClaimToken:     c.claimToken,
		Ids:            ids,
	})
	if err != nil {
		return fmt.Errorf("mark okta push inbox ignored: %w", err)
	}
	if marked != int64(len(ids)) {
		return errOktaPushInboxLeaseLost
	}
	c.releaseLocked(ids)
	return nil
}

func (c *processingClaim) MarkDeadLetter(ctx context.Context, msg string, ids []int64) error {
	ids = uniquePositiveIDs(ids)
	if len(ids) == 0 {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.renewIDsLocked(ctx, ids); err != nil {
		return err
	}
	marked, err := c.q.MarkOktaPushInboxDeadLetter(ctx, gen.MarkOktaPushInboxDeadLetterParams{
		ErrorMessage: msg,
		ClaimedBy:    c.claimedBy,
		ClaimToken:   c.claimToken,
		Ids:          ids,
	})
	if err != nil {
		return fmt.Errorf("mark okta push inbox dead-letter: %w", err)
	}
	if marked != int64(len(ids)) {
		return errOktaPushInboxLeaseLost
	}
	c.releaseLocked(ids)
	return nil
}

func (c *processingClaim) MarkRetry(ctx context.Context, nextAttemptAt pgtype.Timestamptz, msg string, ids []int64) error {
	ids = uniquePositiveIDs(ids)
	if len(ids) == 0 {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.renewIDsLocked(ctx, ids); err != nil {
		return err
	}
	marked, err := c.q.MarkOktaPushInboxRetry(ctx, gen.MarkOktaPushInboxRetryParams{
		NextAttemptAt: nextAttemptAt,
		ErrorMessage:  msg,
		ClaimedBy:     c.claimedBy,
		ClaimToken:    c.claimToken,
		Ids:           ids,
	})
	if err != nil {
		return fmt.Errorf("mark okta push inbox retry: %w", err)
	}
	if marked != int64(len(ids)) {
		return errOktaPushInboxLeaseLost
	}
	c.releaseLocked(ids)
	return nil
}

func (c *processingClaim) renewIDsLocked(ctx context.Context, ids []int64) error {
	if c == nil || c.q == nil {
		return errors.New("okta push inbox claim requires queries")
	}
	renewed, err := c.q.RenewOktaPushInboxProcessingLease(ctx, gen.RenewOktaPushInboxProcessingLeaseParams{
		ClaimedBy:    c.claimedBy,
		ClaimToken:   c.claimToken,
		LeaseSeconds: c.leaseSeconds,
		Ids:          ids,
	})
	if err != nil {
		return fmt.Errorf("renew okta push inbox lease: %w", err)
	}
	if renewed != int64(len(ids)) {
		return errOktaPushInboxLeaseLost
	}
	return nil
}

func (c *processingClaim) activeIDsLocked() []int64 {
	ids := make([]int64, 0, len(c.active))
	for id := range c.active {
		ids = append(ids, id)
	}
	return ids
}

func (c *processingClaim) releaseLocked(ids []int64) {
	for _, id := range ids {
		delete(c.active, id)
	}
}

func uniquePositiveIDs(ids []int64) []int64 {
	if len(ids) == 0 {
		return nil
	}
	seen := make(map[int64]struct{}, len(ids))
	out := make([]int64, 0, len(ids))
	for _, id := range ids {
		if id <= 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func newClaimToken() string {
	return uuid.NewString()
}

func startProcessingLeaseHeartbeat(ctx context.Context, claim *processingClaim, cfg Config, onLost func(error)) func() {
	if claim == nil || cfg.HeartbeatInterval <= 0 || cfg.LeaseTTL <= 0 {
		return func() {}
	}

	hbCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(cfg.HeartbeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-hbCtx.Done():
				return
			case <-ticker.C:
			}

			renewCtx, renewCancel := context.WithTimeout(hbCtx, cfg.HeartbeatInterval)
			err := claim.RenewActive(renewCtx)
			renewCancel()
			if hbCtx.Err() != nil {
				return
			}
			if err != nil {
				if onLost != nil {
					onLost(err)
				}
				cancel()
				return
			}
		}
	}()

	return func() {
		cancel()
		<-done
	}
}

func durationSecondsCeil(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64((d + time.Second - 1) / time.Second)
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

func runProcessorIDIteration(ctx context.Context, q *gen.Queries, pool *pgxpool.Pool, ids []int64, cfg Config) {
	if len(ids) == 0 {
		return
	}
	cfg = cfg.normalized()
	for len(ids) > 0 {
		if err := ctx.Err(); err != nil {
			return
		}
		end := min(int(cfg.BatchSize), len(ids))
		batch := ids[:end]
		ids = ids[end:]

		result, err := ProcessQueuedIDsWithConfig(ctx, q, pool, batch, cfg.BatchSize, cfg)
		if err != nil {
			slog.Warn("Okta push inbox redis-queued processing failed", "error", err, "claimed", result.Claimed, "processed", result.Processed, "ignored", result.Ignored, "dead_letter", result.DeadLetter)
		}
		if result.Claimed == 0 {
			continue
		}
		if err := RefreshMetrics(ctx, q); err != nil {
			slog.Warn("Okta push inbox metrics refresh failed", "error", err)
		}
	}
}

func startInboxQueueConsumer(ctx context.Context, inboxQueue InboxQueue, cfg Config) (<-chan []int64, func()) {
	if inboxQueue == nil {
		return nil, func() {}
	}
	out := make(chan []int64, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(out)
		for {
			if ctx.Err() != nil {
				return
			}
			ids, err := inboxQueue.Dequeue(ctx, cfg.BatchSize, cfg.PollInterval)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
					return
				}
				slog.Warn("Okta push inbox redis dequeue failed", "error", err)
				if !timing.SleepContext(ctx, cfg.PollInterval) {
					return
				}
				continue
			}
			if len(ids) == 0 {
				continue
			}
			select {
			case out <- ids:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, func() { <-done }
}

func refreshInboxQueueDepth(ctx context.Context, inboxQueue InboxQueue) {
	reporter, ok := inboxQueue.(inboxQueueDepthReporter)
	if !ok {
		return
	}
	depth, err := reporter.Depth(ctx)
	if err != nil {
		if ctx.Err() == nil {
			slog.Warn("Okta push inbox redis queue depth refresh failed", "error", err)
		}
		return
	}
	metrics.OktaPushRedisQueueDepth.WithLabelValues(oktaPushInboxQueueName).Set(float64(depth))
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
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = cfg.StaleProcessingAfter
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = defaults.LeaseTTL
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = defaults.HeartbeatInterval
	}
	if cfg.HeartbeatInterval >= cfg.LeaseTTL {
		cfg.HeartbeatInterval = max(cfg.LeaseTTL/3, time.Second)
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
		cfg.ClaimedBy = "okta-push-ingest/" + uuid.NewString()
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

func (cfg Config) observeLeaseLost() {
	if cfg.OnLeaseLost != nil {
		cfg.OnLeaseLost()
	}
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
