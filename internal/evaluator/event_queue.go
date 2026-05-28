package evaluator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

const defaultEventEvaluationLease = 5 * time.Minute
const defaultEventEvaluationMaxAttempts int32 = 10

var errEventEvaluationLeaseLost = errors.New("event evaluation lease lost")

type EventQueueProcessor struct {
	q           *gen.Queries
	evaluator   *EventEvaluator
	signalSink  EventSignalSink
	claimedBy   string
	lease       time.Duration
	maxAttempts int32
}

type EventQueueProcessorConfig struct {
	ClaimedBy   string
	Lease       time.Duration
	MaxAttempts int32
	Evaluator   *EventEvaluator
	SignalSink  EventSignalSink
}

type EventQueueProcessResult struct {
	Claimed   int
	Processed int
	Retried   int
	Dead      int
	LeaseLost int
	Signals   int
	Findings  int
}

type EventSignal struct {
	EventReceivedAt pgtype.Timestamptz
	EventID         pgtype.UUID
	SourceKind      string
	SourceName      string
	EntityKind      string
	EntityID        string
	EntityName      string
	Signal          RiskSignal
	Output          []byte
	EvaluatedAt     time.Time
}

type EventSignalSink interface {
	WriteEventSignal(context.Context, EventSignal) (bool, error)
}

type EventSignalSinkFunc func(context.Context, EventSignal) (bool, error)

func (fn EventSignalSinkFunc) WriteEventSignal(ctx context.Context, signal EventSignal) (bool, error) {
	return fn(ctx, signal)
}

func NewEventQueueProcessor(q *gen.Queries, cfg EventQueueProcessorConfig) (*EventQueueProcessor, error) {
	if q == nil {
		return nil, errors.New("event evaluator queue processor requires queries")
	}
	evaluator := cfg.Evaluator
	if evaluator == nil {
		var err error
		evaluator, err = NewEventEvaluator(nil)
		if err != nil {
			return nil, err
		}
	}
	claimedBy := strings.TrimSpace(cfg.ClaimedBy)
	if claimedBy == "" {
		claimedBy = "event-evaluator/" + uuid.NewString()
	}
	lease := cfg.Lease
	if lease <= 0 {
		lease = defaultEventEvaluationLease
	}
	maxAttempts := cfg.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = defaultEventEvaluationMaxAttempts
	}
	return &EventQueueProcessor{
		q:           q,
		evaluator:   evaluator,
		signalSink:  cfg.SignalSink,
		claimedBy:   claimedBy,
		lease:       lease,
		maxAttempts: maxAttempts,
	}, nil
}

func (p *EventQueueProcessor) ProcessQueued(ctx context.Context, limit int32) (EventQueueProcessResult, error) {
	if p == nil || p.q == nil || p.evaluator == nil {
		return EventQueueProcessResult{}, errors.New("event evaluator queue processor is not configured")
	}
	if limit <= 0 {
		limit = 100
	}
	rows, err := p.q.ClaimEventEvaluations(ctx, gen.ClaimEventEvaluationsParams{
		ClaimedBy:    p.claimedBy,
		LeaseSeconds: durationSecondsCeil(p.lease),
		LimitRows:    limit,
	})
	if err != nil {
		return EventQueueProcessResult{}, fmt.Errorf("claim event evaluations: %w", err)
	}
	result := EventQueueProcessResult{Claimed: len(rows)}
	claim := newEventEvaluationClaim(p.q, rows, p.claimedBy, p.lease)
	processCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	stopHeartbeat := startEventEvaluationLeaseHeartbeat(processCtx, claim, eventEvaluationHeartbeatInterval(p.lease), func(err error) {
		cancel(err)
	})
	defer stopHeartbeat()

	for _, row := range rows {
		if err := eventQueueContextError(processCtx); err != nil {
			return finishEventQueueLeaseStop(result, claim, err)
		}
		rowResult, err := p.processRow(processCtx, row)
		if err := eventQueueContextError(processCtx); err != nil {
			return finishEventQueueLeaseStop(result, claim, err)
		}
		if err != nil {
			dead, marked, markErr := p.markFailedRowWithClaim(processCtx, claim, row, err)
			if markErr != nil {
				return result, errors.Join(err, markErr)
			}
			if !marked {
				result.LeaseLost++
				continue
			}
			if dead {
				result.Dead++
			} else {
				result.Retried++
			}
			continue
		}
		updated, err := claim.MarkProcessed(processCtx, row.ID)
		if err != nil {
			return result, fmt.Errorf("mark event evaluation processed: %w", err)
		}
		if updated == 0 {
			result.LeaseLost++
			continue
		}
		result.Processed++
		result.Signals += rowResult.Signals
		result.Findings += rowResult.Findings
	}
	return result, nil
}

func (p *EventQueueProcessor) markFailedRow(ctx context.Context, row gen.EventEvaluationQueue, cause error) (bool, bool, error) {
	return p.markFailedRowWithClaim(ctx, nil, row, cause)
}

func (p *EventQueueProcessor) markFailedRowWithClaim(ctx context.Context, claim *eventEvaluationClaim, row gen.EventEvaluationQueue, cause error) (bool, bool, error) {
	lastError := truncateQueueError(cause)
	if row.Attempts >= p.maxAttempts {
		updated, markErr := p.markEventEvaluationDead(ctx, claim, row.ID, lastError)
		if markErr != nil {
			return false, false, fmt.Errorf("mark event evaluation dead: %w", markErr)
		}
		return true, updated > 0, nil
	}
	updated, markErr := p.markEventEvaluationRetry(ctx, claim, row.ID, time.Now().Add(time.Minute), lastError)
	if markErr != nil {
		return false, false, fmt.Errorf("mark event evaluation retry: %w", markErr)
	}
	return false, updated > 0, nil
}

func (p *EventQueueProcessor) markEventEvaluationDead(ctx context.Context, claim *eventEvaluationClaim, id int64, lastError string) (int64, error) {
	if claim != nil {
		return claim.MarkDead(ctx, id, lastError)
	}
	return p.q.MarkEventEvaluationDead(ctx, gen.MarkEventEvaluationDeadParams{
		LastError: lastError,
		ID:        id,
		ClaimedBy: p.claimedBy,
	})
}

func (p *EventQueueProcessor) markEventEvaluationRetry(ctx context.Context, claim *eventEvaluationClaim, id int64, availableAt time.Time, lastError string) (int64, error) {
	if claim != nil {
		return claim.MarkRetry(ctx, id, availableAt, lastError)
	}
	return p.q.MarkEventEvaluationRetry(ctx, gen.MarkEventEvaluationRetryParams{
		AvailableAt: pgtype.Timestamptz{Time: availableAt.UTC(), Valid: true},
		LastError:   lastError,
		ID:          id,
		ClaimedBy:   p.claimedBy,
	})
}

type eventEvaluationClaim struct {
	q         *gen.Queries
	claimedBy string
	lease     time.Duration
	mu        sync.Mutex
	active    map[int64]struct{}
}

func newEventEvaluationClaim(q *gen.Queries, rows []gen.EventEvaluationQueue, claimedBy string, lease time.Duration) *eventEvaluationClaim {
	active := make(map[int64]struct{}, len(rows))
	for _, row := range rows {
		if row.ID > 0 {
			active[row.ID] = struct{}{}
		}
	}
	return &eventEvaluationClaim{
		q:         q,
		claimedBy: strings.TrimSpace(claimedBy),
		lease:     lease,
		active:    active,
	}
}

func (c *eventEvaluationClaim) RenewActive(ctx context.Context) error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	ids := c.activeIDsLocked()
	if len(ids) == 0 {
		return nil
	}
	renewed, err := c.q.RenewEventEvaluationLease(ctx, gen.RenewEventEvaluationLeaseParams{
		Ids:          ids,
		ClaimedBy:    c.claimedBy,
		LeaseSeconds: durationSecondsCeil(c.lease),
	})
	if err != nil {
		return fmt.Errorf("renew event evaluation lease: %w", err)
	}
	if renewed != int64(len(ids)) {
		return errEventEvaluationLeaseLost
	}
	return nil
}

func (c *eventEvaluationClaim) MarkProcessed(ctx context.Context, id int64) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	updated, err := c.q.MarkEventEvaluationProcessed(ctx, gen.MarkEventEvaluationProcessedParams{
		ID:        id,
		ClaimedBy: c.claimedBy,
	})
	c.releaseLocked(id)
	return updated, err
}

func (c *eventEvaluationClaim) MarkDead(ctx context.Context, id int64, lastError string) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	updated, err := c.q.MarkEventEvaluationDead(ctx, gen.MarkEventEvaluationDeadParams{
		LastError: lastError,
		ID:        id,
		ClaimedBy: c.claimedBy,
	})
	c.releaseLocked(id)
	return updated, err
}

func (c *eventEvaluationClaim) MarkRetry(ctx context.Context, id int64, availableAt time.Time, lastError string) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	updated, err := c.q.MarkEventEvaluationRetry(ctx, gen.MarkEventEvaluationRetryParams{
		AvailableAt: pgtype.Timestamptz{Time: availableAt.UTC(), Valid: true},
		LastError:   lastError,
		ID:          id,
		ClaimedBy:   c.claimedBy,
	})
	c.releaseLocked(id)
	return updated, err
}

func (c *eventEvaluationClaim) ActiveCount() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.active)
}

func (c *eventEvaluationClaim) releaseLocked(id int64) {
	if c == nil || id <= 0 {
		return
	}
	delete(c.active, id)
}

func (c *eventEvaluationClaim) activeIDsLocked() []int64 {
	ids := make([]int64, 0, len(c.active))
	for id := range c.active {
		ids = append(ids, id)
	}
	return ids
}

func startEventEvaluationLeaseHeartbeat(ctx context.Context, claim *eventEvaluationClaim, interval time.Duration, onLost func(error)) func() {
	if claim == nil || interval <= 0 {
		return func() {}
	}
	hbCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-ticker.C:
			}
			timeout := interval
			if timeout < time.Second {
				timeout = time.Second
			}
			renewCtx, renewCancel := context.WithTimeout(hbCtx, timeout)
			err := claim.RenewActive(renewCtx)
			renewCancel()
			if hbCtx.Err() != nil {
				return
			}
			if err != nil {
				if onLost != nil {
					onLost(err)
				}
				return
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func eventEvaluationHeartbeatInterval(lease time.Duration) time.Duration {
	if lease <= 0 {
		lease = defaultEventEvaluationLease
	}
	interval := lease / 2
	if interval <= 0 {
		interval = time.Second
	}
	if interval > time.Minute {
		return time.Minute
	}
	return interval
}

func eventQueueContextError(ctx context.Context) error {
	if err := ctx.Err(); err == nil {
		return nil
	}
	if cause := context.Cause(ctx); cause != nil && !errors.Is(cause, context.Canceled) {
		return cause
	}
	return ctx.Err()
}

func finishEventQueueLeaseStop(result EventQueueProcessResult, claim *eventEvaluationClaim, err error) (EventQueueProcessResult, error) {
	if errors.Is(err, errEventEvaluationLeaseLost) {
		result.LeaseLost += claim.ActiveCount()
		return result, nil
	}
	return result, err
}

type eventQueueRowResult struct {
	Signals  int
	Findings int
}

func (p *EventQueueProcessor) processRow(ctx context.Context, row gen.EventEvaluationQueue) (eventQueueRowResult, error) {
	event, err := p.q.GetEventForEvaluation(ctx, gen.GetEventForEvaluationParams{
		ReceivedAt: row.EventReceivedAt,
		ID:         row.EventID,
	})
	if err != nil {
		return eventQueueRowResult{}, fmt.Errorf("load event for evaluation: %w", err)
	}
	result, err := p.evaluator.Evaluate(eventInputFromDB(event))
	if err != nil {
		return eventQueueRowResult{}, err
	}
	rowResult := eventQueueRowResult{Signals: len(result.Signals)}
	for _, signal := range result.Signals {
		output := mustEvaluationOutputJSON(map[string]any{
			"pipeline": "canonical_event",
			"signal":   signal,
		})
		if p.signalSink == nil {
			continue
		}
		written, err := p.signalSink.WriteEventSignal(ctx, EventSignal{
			EventReceivedAt: row.EventReceivedAt,
			EventID:         row.EventID,
			SourceKind:      event.SourceKind,
			SourceName:      event.SourceName,
			EntityKind:      eventEntityKind(event),
			EntityID:        eventEntityID(event),
			EntityName:      eventEntityName(event),
			Signal:          signal,
			Output:          output,
			EvaluatedAt:     time.Now().UTC(),
		})
		if err != nil {
			return rowResult, fmt.Errorf("write event finding %s: %w", signal.ID, err)
		}
		if written {
			rowResult.Findings++
		}
	}
	return rowResult, nil
}

func eventInputFromDB(event gen.Event) EventInput {
	return EventInput{
		SourceKind:       event.SourceKind,
		SourceName:       event.SourceName,
		Channel:          event.Channel,
		ProviderEventID:  event.ProviderEventID,
		EventType:        event.EventType,
		Category:         event.Category,
		Action:           event.Action,
		Outcome:          event.Outcome,
		Severity:         int64(event.Severity),
		ActorKind:        event.ActorKind,
		ActorID:          event.ActorID,
		ActorEmail:       event.ActorEmail,
		ActorDisplayName: event.ActorDisplayName,
		TargetKind:       event.TargetKind,
		TargetID:         event.TargetID,
		TargetName:       event.TargetName,
		OccurredAt:       timestamptzTime(event.OccurredAt),
		EvaluatedAt:      time.Now().UTC(),
	}
}

func timestamptzTime(value pgtype.Timestamptz) time.Time {
	if !value.Valid {
		return time.Time{}
	}
	return value.Time.UTC()
}

func eventEntityKind(event gen.Event) string {
	if value := strings.TrimSpace(event.TargetKind); value != "" {
		return value
	}
	return "event"
}

func eventEntityID(event gen.Event) string {
	if value := strings.TrimSpace(event.TargetID); value != "" {
		return value
	}
	return strings.TrimSpace(event.ProviderEventID)
}

func eventEntityName(event gen.Event) string {
	if value := strings.TrimSpace(event.TargetName); value != "" {
		return value
	}
	return strings.TrimSpace(event.EventType)
}

func durationSecondsCeil(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	seconds := int64(d / time.Second)
	if d%time.Second != 0 {
		seconds++
	}
	if seconds < 1 {
		return 1
	}
	return seconds
}

func truncateQueueError(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.TrimSpace(err.Error())
	const limit = 2048
	if len(msg) <= limit {
		return msg
	}
	return msg[:limit]
}

func mustEvaluationOutputJSON(value any) []byte {
	b, err := json.Marshal(value)
	if err != nil {
		return []byte("{}")
	}
	return b
}
