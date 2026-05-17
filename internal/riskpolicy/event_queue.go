package riskpolicy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

const defaultEventEvaluationLease = 5 * time.Minute
const defaultEventEvaluationMaxAttempts int32 = 10

type EventQueueProcessor struct {
	q           *gen.Queries
	evaluator   *EventEvaluator
	claimedBy   string
	lease       time.Duration
	maxAttempts int32
}

type EventQueueProcessorConfig struct {
	ClaimedBy   string
	Lease       time.Duration
	MaxAttempts int32
	Evaluator   *EventEvaluator
}

type EventQueueProcessResult struct {
	Claimed   int
	Processed int
	Retried   int
	Dead      int
	LeaseLost int
	Signals   int
}

func NewEventQueueProcessor(q *gen.Queries, cfg EventQueueProcessorConfig) (*EventQueueProcessor, error) {
	if q == nil {
		return nil, errors.New("riskpolicy event queue processor requires queries")
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
		claimedBy = "riskpolicy-event/" + uuid.NewString()
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
		claimedBy:   claimedBy,
		lease:       lease,
		maxAttempts: maxAttempts,
	}, nil
}

func (p *EventQueueProcessor) ProcessQueued(ctx context.Context, limit int32) (EventQueueProcessResult, error) {
	if p == nil || p.q == nil || p.evaluator == nil {
		return EventQueueProcessResult{}, errors.New("riskpolicy event queue processor is not configured")
	}
	if limit <= 0 {
		limit = 100
	}
	rows, err := p.q.ClaimRiskpolicyEventEvaluations(ctx, gen.ClaimRiskpolicyEventEvaluationsParams{
		ClaimedBy:    p.claimedBy,
		LeaseSeconds: durationSecondsCeil(p.lease),
		LimitRows:    limit,
	})
	if err != nil {
		return EventQueueProcessResult{}, fmt.Errorf("claim riskpolicy event evaluations: %w", err)
	}
	result := EventQueueProcessResult{Claimed: len(rows)}
	for _, row := range rows {
		signals, err := p.processRow(ctx, row)
		if err != nil {
			dead, marked, markErr := p.markFailedRow(ctx, row, err)
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
		updated, err := p.q.MarkRiskpolicyEventEvaluationProcessed(ctx, gen.MarkRiskpolicyEventEvaluationProcessedParams{
			ID:        row.ID,
			ClaimedBy: p.claimedBy,
		})
		if err != nil {
			return result, fmt.Errorf("mark riskpolicy event evaluation processed: %w", err)
		}
		if updated == 0 {
			result.LeaseLost++
			continue
		}
		result.Processed++
		result.Signals += signals
	}
	return result, nil
}

func (p *EventQueueProcessor) markFailedRow(ctx context.Context, row gen.RiskpolicyEventQueue, cause error) (bool, bool, error) {
	lastError := truncateQueueError(cause)
	if row.Attempts >= p.maxAttempts {
		updated, markErr := p.q.MarkRiskpolicyEventEvaluationDead(ctx, gen.MarkRiskpolicyEventEvaluationDeadParams{
			LastError: lastError,
			ID:        row.ID,
			ClaimedBy: p.claimedBy,
		})
		if markErr != nil {
			return false, false, fmt.Errorf("mark riskpolicy event evaluation dead: %w", markErr)
		}
		return true, updated > 0, nil
	}
	updated, markErr := p.q.MarkRiskpolicyEventEvaluationRetry(ctx, gen.MarkRiskpolicyEventEvaluationRetryParams{
		AvailableAt: pgtype.Timestamptz{Time: time.Now().Add(time.Minute).UTC(), Valid: true},
		LastError:   lastError,
		ID:          row.ID,
		ClaimedBy:   p.claimedBy,
	})
	if markErr != nil {
		return false, false, fmt.Errorf("mark riskpolicy event evaluation retry: %w", markErr)
	}
	return false, updated > 0, nil
}

func (p *EventQueueProcessor) processRow(ctx context.Context, row gen.RiskpolicyEventQueue) (int, error) {
	event, err := p.q.GetEventForRiskpolicyEvaluation(ctx, gen.GetEventForRiskpolicyEvaluationParams{
		ReceivedAt: row.EventReceivedAt,
		ID:         row.EventID,
	})
	if err != nil {
		return 0, fmt.Errorf("load event for riskpolicy evaluation: %w", err)
	}
	result, err := p.evaluator.Evaluate(eventInputFromDB(event))
	if err != nil {
		return 0, err
	}
	for _, signal := range result.Signals {
		output := mustRiskpolicyJSON(map[string]any{
			"signal": signal,
			"shadow": true,
		})
		if err := p.q.UpsertRiskpolicyEventShadowSignal(ctx, gen.UpsertRiskpolicyEventShadowSignalParams{
			EventReceivedAt:   row.EventReceivedAt,
			EventID:           row.EventID,
			SignalID:          signal.ID,
			PolicyPackID:      signal.PolicyPackID,
			PolicyPackVersion: signal.PolicyPackVersion,
			Severity:          signal.Severity,
			Title:             signal.Title,
			Evidence:          signal.Evidence,
			Output:            output,
		}); err != nil {
			return 0, fmt.Errorf("upsert riskpolicy event shadow signal %s: %w", signal.ID, err)
		}
	}
	return len(result.Signals), nil
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

func mustRiskpolicyJSON(value any) []byte {
	b, err := json.Marshal(value)
	if err != nil {
		return []byte("{}")
	}
	return b
}
