package findings

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

type Writer struct {
	q *gen.Queries
}

var ErrInvalidResult = errors.New("invalid finding result")

func NewWriter(q *gen.Queries) *Writer {
	return &Writer{q: q}
}

func (w *Writer) Write(ctx context.Context, result FindingResult) error {
	if w == nil || w.q == nil {
		return errors.New("findings writer is not configured")
	}
	result = normalizeResult(result)
	if err := validateResult(result); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidResult, err)
	}

	row, err := w.q.UpsertFinding(ctx, gen.UpsertFindingParams{
		FindingKey:          result.Key,
		Status:              string(result.Status),
		BaseSeverity:        result.BaseSeverity,
		EffectiveSeverity:   result.EffectiveSeverity,
		SeveritySource:      string(result.SeveritySource),
		Title:               result.Title,
		Summary:             result.Summary,
		Evidence:            result.Evidence,
		Remediation:         result.Remediation,
		SourceKind:          normalize(result.Source.Kind),
		SourceName:          strings.TrimSpace(result.Source.Name),
		ScopeKind:           normalize(result.Scope.Kind),
		ScopeSourceKind:     normalize(result.Scope.SourceKind),
		ScopeSourceName:     strings.TrimSpace(result.Scope.SourceName),
		EntityKind:          normalize(result.Entity.Kind),
		EntityID:            strings.TrimSpace(result.Entity.ID),
		EntityName:          strings.TrimSpace(result.Entity.Name),
		ResourceKind:        normalize(result.Resource.Kind),
		ResourceID:          strings.TrimSpace(result.Resource.ID),
		ResourceName:        strings.TrimSpace(result.Resource.Name),
		PolicyBundleID:      strings.TrimSpace(result.Policy.BundleID),
		PolicyBundleVersion: strings.TrimSpace(result.Policy.BundleVersion),
		PolicyID:            strings.TrimSpace(result.Policy.ID),
		PolicyTitle:         strings.TrimSpace(result.Policy.Title),
		RuleID:              pgTextPtr(result.Policy.RuleID),
		RulesetID:           pgTextPtr(result.Policy.RulesetID),
		EventReceivedAt:     eventReceivedAt(result.EventRef),
		EventID:             eventID(result.EventRef),
		FirstSeenAt:         pgTimestamptz(result.EvaluatedAt),
		LastSeenAt:          pgTimestamptz(result.EvaluatedAt),
		ResolvedAt:          resolvedAt(result),
		SuppressedUntil:     pgtype.Timestamptz{},
		SuppressionReason:   "",
		SuppressedBy:        "",
		SuppressedAt:        pgtype.Timestamptz{},
		Output:              mustJSON(result.Output),
	})
	if err != nil {
		return fmt.Errorf("upsert finding: %w", err)
	}

	previousStatus := strings.TrimSpace(row.PreviousStatus)
	if previousStatus == strings.TrimSpace(row.Status) {
		return nil
	}

	eventType := lifecycleEventType(row.Status)
	return w.q.InsertFindingEvent(ctx, gen.InsertFindingEventParams{
		FindingKey:            row.FindingKey,
		EventKey:              eventKey(result, eventType),
		EventType:             eventType,
		OccurredAt:            pgTimestamptz(result.EvaluatedAt),
		ActorKind:             "evaluator",
		ActorID:               strings.TrimSpace(result.Policy.BundleID),
		Message:               result.Summary,
		OldStatus:             previousStatus,
		NewStatus:             row.Status,
		OldSeverity:           strings.TrimSpace(row.PreviousEffectiveSeverity),
		NewSeverity:           row.EffectiveSeverity,
		SourceEventReceivedAt: eventReceivedAt(result.EventRef),
		SourceEventID:         eventID(result.EventRef),
		EvaluationRunID:       pgInt8Ptr(result.SyncRunID),
		SyncRunID:             pgInt8Ptr(result.SyncRunID),
		PolicyControlID:       pgtype.Int8{},
		Payload:               mustJSON(result.Output),
	})
}

func normalizeResult(result FindingResult) FindingResult {
	if strings.TrimSpace(result.Key) == "" {
		result.Key = BuildKey(result)
	}
	if result.Status == "" {
		result.Status = StatusOpen
	}
	if result.SeveritySource == "" {
		result.SeveritySource = SeveritySourcePolicy
	}
	result.BaseSeverity = strings.TrimSpace(result.BaseSeverity)
	if strings.TrimSpace(result.EffectiveSeverity) == "" {
		result.EffectiveSeverity = result.BaseSeverity
	}
	if result.EvaluatedAt.IsZero() {
		result.EvaluatedAt = time.Now()
	}
	result.EvaluatedAt = result.EvaluatedAt.UTC()
	if result.Output == nil {
		result.Output = map[string]any{}
	}
	return result
}

func validateResult(result FindingResult) error {
	if strings.TrimSpace(result.Key) == "" {
		return errors.New("finding key is required")
	}
	if strings.TrimSpace(result.Source.Kind) == "" || strings.TrimSpace(result.Source.Name) == "" {
		return errors.New("finding source kind and name are required")
	}
	if strings.TrimSpace(result.Policy.BundleID) == "" || strings.TrimSpace(result.Policy.ID) == "" {
		return errors.New("finding policy bundle and policy id are required")
	}
	return nil
}

func lifecycleEventType(status string) string {
	switch Status(strings.TrimSpace(status)) {
	case StatusResolved:
		return "resolved"
	case StatusSuppressed:
		return "suppressed"
	default:
		return "opened"
	}
}

func eventKey(result FindingResult, eventType string) string {
	return strings.Join([]string{
		result.Key,
		eventType,
		result.EvaluatedAt.Format(time.RFC3339Nano),
	}, ":")
}

func resolvedAt(result FindingResult) pgtype.Timestamptz {
	if result.Status == StatusResolved {
		return pgTimestamptz(result.EvaluatedAt)
	}
	return pgtype.Timestamptz{}
}

func pgTextPtr(value string) pgtype.Text {
	value = strings.TrimSpace(value)
	return pgtype.Text{String: value, Valid: value != ""}
}

func pgInt8Ptr(value *int64) pgtype.Int8 {
	if value == nil || *value == 0 {
		return pgtype.Int8{}
	}
	return pgtype.Int8{Int64: *value, Valid: true}
}

func pgTimestamptz(value time.Time) pgtype.Timestamptz {
	if value.IsZero() {
		return pgtype.Timestamptz{}
	}
	return pgtype.Timestamptz{Time: value.UTC(), Valid: true}
}

func eventReceivedAt(ref *EventRef) pgtype.Timestamptz {
	if ref == nil || !ref.Valid {
		return pgtype.Timestamptz{}
	}
	return pgTimestamptz(ref.ReceivedAt)
}

func eventID(ref *EventRef) pgtype.UUID {
	if ref == nil || !ref.Valid {
		return pgtype.UUID{}
	}
	return pgtype.UUID{Bytes: ref.ID, Valid: true}
}

func mustJSON(value any) []byte {
	if value == nil {
		return []byte("{}")
	}
	b, err := json.Marshal(value)
	if err != nil || len(b) == 0 {
		return []byte("{}")
	}
	return b
}
