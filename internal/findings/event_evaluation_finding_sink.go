package findings

import (
	"context"
	"encoding/hex"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/evaluator"
)

const EventEvaluationSource = "event_evaluation"

type EventEvaluationFindingSink struct {
	writer *Writer
}

func NewEventEvaluationFindingSink(q *gen.Queries) *EventEvaluationFindingSink {
	return &EventEvaluationFindingSink{writer: NewWriter(q)}
}

func (s *EventEvaluationFindingSink) WriteEventSignal(ctx context.Context, signal evaluator.EventSignal) (bool, error) {
	if s == nil || s.writer == nil {
		return false, errors.New("event evaluation finding sink is not configured")
	}
	if err := s.writer.Write(ctx, eventEvaluationFindingResult(signal)); err != nil {
		if errors.Is(err, ErrInvalidResult) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func eventEvaluationFindingResult(signal evaluator.EventSignal) FindingResult {
	output := map[string]any{
		"signal_id":         strings.TrimSpace(signal.Signal.ID),
		"evaluation_output": JSONObject(signal.Output),
	}
	eventID := signal.EventID.Bytes
	return FindingResult{
		Key:               eventEvaluationFindingKey(signal),
		Status:            StatusOpen,
		BaseSeverity:      strings.TrimSpace(signal.Signal.Severity),
		EffectiveSeverity: strings.TrimSpace(signal.Signal.Severity),
		SeveritySource:    SeveritySourcePolicy,
		Title:             strings.TrimSpace(signal.Signal.Title),
		Summary:           strings.TrimSpace(signal.Signal.Evidence),
		Evidence:          strings.TrimSpace(signal.Signal.Evidence),
		Source:            SourceRef{Kind: strings.ToLower(strings.TrimSpace(signal.SourceKind)), Name: strings.TrimSpace(signal.SourceName)},
		Scope:             ScopeRef{Kind: "event", SourceKind: strings.ToLower(strings.TrimSpace(signal.SourceKind)), SourceName: strings.TrimSpace(signal.SourceName)},
		Entity:            EntityRef{Kind: strings.TrimSpace(signal.EntityKind), ID: strings.TrimSpace(signal.EntityID), Name: strings.TrimSpace(signal.EntityName)},
		Resource:          ResourceRef{Kind: "event", ID: uuidString(signal.EventID), Name: strings.TrimSpace(signal.Signal.Title)},
		Policy: PolicyRef{
			BundleID:      strings.TrimSpace(signal.Signal.PolicyPackID),
			BundleVersion: strings.TrimSpace(signal.Signal.PolicyPackVersion),
			ID:            strings.TrimSpace(signal.Signal.ID),
			Title:         strings.TrimSpace(signal.Signal.Title),
		},
		EventRef: &EventRef{
			ReceivedAt: signal.EventReceivedAt.Time,
			ID:         eventID,
			Valid:      signal.EventID.Valid && signal.EventReceivedAt.Valid,
		},
		Output:      output,
		EvaluatedAt: signal.EvaluatedAt,
	}
}

func eventEvaluationFindingKey(signal evaluator.EventSignal) string {
	return strings.Join([]string{
		EventEvaluationSource,
		uuidString(signal.EventID),
		strings.TrimSpace(signal.Signal.ID),
	}, ":")
}

func uuidString(id pgtype.UUID) string {
	if !id.Valid {
		return ""
	}
	return hex.EncodeToString(id.Bytes[:])
}
