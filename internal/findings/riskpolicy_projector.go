package findings

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

const RiskpolicyEventShadowSource = "riskpolicy_event_shadow"

type RiskpolicyProjector struct {
	q *gen.Queries
}

type RiskpolicyProjectionParams struct {
	Since  time.Time
	Until  time.Time
	Limit  int32
	Shadow *bool
}

type RiskpolicyProjectionResult struct {
	Projected int
	Skipped   int
}

func NewRiskpolicyProjector(q *gen.Queries) *RiskpolicyProjector {
	return &RiskpolicyProjector{q: q}
}

func (p *RiskpolicyProjector) ProjectEventShadowFindings(ctx context.Context, params RiskpolicyProjectionParams) (RiskpolicyProjectionResult, error) {
	if p == nil || p.q == nil {
		return RiskpolicyProjectionResult{}, errors.New("riskpolicy findings projector is not configured")
	}
	if params.Limit <= 0 {
		params.Limit = 1000
	}
	rows, err := p.q.ListRiskpolicyEventShadowSignalsForFindingProjection(ctx, gen.ListRiskpolicyEventShadowSignalsForFindingProjectionParams{
		Since:     optionalTimestamptz(params.Since),
		Until:     optionalTimestamptz(params.Until),
		LimitRows: params.Limit,
	})
	if err != nil {
		return RiskpolicyProjectionResult{}, err
	}
	shadow := true
	if params.Shadow != nil {
		shadow = *params.Shadow
	}
	result := RiskpolicyProjectionResult{}
	for _, row := range rows {
		// PHASE-TWO-DELETE: keep writing riskpolicy_findings for shadow/parity comparison until generic findings fully own riskpolicy output.
		if err := p.q.UpsertRiskpolicyFinding(ctx, gen.UpsertRiskpolicyFindingParams{
			FindingKey:        eventShadowFindingKey(row),
			Source:            RiskpolicyEventShadowSource,
			Shadow:            shadow,
			SourceKind:        strings.ToLower(strings.TrimSpace(row.SourceKind)),
			SourceName:        strings.TrimSpace(row.SourceName),
			EntityKind:        strings.TrimSpace(row.EntityKind),
			EntityID:          strings.TrimSpace(row.EntityID),
			EntityName:        strings.TrimSpace(row.EntityName),
			EventReceivedAt:   row.EventReceivedAt,
			EventID:           row.EventID,
			SignalID:          strings.TrimSpace(row.SignalID),
			PolicyPackID:      strings.TrimSpace(row.PolicyPackID),
			PolicyPackVersion: strings.TrimSpace(row.PolicyPackVersion),
			Severity:          strings.TrimSpace(row.Severity),
			Title:             strings.TrimSpace(row.Title),
			Evidence:          strings.TrimSpace(row.Evidence),
			Output:            row.Output,
		}); err != nil {
			return result, err
		}
		if err := NewWriter(p.q).Write(ctx, riskpolicyFindingResult(row)); err != nil {
			if errors.Is(err, ErrInvalidResult) {
				result.Skipped++
				continue
			}
			return result, err
		}
		result.Projected++
	}
	return result, nil
}

func riskpolicyFindingResult(row gen.ListRiskpolicyEventShadowSignalsForFindingProjectionRow) FindingResult {
	output := map[string]any{
		"legacy_signal_id":  strings.TrimSpace(row.SignalID),
		"riskpolicy_output": jsonObject(row.Output),
	}
	eventID := row.EventID.Bytes
	return FindingResult{
		Key:               eventShadowFindingKey(row),
		Status:            StatusOpen,
		BaseSeverity:      strings.TrimSpace(row.Severity),
		EffectiveSeverity: strings.TrimSpace(row.Severity),
		SeveritySource:    SeveritySourcePolicy,
		Title:             strings.TrimSpace(row.Title),
		Summary:           strings.TrimSpace(row.Evidence),
		Evidence:          strings.TrimSpace(row.Evidence),
		Source:            SourceRef{Kind: strings.ToLower(strings.TrimSpace(row.SourceKind)), Name: strings.TrimSpace(row.SourceName)},
		Scope:             ScopeRef{Kind: "event", SourceKind: strings.ToLower(strings.TrimSpace(row.SourceKind)), SourceName: strings.TrimSpace(row.SourceName)},
		Entity:            EntityRef{Kind: strings.TrimSpace(row.EntityKind), ID: strings.TrimSpace(row.EntityID), Name: strings.TrimSpace(row.EntityName)},
		Resource:          ResourceRef{Kind: "event", ID: uuidString(row.EventID), Name: strings.TrimSpace(row.Title)},
		Policy: PolicyRef{
			BundleID:      strings.TrimSpace(row.PolicyPackID),
			BundleVersion: strings.TrimSpace(row.PolicyPackVersion),
			ID:            strings.TrimSpace(row.SignalID),
			Title:         strings.TrimSpace(row.Title),
		},
		EventRef: &EventRef{
			ReceivedAt: row.EventReceivedAt.Time,
			ID:         eventID,
			Valid:      row.EventID.Valid && row.EventReceivedAt.Valid,
		},
		Output:      output,
		EvaluatedAt: row.EventReceivedAt.Time,
	}
}

func jsonObject(raw []byte) map[string]any {
	if len(raw) == 0 {
		return map[string]any{}
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil || out == nil {
		return map[string]any{}
	}
	return out
}

func eventShadowFindingKey(row gen.ListRiskpolicyEventShadowSignalsForFindingProjectionRow) string {
	seed := strings.Join([]string{
		RiskpolicyEventShadowSource,
		row.EventReceivedAt.Time.UTC().Format(time.RFC3339Nano),
		uuidString(row.EventID),
		strings.TrimSpace(row.SignalID),
	}, "\x00")
	sum := sha256.Sum256([]byte(seed))
	return RiskpolicyEventShadowSource + ":" + hex.EncodeToString(sum[:16])
}

func uuidString(id pgtype.UUID) string {
	if !id.Valid {
		return ""
	}
	return hex.EncodeToString(id.Bytes[:])
}

func optionalTimestamptz(t time.Time) pgtype.Timestamptz {
	if t.IsZero() {
		return pgtype.Timestamptz{}
	}
	return pgtype.Timestamptz{Time: t.UTC(), Valid: true}
}
