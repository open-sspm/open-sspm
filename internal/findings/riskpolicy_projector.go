package findings

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	Shadow bool
}

type RiskpolicyProjectionResult struct {
	Projected int
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
	shadow := params.Shadow
	if !params.Shadow {
		shadow = true
	}
	result := RiskpolicyProjectionResult{}
	for _, row := range rows {
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
		result.Projected++
	}
	return result, nil
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
