package projectors

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

const DiscoveryProjectionName = "canonical_discovery_v1"

type EventProjector struct {
	q *gen.Queries
}

type DiscoveryProjectionParams struct {
	ProjectionName       string
	SourceKind           string
	SourceName           string
	WindowStart          time.Time
	WindowEnd            time.Time
	Limit                int32
	ResumeFromCheckpoint bool
}

type DiscoveryProjectionResult struct {
	ProjectionName string
	SourceKind     string
	SourceName     string
	Projected      int
	LastReceivedAt time.Time
	ResumedFrom    time.Time
}

type DiscoveryEventKey struct {
	SourceKind      string `json:"source_kind"`
	SourceName      string `json:"source_name"`
	SignalKind      string `json:"signal_kind"`
	EventExternalID string `json:"event_external_id"`
}

type DiscoveryParityDiff struct {
	ProjectionName      string
	SourceKind          string
	SourceName          string
	BaselineCount       int
	ProjectedCount      int
	MatchingCount       int
	MissingInProjection []DiscoveryEventKey
	MissingInBaseline   []DiscoveryEventKey
}

func NewEventProjector(q *gen.Queries) *EventProjector {
	return &EventProjector{q: q}
}

func (p *EventProjector) ProjectDiscovery(ctx context.Context, params DiscoveryProjectionParams) (DiscoveryProjectionResult, error) {
	if p == nil || p.q == nil {
		return DiscoveryProjectionResult{}, errors.New("event projector is not configured")
	}
	params = normalizeDiscoveryProjectionParams(params)
	afterReceivedAt, afterEventID, err := p.discoveryProjectionCheckpoint(ctx, params)
	if err != nil {
		return DiscoveryProjectionResult{}, err
	}
	rows, err := p.q.ListCanonicalDiscoveryEventProjections(ctx, gen.ListCanonicalDiscoveryEventProjectionsParams{
		SourceKind:           params.SourceKind,
		SourceName:           params.SourceName,
		AfterEventReceivedAt: afterReceivedAt,
		AfterEventID:         afterEventID,
		WindowStart:          optionalTimestamptz(params.WindowStart),
		WindowEnd:            optionalTimestamptz(params.WindowEnd),
		LimitRows:            params.Limit,
	})
	if err != nil {
		return DiscoveryProjectionResult{}, fmt.Errorf("list canonical discovery projections: %w", err)
	}

	result := DiscoveryProjectionResult{
		ProjectionName: params.ProjectionName,
		SourceKind:     params.SourceKind,
		SourceName:     params.SourceName,
		ResumedFrom:    timeFromTimestamptz(afterReceivedAt),
	}
	var lastEventID pgtype.UUID
	var lastReceivedAt pgtype.Timestamptz
	for _, row := range rows {
		if err := p.q.UpsertShadowDiscoveryEventProjection(ctx, gen.UpsertShadowDiscoveryEventProjectionParams{
			ProjectionName:   params.ProjectionName,
			SourceKind:       row.SourceKind,
			SourceName:       row.SourceName,
			SignalKind:       row.SignalKind,
			EventExternalID:  row.EventExternalID,
			SourceAppID:      row.SourceAppID,
			SourceAppName:    row.SourceAppName,
			SourceAppDomain:  row.SourceAppDomain,
			ActorExternalID:  row.ActorExternalID,
			ActorEmail:       row.ActorEmail,
			ActorDisplayName: row.ActorDisplayName,
			ObservedAt:       row.ObservedAt,
			EventReceivedAt:  row.EventReceivedAt,
			EventID:          row.EventID,
		}); err != nil {
			return result, fmt.Errorf("upsert shadow discovery event %s/%s: %w", row.SignalKind, row.EventExternalID, err)
		}
		result.Projected++
		lastEventID = row.EventID
		lastReceivedAt = row.EventReceivedAt
	}
	if lastReceivedAt.Valid {
		result.LastReceivedAt = lastReceivedAt.Time.UTC()
	}

	if !lastReceivedAt.Valid {
		return result, nil
	}

	stats := mustJSON(map[string]any{
		"projected":              result.Projected,
		"limit":                  params.Limit,
		"resume_from_checkpoint": params.ResumeFromCheckpoint,
	})
	if err := p.q.UpsertEventProjectionCheckpoint(ctx, gen.UpsertEventProjectionCheckpointParams{
		ProjectionName:      params.ProjectionName,
		SourceKind:          params.SourceKind,
		SourceName:          params.SourceName,
		LastEventReceivedAt: lastReceivedAt,
		LastEventID:         lastEventID,
		Stats:               stats,
	}); err != nil {
		return result, fmt.Errorf("upsert event projection checkpoint: %w", err)
	}
	return result, nil
}

func (p *EventProjector) discoveryProjectionCheckpoint(ctx context.Context, params DiscoveryProjectionParams) (pgtype.Timestamptz, pgtype.UUID, error) {
	if !params.ResumeFromCheckpoint || !params.WindowStart.IsZero() {
		return pgtype.Timestamptz{}, pgtype.UUID{}, nil
	}
	checkpoint, err := p.q.GetEventProjectionCheckpoint(ctx, gen.GetEventProjectionCheckpointParams{
		ProjectionName: params.ProjectionName,
		SourceKind:     params.SourceKind,
		SourceName:     params.SourceName,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return pgtype.Timestamptz{}, pgtype.UUID{}, nil
		}
		return pgtype.Timestamptz{}, pgtype.UUID{}, fmt.Errorf("get event projection checkpoint: %w", err)
	}
	if !checkpoint.LastEventReceivedAt.Valid {
		return pgtype.Timestamptz{}, pgtype.UUID{}, nil
	}
	return checkpoint.LastEventReceivedAt, checkpoint.LastEventID, nil
}

func (p *EventProjector) DiffDiscovery(ctx context.Context, params DiscoveryProjectionParams) (DiscoveryParityDiff, error) {
	if p == nil || p.q == nil {
		return DiscoveryParityDiff{}, errors.New("event projector is not configured")
	}
	params = normalizeDiscoveryProjectionParams(params)
	shadowRows, err := p.q.ListShadowDiscoveryEventKeys(ctx, gen.ListShadowDiscoveryEventKeysParams{
		ProjectionName: params.ProjectionName,
		SourceKind:     params.SourceKind,
		SourceName:     params.SourceName,
		WindowStart:    optionalTimestamptz(params.WindowStart),
		WindowEnd:      optionalTimestamptz(params.WindowEnd),
	})
	if err != nil {
		return DiscoveryParityDiff{}, fmt.Errorf("list shadow discovery keys: %w", err)
	}
	baselineRows, err := p.q.ListBaselineDiscoveryEventKeys(ctx, gen.ListBaselineDiscoveryEventKeysParams{
		SourceKind:  params.SourceKind,
		SourceName:  params.SourceName,
		WindowStart: optionalTimestamptz(params.WindowStart),
		WindowEnd:   optionalTimestamptz(params.WindowEnd),
	})
	if err != nil {
		return DiscoveryParityDiff{}, fmt.Errorf("list baseline discovery keys: %w", err)
	}

	diff := CompareDiscoveryEventKeys(keysFromShadowRows(shadowRows), keysFromBaselineRows(baselineRows))
	diff.ProjectionName = params.ProjectionName
	diff.SourceKind = params.SourceKind
	diff.SourceName = params.SourceName

	sample := mustJSON(map[string]any{
		"missing_in_projection": sampleKeys(diff.MissingInProjection, 20),
		"missing_in_baseline":   sampleKeys(diff.MissingInBaseline, 20),
	})
	if _, err := p.q.CreateEventProjectionDiffRun(ctx, gen.CreateEventProjectionDiffRunParams{
		ProjectionName:           params.ProjectionName,
		SourceKind:               params.SourceKind,
		SourceName:               params.SourceName,
		WindowStart:              optionalTimestamptz(params.WindowStart),
		WindowEnd:                optionalTimestamptz(params.WindowEnd),
		BaselineCount:            int64(diff.BaselineCount),
		ProjectedCount:           int64(diff.ProjectedCount),
		MatchingCount:            int64(diff.MatchingCount),
		MissingInProjectionCount: int64(len(diff.MissingInProjection)),
		MissingInBaselineCount:   int64(len(diff.MissingInBaseline)),
		Sample:                   sample,
	}); err != nil {
		return diff, fmt.Errorf("record event projection diff: %w", err)
	}
	return diff, nil
}

func CompareDiscoveryEventKeys(projected, baseline []DiscoveryEventKey) DiscoveryParityDiff {
	projectedSet := make(map[DiscoveryEventKey]struct{}, len(projected))
	baselineSet := make(map[DiscoveryEventKey]struct{}, len(baseline))
	for _, key := range projected {
		projectedSet[normalizeDiscoveryEventKey(key)] = struct{}{}
	}
	for _, key := range baseline {
		baselineSet[normalizeDiscoveryEventKey(key)] = struct{}{}
	}

	diff := DiscoveryParityDiff{
		ProjectedCount: len(projectedSet),
		BaselineCount:  len(baselineSet),
	}
	for key := range baselineSet {
		if _, ok := projectedSet[key]; ok {
			diff.MatchingCount++
			continue
		}
		diff.MissingInProjection = append(diff.MissingInProjection, key)
	}
	for key := range projectedSet {
		if _, ok := baselineSet[key]; !ok {
			diff.MissingInBaseline = append(diff.MissingInBaseline, key)
		}
	}
	return diff
}

func normalizeDiscoveryProjectionParams(params DiscoveryProjectionParams) DiscoveryProjectionParams {
	params.ProjectionName = strings.TrimSpace(params.ProjectionName)
	if params.ProjectionName == "" {
		params.ProjectionName = DiscoveryProjectionName
	}
	params.SourceKind = strings.ToLower(strings.TrimSpace(params.SourceKind))
	params.SourceName = strings.TrimSpace(params.SourceName)
	if params.Limit <= 0 {
		params.Limit = 1000
	}
	return params
}

func normalizeDiscoveryEventKey(key DiscoveryEventKey) DiscoveryEventKey {
	return DiscoveryEventKey{
		SourceKind:      strings.ToLower(strings.TrimSpace(key.SourceKind)),
		SourceName:      strings.TrimSpace(key.SourceName),
		SignalKind:      strings.TrimSpace(key.SignalKind),
		EventExternalID: strings.TrimSpace(key.EventExternalID),
	}
}

func keysFromShadowRows(rows []gen.ListShadowDiscoveryEventKeysRow) []DiscoveryEventKey {
	keys := make([]DiscoveryEventKey, 0, len(rows))
	for _, row := range rows {
		keys = append(keys, DiscoveryEventKey(row))
	}
	return keys
}

func keysFromBaselineRows(rows []gen.ListBaselineDiscoveryEventKeysRow) []DiscoveryEventKey {
	keys := make([]DiscoveryEventKey, 0, len(rows))
	for _, row := range rows {
		keys = append(keys, DiscoveryEventKey(row))
	}
	return keys
}

func optionalTimestamptz(t time.Time) pgtype.Timestamptz {
	if t.IsZero() {
		return pgtype.Timestamptz{}
	}
	return pgtype.Timestamptz{Time: t.UTC(), Valid: true}
}

func timeFromTimestamptz(t pgtype.Timestamptz) time.Time {
	if !t.Valid {
		return time.Time{}
	}
	return t.Time.UTC()
}

func sampleKeys(keys []DiscoveryEventKey, limit int) []DiscoveryEventKey {
	if limit <= 0 || len(keys) <= limit {
		return keys
	}
	return keys[:limit]
}

func mustJSON(value any) []byte {
	b, err := json.Marshal(value)
	if err != nil {
		return []byte("{}")
	}
	return b
}
