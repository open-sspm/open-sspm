package events

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/records"
)

type Beginner interface {
	Begin(context.Context) (pgx.Tx, error)
}

type Writer struct {
	db Beginner
}

type WriteOptions struct {
	IngestID   *int64
	ReceivedAt time.Time
}

type WriteResult struct {
	EventID    uuid.UUID
	ReceivedAt time.Time
	Inserted   bool
}

func NewWriter(db Beginner) *Writer {
	return &Writer{db: db}
}

func (w *Writer) WriteEvent(ctx context.Context, record records.EventRecord, opts WriteOptions) (WriteResult, error) {
	if w == nil || w.db == nil {
		return WriteResult{}, errors.New("event writer is not configured")
	}
	normalized, err := normalizeEventRecord(record, opts)
	if err != nil {
		return WriteResult{}, err
	}

	eventID, err := uuid.NewV7()
	if err != nil {
		return WriteResult{}, fmt.Errorf("generate event id: %w", err)
	}
	dedupeHash := EventDedupeHash(normalized.Source, normalized.DedupeKeyValue)

	tx, err := w.db.Begin(ctx)
	if err != nil {
		return WriteResult{}, fmt.Errorf("begin event write: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	q := gen.New(tx)
	dedupe, err := q.InsertEventDedupeKey(ctx, gen.InsertEventDedupeKeyParams{
		SourceKind:      normalized.SourceKind(),
		SourceID:        pgInt8Ptr(normalized.Source.ID),
		SourceName:      normalized.SourceName(),
		DedupeHash:      dedupeHash,
		DedupeKey:       normalized.DedupeKeyValue,
		EventReceivedAt: pgTimestamptz(normalized.ReceivedAt),
		EventID:         pgUUID(eventID),
	})
	if err != nil {
		return WriteResult{}, fmt.Errorf("insert event dedupe key: %w", err)
	}
	if !dedupe.Inserted {
		if err := tx.Commit(ctx); err != nil {
			return WriteResult{}, fmt.Errorf("commit duplicate event write: %w", err)
		}
		return WriteResult{EventID: uuidFromPG(dedupe.EventID), ReceivedAt: timeFromPG(dedupe.EventReceivedAt), Inserted: false}, nil
	}

	if err := q.InsertEvent(ctx, insertEventParams(eventID, dedupeHash, normalized, opts)); err != nil {
		return WriteResult{}, fmt.Errorf("insert event: %w", err)
	}
	for idx, target := range normalized.Targets {
		if err := q.InsertEventTarget(ctx, insertTargetParams(eventID, normalized.ReceivedAt, idx, target)); err != nil {
			return WriteResult{}, fmt.Errorf("insert event target %d: %w", idx, err)
		}
	}
	if err := q.EnqueueEventEvaluation(ctx, gen.EnqueueEventEvaluationParams{
		EventReceivedAt: pgTimestamptz(normalized.ReceivedAt),
		EventID:         pgUUID(eventID),
	}); err != nil {
		return WriteResult{}, fmt.Errorf("enqueue event evaluation: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return WriteResult{}, fmt.Errorf("commit event write: %w", err)
	}
	return WriteResult{EventID: eventID, ReceivedAt: normalized.ReceivedAt, Inserted: true}, nil
}

type normalizedEventRecord struct {
	records.EventRecord
	ReceivedAt time.Time
}

func normalizeEventRecord(record records.EventRecord, opts WriteOptions) (normalizedEventRecord, error) {
	record.Source.Kind = strings.ToLower(strings.TrimSpace(record.Source.Kind))
	record.Source.Name = strings.TrimSpace(record.Source.Name)
	record.Channel = strings.TrimSpace(record.Channel)
	record.ProviderEventID = strings.TrimSpace(record.ProviderEventID)
	record.DedupeKeyValue = strings.TrimSpace(record.DedupeKeyValue)
	record.EventType = strings.TrimSpace(record.EventType)
	record.Category = strings.TrimSpace(record.Category)
	record.Action = strings.TrimSpace(record.Action)
	record.TraceID = strings.TrimSpace(record.TraceID)
	if record.Source.Kind == "" {
		return normalizedEventRecord{}, errors.New("event source kind is required")
	}
	if record.Source.Name == "" {
		return normalizedEventRecord{}, errors.New("event source name is required")
	}
	if record.Channel == "" {
		return normalizedEventRecord{}, errors.New("event channel is required")
	}
	if record.DedupeKeyValue == "" {
		return normalizedEventRecord{}, errors.New("event dedupe key is required")
	}
	if record.EventType == "" {
		return normalizedEventRecord{}, errors.New("event type is required")
	}
	if record.Category == "" {
		return normalizedEventRecord{}, errors.New("event category is required")
	}
	if record.Raw == nil {
		return normalizedEventRecord{}, errors.New("event raw payload is required")
	}

	receivedAt := opts.ReceivedAt
	if receivedAt.IsZero() {
		receivedAt = time.Now()
	}
	receivedAt = receivedAt.UTC()
	if record.OccurredAt.IsZero() {
		if !record.ObservedAt.IsZero() {
			record.OccurredAt = record.ObservedAt
		} else {
			record.OccurredAt = receivedAt
		}
	}
	record.OccurredAt = record.OccurredAt.UTC()
	if !record.ObservedAt.IsZero() {
		record.ObservedAt = record.ObservedAt.UTC()
	}
	if record.Envelope == nil {
		record.Envelope = map[string]any{}
	}

	targets := make([]records.TargetRef, 0, len(record.Targets)+1)
	if record.PrimaryTarget != nil {
		primary := normalizeTarget(*record.PrimaryTarget)
		record.PrimaryTarget = &primary
		targets = append(targets, primary)
	}
	for _, target := range record.Targets {
		targets = append(targets, normalizeTarget(target))
	}
	if record.PrimaryTarget == nil && len(targets) > 0 {
		record.PrimaryTarget = &targets[0]
	}
	record.Targets = targets

	return normalizedEventRecord{EventRecord: record, ReceivedAt: receivedAt}, nil
}

func insertEventParams(eventID uuid.UUID, dedupeHash []byte, record normalizedEventRecord, opts WriteOptions) gen.InsertEventParams {
	primaryTarget := records.TargetRef{}
	if record.PrimaryTarget != nil {
		primaryTarget = *record.PrimaryTarget
	}
	return gen.InsertEventParams{
		ID:               pgUUID(eventID),
		ReceivedAt:       pgTimestamptz(record.ReceivedAt),
		OccurredAt:       pgTimestamptz(record.OccurredAt),
		ObservedAt:       pgOptionalTimestamptz(record.ObservedAt),
		IngestID:         pgInt8Ptr(opts.IngestID),
		SourceKind:       record.SourceKind(),
		SourceID:         pgInt8Ptr(record.Source.ID),
		SourceName:       record.SourceName(),
		Channel:          record.Channel,
		ProviderEventID:  record.ProviderEventID,
		DedupeKey:        record.DedupeKeyValue,
		DedupeHash:       dedupeHash,
		EventType:        record.EventType,
		Category:         record.Category,
		Action:           record.Action,
		Severity:         record.Severity,
		ActorKind:        strings.TrimSpace(record.Actor.Kind),
		ActorID:          strings.TrimSpace(record.Actor.ID),
		ActorEmail:       strings.TrimSpace(record.Actor.Email),
		ActorDisplayName: strings.TrimSpace(record.Actor.DisplayName),
		TargetKind:       strings.TrimSpace(primaryTarget.Kind),
		TargetID:         strings.TrimSpace(primaryTarget.ID),
		TargetName:       strings.TrimSpace(primaryTarget.Name),
		TargetRef:        mustJSON(primaryTarget),
		Outcome:          string(record.Outcome),
		Ip:               optionalAddr(record.Client.IP),
		UserAgent:        strings.TrimSpace(record.Client.UserAgent),
		IdentityID:       pgInt8Ptr(record.IdentityID),
		SaasAppID:        pgInt8Ptr(record.SaaSAppID),
		Envelope:         mustJSON(record.Envelope),
		Raw:              mustJSON(record.Raw),
		TraceID:          record.TraceID,
	}
}

func insertTargetParams(eventID uuid.UUID, receivedAt time.Time, idx int, target records.TargetRef) gen.InsertEventTargetParams {
	role := strings.TrimSpace(target.Role)
	if role == "" {
		role = "target"
	}
	return gen.InsertEventTargetParams{
		EventReceivedAt: pgTimestamptz(receivedAt),
		EventID:         pgUUID(eventID),
		Ordinal:         int32(idx),
		Role:            role,
		TargetKind:      strings.TrimSpace(target.Kind),
		TargetID:        strings.TrimSpace(target.ID),
		TargetName:      strings.TrimSpace(target.Name),
		TargetEmail:     strings.TrimSpace(target.Email),
		IdentityID:      pgInt8Ptr(target.IdentityID),
		SaasAppID:       pgInt8Ptr(target.SaaSAppID),
		Envelope:        mustJSON(target.Envelope),
	}
}

func normalizeTarget(target records.TargetRef) records.TargetRef {
	target.Kind = strings.TrimSpace(target.Kind)
	target.ID = strings.TrimSpace(target.ID)
	target.Name = strings.TrimSpace(target.Name)
	target.Email = strings.TrimSpace(target.Email)
	target.Role = strings.TrimSpace(target.Role)
	if target.Envelope == nil {
		target.Envelope = map[string]any{}
	}
	return target
}

func EventDedupeHash(source records.SourceRef, dedupeKey string) []byte {
	sourceKind := strings.ToLower(strings.TrimSpace(source.Kind))
	sourceIdentity := strings.TrimSpace(source.Name)
	if source.ID != nil {
		sourceIdentity = strconv.FormatInt(*source.ID, 10)
	}
	sum := sha256.Sum256([]byte(sourceKind + "\x00" + sourceIdentity + "\x00" + strings.TrimSpace(dedupeKey)))
	return sum[:]
}

func mustJSON(value any) []byte {
	if value == nil {
		return []byte("{}")
	}
	b, err := json.Marshal(value)
	if err != nil {
		return []byte("{}")
	}
	return b
}

func optionalAddr(value string) *netip.Addr {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	addr, err := netip.ParseAddr(value)
	if err != nil {
		return nil
	}
	return &addr
}

func pgUUID(id uuid.UUID) pgtype.UUID {
	return pgtype.UUID{Bytes: [16]byte(id), Valid: true}
}

func uuidFromPG(id pgtype.UUID) uuid.UUID {
	if !id.Valid {
		return uuid.Nil
	}
	return uuid.UUID(id.Bytes)
}

func pgTimestamptz(t time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: t.UTC(), Valid: true}
}

func timeFromPG(t pgtype.Timestamptz) time.Time {
	if !t.Valid {
		return time.Time{}
	}
	return t.Time.UTC()
}

func pgOptionalTimestamptz(t time.Time) pgtype.Timestamptz {
	if t.IsZero() {
		return pgtype.Timestamptz{}
	}
	return pgTimestamptz(t)
}

func pgInt8Ptr(value *int64) pgtype.Int8 {
	if value == nil {
		return pgtype.Int8{}
	}
	return pgtype.Int8{Int64: *value, Valid: true}
}
