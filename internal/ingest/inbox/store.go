package inbox

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/records"
)

const (
	StatusQueued     = "queued"
	StatusProcessing = "processing"
	StatusIgnored    = "ignored"
	StatusProcessed  = "processed"
	StatusDead       = "dead"
)

var ErrLeaseLost = errors.New("event inbox lease lost")

type Store struct {
	q *gen.Queries
}

type Delivery struct {
	ID              int64
	Source          records.SourceRef
	Channel         string
	ExternalEventID string
	DedupeKey       string
	DedupeHash      []byte
	PayloadHash     []byte
	Status          string
	ReceivedAt      time.Time
	AttemptCount    int32
	MaxAttempts     int32
	LeaseOwner      string
	LeaseUntil      time.Time
	Headers         map[string]any
	Query           map[string]any
	RawBody         []byte
	DecodedSummary  map[string]any
	IgnoreReason    string
	LastError       string
	TraceID         string
}

type EnqueueResult struct {
	ID     int64
	Status string
}

func NewStore(q *gen.Queries) *Store {
	return &Store{q: q}
}

func (s *Store) Enqueue(ctx context.Context, delivery Delivery) (EnqueueResult, error) {
	if s == nil || s.q == nil {
		return EnqueueResult{}, errors.New("event inbox store is not configured")
	}
	delivery = normalizeDelivery(delivery)
	if err := validateDelivery(delivery); err != nil {
		return EnqueueResult{}, err
	}
	if len(delivery.PayloadHash) == 0 {
		delivery.PayloadHash = PayloadHash(delivery.RawBody)
	}
	if len(delivery.DedupeHash) == 0 {
		delivery.DedupeHash = DeliveryDedupeHash(delivery.Source, delivery.Channel, delivery.DedupeKey)
	}
	row, err := s.q.UpsertEventInboxDelivery(ctx, gen.UpsertEventInboxDeliveryParams{
		SourceKind:      delivery.Source.Kind,
		SourceID:        pgInt8Ptr(delivery.Source.ID),
		SourceName:      delivery.Source.Name,
		Channel:         delivery.Channel,
		ExternalEventID: delivery.ExternalEventID,
		DedupeKey:       delivery.DedupeKey,
		DedupeHash:      delivery.DedupeHash,
		PayloadHash:     delivery.PayloadHash,
		Headers:         mustJSON(delivery.Headers),
		Query:           mustJSON(delivery.Query),
		RawBody:         delivery.RawBody,
		DecodedSummary:  mustJSON(delivery.DecodedSummary),
		TraceID:         delivery.TraceID,
	})
	if err != nil {
		return EnqueueResult{}, fmt.Errorf("upsert event inbox delivery: %w", err)
	}
	return EnqueueResult{ID: row.ID, Status: row.Status}, nil
}

func (s *Store) ClaimQueued(ctx context.Context, limit int32, leaseOwner string, leaseTTL time.Duration) ([]Delivery, error) {
	if s == nil || s.q == nil {
		return nil, errors.New("event inbox store is not configured")
	}
	leaseOwner = strings.TrimSpace(leaseOwner)
	if leaseOwner == "" {
		return nil, errors.New("event inbox lease owner is required")
	}
	if limit <= 0 {
		limit = 100
	}
	if leaseTTL <= 0 {
		leaseTTL = 5 * time.Minute
	}
	rows, err := s.q.ClaimQueuedEventInboxDeliveries(ctx, gen.ClaimQueuedEventInboxDeliveriesParams{
		LeaseOwner:   leaseOwner,
		LeaseSeconds: durationSecondsCeil(leaseTTL),
		LimitRows:    limit,
	})
	if err != nil {
		return nil, fmt.Errorf("claim event inbox deliveries: %w", err)
	}
	deliveries := make([]Delivery, 0, len(rows))
	for _, row := range rows {
		deliveries = append(deliveries, deliveryFromClaimRow(row))
	}
	return deliveries, nil
}

func (s *Store) RenewLease(ctx context.Context, leaseOwner string, ids []int64, leaseTTL time.Duration) error {
	if s == nil || s.q == nil {
		return errors.New("event inbox store is not configured")
	}
	ids = uniquePositiveIDs(ids)
	if len(ids) == 0 {
		return nil
	}
	if leaseTTL <= 0 {
		leaseTTL = 5 * time.Minute
	}
	rows, err := s.q.RenewEventInboxLease(ctx, gen.RenewEventInboxLeaseParams{
		Ids:          ids,
		LeaseOwner:   strings.TrimSpace(leaseOwner),
		LeaseSeconds: durationSecondsCeil(leaseTTL),
	})
	return expectMarkedRows("renew event inbox lease", rows, len(ids), err)
}

func (s *Store) MarkProcessed(ctx context.Context, leaseOwner string, ids []int64, summary map[string]any) error {
	rows, err := s.q.MarkEventInboxProcessed(ctx, gen.MarkEventInboxProcessedParams{
		DecodedSummary: mustJSON(summary),
		Ids:            ids,
		LeaseOwner:     strings.TrimSpace(leaseOwner),
	})
	return expectMarkedRows("mark event inbox processed", rows, len(ids), err)
}

func (s *Store) MarkIgnored(ctx context.Context, leaseOwner string, ids []int64, reason string, summary map[string]any) error {
	rows, err := s.q.MarkEventInboxIgnored(ctx, gen.MarkEventInboxIgnoredParams{
		IgnoreReason:   strings.TrimSpace(reason),
		DecodedSummary: mustJSON(summary),
		Ids:            ids,
		LeaseOwner:     strings.TrimSpace(leaseOwner),
	})
	return expectMarkedRows("mark event inbox ignored", rows, len(ids), err)
}

func (s *Store) MarkRetry(ctx context.Context, leaseOwner string, ids []int64, availableAt time.Time, lastError string) error {
	rows, err := s.q.MarkEventInboxRetry(ctx, gen.MarkEventInboxRetryParams{
		AvailableAt: pgTimestamptz(availableAt),
		LastError:   strings.TrimSpace(lastError),
		Ids:         ids,
		LeaseOwner:  strings.TrimSpace(leaseOwner),
	})
	return expectMarkedRows("mark event inbox retry", rows, len(ids), err)
}

func (s *Store) MarkDead(ctx context.Context, leaseOwner string, ids []int64, lastError string) error {
	rows, err := s.q.MarkEventInboxDead(ctx, gen.MarkEventInboxDeadParams{
		LastError:  strings.TrimSpace(lastError),
		Ids:        ids,
		LeaseOwner: strings.TrimSpace(leaseOwner),
	})
	return expectMarkedRows("mark event inbox dead", rows, len(ids), err)
}

func DeliveryDedupeHash(source records.SourceRef, channel, dedupeKey string) []byte {
	sourceKind := strings.ToLower(strings.TrimSpace(source.Kind))
	sourceIdentity := strings.TrimSpace(source.Name)
	if source.ID != nil {
		sourceIdentity = strconv.FormatInt(*source.ID, 10)
	}
	sum := sha256.Sum256([]byte(sourceKind + "\x00" + sourceIdentity + "\x00" + strings.TrimSpace(channel) + "\x00" + strings.TrimSpace(dedupeKey)))
	return sum[:]
}

func PayloadHash(raw []byte) []byte {
	sum := sha256.Sum256(raw)
	return sum[:]
}

func normalizeDelivery(delivery Delivery) Delivery {
	delivery.Source.Kind = strings.ToLower(strings.TrimSpace(delivery.Source.Kind))
	delivery.Source.Name = strings.TrimSpace(delivery.Source.Name)
	delivery.Channel = strings.TrimSpace(delivery.Channel)
	delivery.ExternalEventID = strings.TrimSpace(delivery.ExternalEventID)
	delivery.DedupeKey = strings.TrimSpace(delivery.DedupeKey)
	delivery.TraceID = strings.TrimSpace(delivery.TraceID)
	if delivery.Headers == nil {
		delivery.Headers = map[string]any{}
	}
	if delivery.Query == nil {
		delivery.Query = map[string]any{}
	}
	if delivery.DecodedSummary == nil {
		delivery.DecodedSummary = map[string]any{}
	}
	return delivery
}

func validateDelivery(delivery Delivery) error {
	if delivery.Source.Kind == "" {
		return errors.New("event inbox source kind is required")
	}
	if delivery.Source.Name == "" {
		return errors.New("event inbox source name is required")
	}
	if delivery.Channel == "" {
		return errors.New("event inbox channel is required")
	}
	if delivery.DedupeKey == "" {
		return errors.New("event inbox dedupe key is required")
	}
	if len(delivery.RawBody) == 0 {
		return errors.New("event inbox raw body is required")
	}
	return nil
}

func deliveryFromClaimRow(row gen.ClaimQueuedEventInboxDeliveriesRow) Delivery {
	return Delivery{
		ID:              row.ID,
		Source:          records.SourceRef{Kind: row.SourceKind, ID: int8Ptr(row.SourceID), Name: row.SourceName},
		Channel:         row.Channel,
		ExternalEventID: row.ExternalEventID,
		DedupeKey:       row.DedupeKey,
		DedupeHash:      row.DedupeHash,
		PayloadHash:     row.PayloadHash,
		Status:          row.Status,
		ReceivedAt:      timeFromPG(row.ReceivedAt),
		AttemptCount:    row.AttemptCount,
		MaxAttempts:     row.MaxAttempts,
		LeaseOwner:      textFromPG(row.LeaseOwner),
		LeaseUntil:      timeFromPG(row.LeaseUntil),
		Headers:         jsonMap(row.Headers),
		Query:           jsonMap(row.Query),
		RawBody:         row.RawBody,
		DecodedSummary:  jsonMap(row.DecodedSummary),
		IgnoreReason:    row.IgnoreReason,
		LastError:       row.LastError,
		TraceID:         row.TraceID,
	}
}

func expectMarkedRows(action string, rows int64, want int, err error) error {
	if err != nil {
		return fmt.Errorf("%s: %w", action, err)
	}
	if rows != int64(want) {
		return fmt.Errorf("%s: %w for %d of %d rows", action, ErrLeaseLost, want-int(rows), want)
	}
	return nil
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

func durationSecondsCeil(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	seconds := d / time.Second
	if d%time.Second != 0 {
		seconds++
	}
	if seconds <= 0 {
		return 1
	}
	return int64(seconds)
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

func jsonMap(raw []byte) map[string]any {
	if len(raw) == 0 {
		return map[string]any{}
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil || out == nil {
		return map[string]any{}
	}
	return out
}

func pgInt8Ptr(value *int64) pgtype.Int8 {
	if value == nil {
		return pgtype.Int8{}
	}
	return pgtype.Int8{Int64: *value, Valid: true}
}

func int8Ptr(value pgtype.Int8) *int64 {
	if !value.Valid {
		return nil
	}
	return &value.Int64
}

func pgTimestamptz(t time.Time) pgtype.Timestamptz {
	if t.IsZero() {
		t = time.Now()
	}
	return pgtype.Timestamptz{Time: t.UTC(), Valid: true}
}

func timeFromPG(t pgtype.Timestamptz) time.Time {
	if !t.Valid {
		return time.Time{}
	}
	return t.Time.UTC()
}

func textFromPG(t pgtype.Text) string {
	if !t.Valid {
		return ""
	}
	return strings.TrimSpace(t.String)
}
