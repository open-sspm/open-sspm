package tail

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

type Scheduler struct {
	q *gen.Queries
}

type Wakeup struct {
	SourceKind string
	SourceName string
	Resource   string
	Reason     string
	Payload    map[string]any
	Priority   int32
	RunAfter   time.Time
}

func NewScheduler(q *gen.Queries) *Scheduler {
	return &Scheduler{q: q}
}

func (s *Scheduler) Wake(ctx context.Context, wakeup Wakeup) (bool, error) {
	if s == nil || s.q == nil {
		return false, errors.New("tail scheduler is not configured")
	}
	wakeup = normalizeWakeup(wakeup)
	if err := validateWakeup(wakeup); err != nil {
		return false, err
	}
	_, err := s.q.GetActiveTailSyncJobByScope(ctx, gen.GetActiveTailSyncJobByScopeParams{
		ConnectorKind: pgText(wakeup.SourceKind),
		SourceName:    pgText(wakeup.SourceName),
		Resource:      pgText(wakeup.Resource),
	})
	if err == nil {
		return false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return false, fmt.Errorf("check active tail job: %w", err)
	}

	id, err := uuid.NewV7()
	if err != nil {
		return false, fmt.Errorf("generate tail job id: %w", err)
	}
	_, err = s.q.CreateTailSyncJob(ctx, gen.CreateTailSyncJobParams{
		ID:            pgUUID(id),
		ConnectorKind: wakeup.SourceKind,
		SourceName:    wakeup.SourceName,
		Resource:      wakeup.Resource,
		AvailableAt:   pgTimestamptz(wakeup.RunAfter),
		Payload:       mustJSON(wakeup.Payload),
		Priority:      wakeup.Priority,
		CreatedReason: wakeup.Reason,
	})
	if err != nil {
		if isUniqueViolation(err) {
			return false, nil
		}
		return false, fmt.Errorf("create tail job: %w", err)
	}
	return true, nil
}

func normalizeWakeup(w Wakeup) Wakeup {
	w.SourceKind = strings.ToLower(strings.TrimSpace(w.SourceKind))
	w.SourceName = strings.TrimSpace(w.SourceName)
	w.Resource = strings.TrimSpace(w.Resource)
	w.Reason = strings.TrimSpace(w.Reason)
	if w.Reason == "" {
		w.Reason = "wakeup"
	}
	if w.Payload == nil {
		w.Payload = map[string]any{}
	}
	if w.RunAfter.IsZero() {
		w.RunAfter = time.Now()
	}
	return w
}

func validateWakeup(w Wakeup) error {
	if w.SourceKind == "" {
		return errors.New("tail wakeup source kind is required")
	}
	if w.SourceName == "" {
		return errors.New("tail wakeup source name is required")
	}
	if w.Resource == "" {
		return errors.New("tail wakeup resource is required")
	}
	return nil
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

func pgText(value string) pgtype.Text {
	value = strings.TrimSpace(value)
	if value == "" {
		return pgtype.Text{}
	}
	return pgtype.Text{String: value, Valid: true}
}

func pgUUID(id uuid.UUID) pgtype.UUID {
	return pgtype.UUID{Bytes: [16]byte(id), Valid: true}
}

func pgTimestamptz(t time.Time) pgtype.Timestamptz {
	if t.IsZero() {
		t = time.Now()
	}
	return pgtype.Timestamptz{Time: t.UTC(), Valid: true}
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
