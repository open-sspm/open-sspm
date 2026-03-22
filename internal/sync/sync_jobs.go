package sync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/normalize"
)

var (
	errActiveSyncJobExists  = errors.New("active sync job already exists")
	errPendingSyncJobExists = errors.New("pending sync job already exists")
)

type syncJobRecord struct {
	ID             pgtype.UUID
	Lane           string
	ConnectorKind  string
	SourceName     string
	TriggerKind    string
	ClaimedBy      string
	Status         string
	AttemptCount   int32
	RerunRequested bool
}

type syncJobStore interface {
	EnqueueManualSyncJob(ctx context.Context, lane, connectorKind, sourceName string) error
	EnqueueScheduledSyncJob(ctx context.Context, lane, connectorKind, sourceName string) error
	RequeueStaleSyncJobsByLane(ctx context.Context, lane string) (int64, error)
	ClaimNextSyncJobByLane(ctx context.Context, lane, claimedBy string, leaseSeconds int64) (syncJobRecord, bool, error)
	RenewSyncJobLease(ctx context.Context, jobID pgtype.UUID, claimedBy string, leaseSeconds int64) (bool, error)
	MarkSyncJobRunning(ctx context.Context, jobID pgtype.UUID, claimedBy string) (syncJobRecord, bool, error)
	CompleteManualSyncJobSuccess(ctx context.Context, jobID pgtype.UUID, claimedBy string) (bool, error)
	CompleteManualSyncJobFailure(ctx context.Context, jobID pgtype.UUID, claimedBy, lastError string) (bool, error)
	CompleteScheduledSyncJobSuccess(ctx context.Context, jobID pgtype.UUID, claimedBy string) (bool, error)
	CompleteScheduledSyncJobFailure(ctx context.Context, jobID pgtype.UUID, claimedBy, lastError string, nextAvailableAt time.Time) (bool, error)
}

type dbSyncJobStore struct {
	db gen.DBTX
	q  *gen.Queries
}

func NewSyncJobStore(db gen.DBTX) syncJobStore {
	if db == nil {
		return nil
	}
	return &dbSyncJobStore{db: db, q: gen.New(db)}
}

func (s *dbSyncJobStore) EnqueueManualSyncJob(ctx context.Context, lane, connectorKind, sourceName string) error {
	if s == nil || s.q == nil {
		return errors.New("sync job store is not configured")
	}
	lane, connectorKind, sourceName = normalizedSyncJobScope(lane, connectorKind, sourceName)

	for attempt := 0; attempt < 5; attempt++ {
		active, ok, err := s.getActiveSyncJobByScope(ctx, lane, connectorKind, sourceName)
		if err != nil {
			return err
		}
		if !ok {
			if err := s.createSyncJob(ctx, lane, connectorKind, sourceName, syncJobTriggerKindManual, time.Now(), 0, false); err != nil {
				if errors.Is(err, errActiveSyncJobExists) {
					continue
				}
				return err
			}
			s.notifyLane(ctx, lane)
			return nil
		}
		switch active.TriggerKind {
		case syncJobTriggerKindManual:
			switch active.Status {
			case syncJobStatusPending:
				return errPendingSyncJobExists
			case syncJobStatusClaimed, syncJobStatusRunning:
				return errActiveSyncJobExists
			default:
				return fmt.Errorf("unsupported active manual sync job status %q", active.Status)
			}
		case syncJobTriggerKindScheduled:
			if _, err := s.q.PromoteSyncJobForManualRequest(ctx, active.ID); err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					continue
				}
				return err
			}
			s.notifyLane(ctx, lane)
			return nil
		default:
			return fmt.Errorf("unsupported sync job trigger kind %q", active.TriggerKind)
		}
	}

	return errors.New("failed to enqueue manual sync job after retries")
}

func (s *dbSyncJobStore) EnqueueScheduledSyncJob(ctx context.Context, lane, connectorKind, sourceName string) error {
	if s == nil || s.q == nil {
		return errors.New("sync job store is not configured")
	}
	lane, connectorKind, sourceName = normalizedSyncJobScope(lane, connectorKind, sourceName)

	for attempt := 0; attempt < 5; attempt++ {
		if _, ok, err := s.getActiveSyncJobByScope(ctx, lane, connectorKind, sourceName); err != nil {
			return err
		} else if ok {
			return nil
		}

		if err := s.createSyncJob(ctx, lane, connectorKind, sourceName, syncJobTriggerKindScheduled, time.Now(), 0, false); err != nil {
			if errors.Is(err, errActiveSyncJobExists) {
				continue
			}
			return err
		}
		s.notifyLane(ctx, lane)
		return nil
	}

	return errors.New("failed to enqueue scheduled sync job after retries")
}

func (s *dbSyncJobStore) RequeueStaleSyncJobsByLane(ctx context.Context, lane string) (int64, error) {
	if s == nil || s.q == nil {
		return 0, errors.New("sync job store is not configured")
	}
	return s.q.RequeueStaleSyncJobsByLane(ctx, normalize.Lower(lane))
}

func (s *dbSyncJobStore) ClaimNextSyncJobByLane(ctx context.Context, lane, claimedBy string, leaseSeconds int64) (syncJobRecord, bool, error) {
	if s == nil || s.q == nil {
		return syncJobRecord{}, false, errors.New("sync job store is not configured")
	}
	row, err := s.q.ClaimNextSyncJobByLane(ctx, gen.ClaimNextSyncJobByLaneParams{
		ClaimedBy:    nullableText(claimedBy),
		LeaseSeconds: leaseSeconds,
		Lane:         normalize.Lower(lane),
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return syncJobRecord{}, false, nil
		}
		return syncJobRecord{}, false, err
	}
	return syncJobRecordFromRow(row), true, nil
}

func (s *dbSyncJobStore) RenewSyncJobLease(ctx context.Context, jobID pgtype.UUID, claimedBy string, leaseSeconds int64) (bool, error) {
	if s == nil || s.q == nil {
		return false, errors.New("sync job store is not configured")
	}
	_, err := s.q.RenewSyncJobLease(ctx, gen.RenewSyncJobLeaseParams{
		LeaseSeconds: leaseSeconds,
		ID:           jobID,
		ClaimedBy:    nullableText(claimedBy),
	})
	if err == nil {
		return true, nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return false, err
}

func (s *dbSyncJobStore) MarkSyncJobRunning(ctx context.Context, jobID pgtype.UUID, claimedBy string) (syncJobRecord, bool, error) {
	if s == nil || s.q == nil {
		return syncJobRecord{}, false, errors.New("sync job store is not configured")
	}
	row, err := s.q.MarkSyncJobRunning(ctx, gen.MarkSyncJobRunningParams{
		ID:        jobID,
		ClaimedBy: nullableText(claimedBy),
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return syncJobRecord{}, false, nil
		}
		return syncJobRecord{}, false, err
	}
	return syncJobRecordFromRow(row), true, nil
}

func (s *dbSyncJobStore) CompleteManualSyncJobSuccess(ctx context.Context, jobID pgtype.UUID, claimedBy string) (bool, error) {
	if s == nil || s.q == nil {
		return false, errors.New("sync job store is not configured")
	}
	rows, err := s.q.CompleteManualSyncJobSuccess(ctx, gen.CompleteManualSyncJobSuccessParams{
		ID:        jobID,
		ClaimedBy: nullableText(claimedBy),
	})
	return rows > 0, err
}

func (s *dbSyncJobStore) CompleteManualSyncJobFailure(ctx context.Context, jobID pgtype.UUID, claimedBy, lastError string) (bool, error) {
	if s == nil || s.q == nil {
		return false, errors.New("sync job store is not configured")
	}
	rows, err := s.q.CompleteManualSyncJobFailure(ctx, gen.CompleteManualSyncJobFailureParams{
		LastError: nullableText(lastError),
		ID:        jobID,
		ClaimedBy: nullableText(claimedBy),
	})
	return rows > 0, err
}

func (s *dbSyncJobStore) CompleteScheduledSyncJobSuccess(ctx context.Context, jobID pgtype.UUID, claimedBy string) (bool, error) {
	if s == nil || s.q == nil {
		return false, errors.New("sync job store is not configured")
	}
	rows, err := s.q.CompleteScheduledSyncJobSuccess(ctx, gen.CompleteScheduledSyncJobSuccessParams{
		ID:        jobID,
		ClaimedBy: nullableText(claimedBy),
	})
	return rows > 0, err
}

func (s *dbSyncJobStore) CompleteScheduledSyncJobFailure(ctx context.Context, jobID pgtype.UUID, claimedBy, lastError string, nextAvailableAt time.Time) (bool, error) {
	if s == nil || s.q == nil {
		return false, errors.New("sync job store is not configured")
	}
	rows, err := s.q.CompleteScheduledSyncJobFailure(ctx, gen.CompleteScheduledSyncJobFailureParams{
		AvailableAt: timestamptz(nextAvailableAt),
		LastError:   nullableText(lastError),
		ID:          jobID,
		ClaimedBy:   nullableText(claimedBy),
	})
	return rows > 0, err
}

func (s *dbSyncJobStore) getActiveSyncJobByScope(ctx context.Context, lane, connectorKind, sourceName string) (syncJobRecord, bool, error) {
	row, err := s.q.GetActiveSyncJobByScope(ctx, gen.GetActiveSyncJobByScopeParams{
		Lane:          lane,
		ConnectorKind: nullableText(connectorKind),
		SourceName:    nullableText(sourceName),
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return syncJobRecord{}, false, nil
		}
		return syncJobRecord{}, false, err
	}
	return syncJobRecordFromRow(row), true, nil
}

func (s *dbSyncJobStore) createSyncJob(ctx context.Context, lane, connectorKind, sourceName, triggerKind string, availableAt time.Time, attemptCount int32, rerunRequested bool) error {
	_, err := s.q.CreateSyncJob(ctx, gen.CreateSyncJobParams{
		ID:             pgUUID(uuid.New()),
		Lane:           lane,
		ConnectorKind:  nullableText(connectorKind),
		SourceName:     nullableText(sourceName),
		TriggerKind:    strings.TrimSpace(triggerKind),
		Status:         syncJobStatusPending,
		AttemptCount:   attemptCount,
		AvailableAt:    timestamptz(availableAt),
		RerunRequested: rerunRequested,
	})
	if err == nil {
		return nil
	}
	if isUniqueViolation(err) {
		return errActiveSyncJobExists
	}
	return err
}

func (s *dbSyncJobStore) notifyLane(ctx context.Context, lane string) {
	if s == nil || s.db == nil {
		return
	}
	channel := syncJobNotifyChannelFull
	switch normalize.Lower(lane) {
	case syncJobLaneDiscovery:
		channel = syncJobNotifyChannelDiscovery
	case syncJobLaneFull:
		channel = syncJobNotifyChannelFull
	}
	if _, err := s.db.Exec(ctx, "SELECT pg_notify($1::text, '')", channel); err != nil {
		slog.Warn("sync job notify failed", "lane", lane, "channel", channel, "err", err)
	}
}

func syncJobRecordFromRow(row gen.SyncJob) syncJobRecord {
	return syncJobRecord{
		ID:             row.ID,
		Lane:           row.Lane,
		ConnectorKind:  normalizedNullableText(row.ConnectorKind),
		SourceName:     normalizedSourceName(row.SourceName),
		TriggerKind:    strings.TrimSpace(row.TriggerKind),
		ClaimedBy:      normalizedNullableText(row.ClaimedBy),
		Status:         strings.TrimSpace(row.Status),
		AttemptCount:   row.AttemptCount,
		RerunRequested: row.RerunRequested,
	}
}

func normalizedSyncJobScope(lane, connectorKind, sourceName string) (string, string, string) {
	return normalize.Lower(lane), normalize.Lower(connectorKind), strings.TrimSpace(sourceName)
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func normalizedNullableText(value pgtype.Text) string {
	if !value.Valid {
		return ""
	}
	return normalize.Trim(value.String)
}

func normalizedSourceName(value pgtype.Text) string {
	if !value.Valid {
		return ""
	}
	return strings.TrimSpace(value.String)
}

func nullableText(value string) pgtype.Text {
	value = strings.TrimSpace(value)
	if value == "" {
		return pgtype.Text{}
	}
	return pgtype.Text{String: value, Valid: true}
}

func timestamptz(ts time.Time) pgtype.Timestamptz {
	if ts.IsZero() {
		ts = time.Now()
	}
	return pgtype.Timestamptz{Time: ts.UTC(), Valid: true}
}
