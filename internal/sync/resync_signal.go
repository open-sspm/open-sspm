package sync

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

const resyncNotifyChannel = "open_sspm_resync_requested"

type ResyncSignalRunner struct {
	pool *pgxpool.Pool
}

func NewResyncSignalRunner(pool *pgxpool.Pool) Runner {
	return &ResyncSignalRunner{pool: pool}
}

func (r *ResyncSignalRunner) RunOnce(ctx context.Context) error {
	if r == nil || r.pool == nil {
		return errors.New("sync runner is not configured")
	}

	conn, err := r.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	q := gen.New(conn)
	defer conn.Release()

	locked, err := q.TryAcquireAdvisoryLock(ctx, globalRunOnceLockKey)
	if err != nil {
		return err
	}
	if !locked {
		return ErrSyncAlreadyRunning
	}

	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = q.ReleaseAdvisoryLock(unlockCtx, globalRunOnceLockKey)
	}()

	if err := q.NotifyResyncRequested(ctx); err != nil {
		return err
	}
	return ErrSyncQueued
}

func ListenForResyncRequests(ctx context.Context, pool *pgxpool.Pool, out chan<- struct{}) error {
	if pool == nil {
		return errors.New("sync pool is nil")
	}
	if out == nil {
		return errors.New("sync signal channel is nil")
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "LISTEN "+resyncNotifyChannel); err != nil {
		return err
	}

	for {
		_, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return err
		}
		select {
		case out <- struct{}{}:
		default:
		}
	}
}
