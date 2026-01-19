package sync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

const resyncNotifyChannel = "open_sspm_resync_requested"

type ResyncSignalRunner struct {
	pool  *pgxpool.Pool
	locks LockManager
}

func NewResyncSignalRunner(pool *pgxpool.Pool, locks LockManager) Runner {
	return &ResyncSignalRunner{pool: pool, locks: locks}
}

func (r *ResyncSignalRunner) RunOnce(ctx context.Context) error {
	if r == nil || r.pool == nil || r.locks == nil {
		return errors.New("sync runner is not configured")
	}

	lock, locked, err := r.locks.TryAcquire(ctx, globalRunOnceScopeKind, globalRunOnceScopeName)
	if err != nil {
		return err
	}
	if !locked {
		return ErrSyncAlreadyRunning
	}

	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	var (
		lockLostMu sync.Mutex
		lockLost   error
	)
	stopHeartbeat := lock.StartHeartbeat(runCtx, func(err error) {
		lockLostMu.Lock()
		if lockLost == nil {
			lockLost = err
		}
		lockLostMu.Unlock()

		slog.Error("sync lock heartbeat failed", "scope_kind", lock.ScopeKind(), "scope_name", lock.ScopeName(), "err", err)
		cancelRun()
	})

	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := lock.Release(unlockCtx); err != nil {
			slog.Warn("failed to release sync lock", "scope_kind", lock.ScopeKind(), "scope_name", lock.ScopeName(), "err", err)
		}
	}()
	defer stopHeartbeat()

	q := gen.New(r.pool)
	notifyErr := q.NotifyResyncRequested(runCtx)

	lockLostMu.Lock()
	lost := lockLost
	lockLostMu.Unlock()
	if notifyErr != nil {
		if lost != nil {
			return errors.Join(notifyErr, fmt.Errorf("sync lock lost: %w", lost))
		}
		return notifyErr
	}
	if lost != nil {
		return errors.Join(ErrSyncQueued, fmt.Errorf("sync lock lost: %w", lost))
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

	cfg := pool.Config()
	conn, err := pgx.ConnectConfig(ctx, cfg.ConnConfig.Copy())
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	if _, err := conn.Exec(ctx, "LISTEN "+resyncNotifyChannel); err != nil {
		return err
	}

	for {
		_, err := conn.WaitForNotification(ctx)
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
