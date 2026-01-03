package sync

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

var globalRunOnceLockKey = registry.ConnectorLockKey("sync", "runonce")

type runOnceLockRunner struct {
	pool    *pgxpool.Pool
	inner   Runner
	tryLock bool
}

func NewBlockingRunOnceLockRunner(pool *pgxpool.Pool, inner Runner) Runner {
	return &runOnceLockRunner{pool: pool, inner: inner}
}

func NewTryRunOnceLockRunner(pool *pgxpool.Pool, inner Runner) Runner {
	return &runOnceLockRunner{pool: pool, inner: inner, tryLock: true}
}

func (r *runOnceLockRunner) RunOnce(ctx context.Context) error {
	if r == nil || r.pool == nil || r.inner == nil {
		return errors.New("sync runner is not configured")
	}

	lockConn, err := r.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	lockQ := gen.New(lockConn)

	locked := false
	defer func() {
		if locked {
			unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = lockQ.ReleaseAdvisoryLock(unlockCtx, globalRunOnceLockKey)
		}
		lockConn.Release()
	}()

	if r.tryLock {
		ok, err := lockQ.TryAcquireAdvisoryLock(ctx, globalRunOnceLockKey)
		if err != nil {
			return err
		}
		if !ok {
			return ErrSyncAlreadyRunning
		}
		locked = true
		return r.inner.RunOnce(ctx)
	}

	if err := lockQ.AcquireAdvisoryLock(ctx, globalRunOnceLockKey); err != nil {
		return err
	}
	locked = true
	return r.inner.RunOnce(ctx)
}
