package sync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

const (
	globalRunOnceScopeKind = "sync"
	globalRunOnceScopeName = "runonce"
)

type runOnceLockRunner struct {
	locks   LockManager
	inner   Runner
	tryLock bool
}

func NewBlockingRunOnceLockRunner(locks LockManager, inner Runner) Runner {
	return &runOnceLockRunner{locks: locks, inner: inner}
}

func NewTryRunOnceLockRunner(locks LockManager, inner Runner) Runner {
	return &runOnceLockRunner{locks: locks, inner: inner, tryLock: true}
}

func (r *runOnceLockRunner) RunOnce(ctx context.Context) error {
	if r == nil || r.locks == nil || r.inner == nil {
		return errors.New("sync runner is not configured")
	}

	var (
		lock Lock
		ok   bool
		err  error
	)

	if r.tryLock {
		lock, ok, err = r.locks.TryAcquire(ctx, globalRunOnceScopeKind, globalRunOnceScopeName)
		if err != nil {
			return err
		}
		if !ok {
			return ErrSyncAlreadyRunning
		}
	} else {
		lock, err = r.locks.Acquire(ctx, globalRunOnceScopeKind, globalRunOnceScopeName)
		if err != nil {
			return err
		}
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

	innerErr := r.inner.RunOnce(runCtx)

	lockLostMu.Lock()
	lost := lockLost
	lockLostMu.Unlock()
	if lost != nil {
		return errors.Join(innerErr, fmt.Errorf("sync lock lost: %w", lost))
	}
	return innerErr
}
