package sync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

type runOnceLockRunner struct {
	locks   LockManager
	inner   Runner
	tryLock bool
	scope   runOnceScope
}

type runOnceScope struct {
	kind string
	name string
}

func NewBlockingRunOnceLockRunner(locks LockManager, inner Runner) Runner {
	return NewBlockingRunOnceLockRunnerWithScope(locks, inner, legacyRunOnceScopeName)
}

func NewTryRunOnceLockRunner(locks LockManager, inner Runner) Runner {
	return NewTryRunOnceLockRunnerWithScope(locks, inner, legacyRunOnceScopeName)
}

func NewBlockingRunOnceLockRunnerWithScope(locks LockManager, inner Runner, scopeName string) Runner {
	return newRunOnceLockRunner(locks, inner, false, scopeName)
}

func NewTryRunOnceLockRunnerWithScope(locks LockManager, inner Runner, scopeName string) Runner {
	return newRunOnceLockRunner(locks, inner, true, scopeName)
}

func newRunOnceLockRunner(locks LockManager, inner Runner, tryLock bool, scopeName string) Runner {
	scopeName = strings.ToLower(strings.TrimSpace(scopeName))
	if scopeName == "" {
		scopeName = legacyRunOnceScopeName
	}
	return &runOnceLockRunner{
		locks:   locks,
		inner:   inner,
		tryLock: tryLock,
		scope: runOnceScope{
			kind: legacyRunOnceScopeKind,
			name: scopeName,
		},
	}
}

func (r *runOnceLockRunner) RunOnce(ctx context.Context) error {
	if r == nil || r.locks == nil || r.inner == nil {
		return errors.New("sync runner is not configured")
	}
	scopeKind := strings.ToLower(strings.TrimSpace(r.scope.kind))
	if scopeKind == "" {
		scopeKind = legacyRunOnceScopeKind
	}
	scopeName := strings.ToLower(strings.TrimSpace(r.scope.name))
	if scopeName == "" {
		scopeName = legacyRunOnceScopeName
	}

	var (
		lock Lock
		ok   bool
		err  error
	)

	if r.tryLock {
		lock, ok, err = r.locks.TryAcquire(ctx, scopeKind, scopeName)
		if err != nil {
			return err
		}
		if !ok {
			return ErrSyncAlreadyRunning
		}
	} else {
		lock, err = r.locks.Acquire(ctx, scopeKind, scopeName)
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
