package sync

import (
	"context"
	"errors"
	"fmt"
	"strings"
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

func NewBlockingRunOnceLockRunnerWithScope(locks LockManager, inner Runner, scopeName string) Runner {
	return newRunOnceLockRunner(locks, inner, false, scopeName)
}

func NewTryRunOnceLockRunnerWithScope(locks LockManager, inner Runner, scopeName string) Runner {
	return newRunOnceLockRunner(locks, inner, true, scopeName)
}

func newRunOnceLockRunner(locks LockManager, inner Runner, tryLock bool, scopeName string) Runner {
	scopeName = strings.ToLower(strings.TrimSpace(scopeName))
	if scopeName == "" {
		scopeName = defaultRunOnceScopeName
	}
	return &runOnceLockRunner{
		locks:   locks,
		inner:   inner,
		tryLock: tryLock,
		scope: runOnceScope{
			kind: defaultRunOnceScopeKind,
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
		scopeKind = defaultRunOnceScopeKind
	}
	scopeName := strings.ToLower(strings.TrimSpace(r.scope.name))
	if scopeName == "" {
		scopeName = defaultRunOnceScopeName
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

	innerErr, lost := runWithManagedLock(ctx, lock, r.inner.RunOnce)
	if lost != nil {
		return errors.Join(innerErr, fmt.Errorf("sync lock lost: %w", lost))
	}
	return innerErr
}
