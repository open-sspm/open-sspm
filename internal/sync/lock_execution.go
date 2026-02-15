package sync

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

func runWithManagedLock(ctx context.Context, lock Lock, run func(context.Context) error) (error, error) {
	if lock == nil {
		return errors.New("sync lock is nil"), nil
	}
	if run == nil {
		return errors.New("sync run function is nil"), nil
	}
	if ctx == nil {
		ctx = context.Background()
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
	defer stopHeartbeat()

	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := lock.Release(unlockCtx); err != nil {
			slog.Warn("failed to release sync lock", "scope_kind", lock.ScopeKind(), "scope_name", lock.ScopeName(), "err", err)
		}
	}()

	runErr := run(runCtx)

	lockLostMu.Lock()
	lost := lockLost
	lockLostMu.Unlock()
	return runErr, lost
}
