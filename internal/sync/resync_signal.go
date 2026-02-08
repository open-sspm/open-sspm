package sync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

type ResyncSignalRunner struct {
	pool             *pgxpool.Pool
	locks            LockManager
	notifyChannel    string
	runOnceScopeName string
}

func NewResyncSignalRunner(pool *pgxpool.Pool, locks LockManager) Runner {
	return NewResyncSignalRunnerWithConfig(pool, locks, ResyncSignalConfig{})
}

type ResyncSignalConfig struct {
	NotifyChannel    string
	RunOnceScopeName string
}

func NewResyncSignalRunnerWithConfig(pool *pgxpool.Pool, locks LockManager, cfg ResyncSignalConfig) Runner {
	channel := normalizeNotifyChannel(cfg.NotifyChannel)
	scopeName := strings.ToLower(strings.TrimSpace(cfg.RunOnceScopeName))
	if scopeName == "" {
		scopeName = RunOnceScopeNameForMode(modeForResyncChannel(channel))
	}
	return &ResyncSignalRunner{
		pool:             pool,
		locks:            locks,
		notifyChannel:    channel,
		runOnceScopeName: scopeName,
	}
}

func (r *ResyncSignalRunner) RunOnce(ctx context.Context) error {
	if r == nil || r.pool == nil || r.locks == nil {
		return errors.New("sync runner is not configured")
	}
	scopeName := strings.ToLower(strings.TrimSpace(r.runOnceScopeName))
	if scopeName == "" {
		scopeName = RunOnceScopeNameForMode(modeForResyncChannel(r.notifyChannel))
	}
	notifyChannel := normalizeNotifyChannel(r.notifyChannel)

	lock, locked, err := r.locks.TryAcquire(ctx, legacyRunOnceScopeKind, scopeName)
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

	// When using advisory locks, TryAcquire holds a pool connection until Release.
	// Avoid deadlocking on small pools (e.g., size 1) by issuing the NOTIFY on the
	// same connection as the advisory lock rather than acquiring a second one.
	var notifyErr error
	if advisory, ok := lock.(*advisoryLock); ok && advisory != nil && advisory.q != nil {
		notifyErr = notifyResyncOnChannel(runCtx, advisory.q, notifyChannel)
	} else {
		q := gen.New(r.pool)
		notifyErr = notifyResyncOnChannel(runCtx, q, notifyChannel)
	}

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
	return ListenForResyncRequestsOnChannel(ctx, pool, ResyncNotifyChannelFull, out)
}

func ListenForResyncRequestsOnChannel(ctx context.Context, pool *pgxpool.Pool, channel string, out chan<- struct{}) error {
	if pool == nil {
		return errors.New("sync pool is nil")
	}
	if out == nil {
		return errors.New("sync signal channel is nil")
	}
	channel = normalizeNotifyChannel(channel)

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

	if _, err := conn.Exec(ctx, "LISTEN "+channel); err != nil {
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

func notifyResyncOnChannel(ctx context.Context, q *gen.Queries, channel string) error {
	if q == nil {
		return errors.New("queries is nil")
	}
	switch normalizeNotifyChannel(channel) {
	case ResyncNotifyChannelDiscovery:
		return q.NotifyResyncDiscoveryRequested(ctx)
	default:
		return q.NotifyResyncRequested(ctx)
	}
}

func normalizeNotifyChannel(channel string) string {
	channel = strings.TrimSpace(channel)
	switch channel {
	case ResyncNotifyChannelDiscovery:
		return ResyncNotifyChannelDiscovery
	default:
		return ResyncNotifyChannelFull
	}
}

func modeForResyncChannel(channel string) registry.RunMode {
	switch normalizeNotifyChannel(channel) {
	case ResyncNotifyChannelDiscovery:
		return registry.RunModeDiscovery
	default:
		return registry.RunModeFull
	}
}
