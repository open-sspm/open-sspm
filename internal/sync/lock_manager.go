package sync

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

const (
	LockModeLease    = "lease"
	LockModeAdvisory = "advisory"

	defaultLockMode              = LockModeLease
	defaultLockTTL               = 60 * time.Second
	defaultLockHeartbeatInterval = 15 * time.Second
	defaultLockHeartbeatTimeout  = 15 * time.Second
)

type LockManagerConfig struct {
	Mode              string
	InstanceID        string
	TTL               time.Duration
	HeartbeatInterval time.Duration
	HeartbeatTimeout  time.Duration
}

type Lock interface {
	ScopeKind() string
	ScopeName() string
	StartHeartbeat(ctx context.Context, onLost func(error)) (stop func())
	Release(ctx context.Context) error
}

type LockManager interface {
	TryAcquire(ctx context.Context, scopeKind, scopeName string) (Lock, bool, error)
	Acquire(ctx context.Context, scopeKind, scopeName string) (Lock, error)
}

func NewLockManager(pool *pgxpool.Pool, cfg LockManagerConfig) (LockManager, error) {
	if pool == nil {
		return nil, errors.New("lock pool is nil")
	}

	mode := strings.ToLower(strings.TrimSpace(cfg.Mode))
	if mode == "" {
		mode = defaultLockMode
	}

	instanceID := strings.TrimSpace(cfg.InstanceID)
	if instanceID == "" {
		if h := strings.TrimSpace(os.Getenv("HOSTNAME")); h != "" {
			instanceID = h
		} else if h, err := os.Hostname(); err == nil {
			instanceID = strings.TrimSpace(h)
		}
	}
	if instanceID == "" {
		instanceID = "unknown"
	}

	ttl := cfg.TTL
	if ttl <= 0 {
		ttl = defaultLockTTL
	}
	hbInterval := cfg.HeartbeatInterval
	if hbInterval <= 0 {
		hbInterval = defaultLockHeartbeatInterval
	}
	hbTimeout := cfg.HeartbeatTimeout
	if hbTimeout <= 0 {
		hbTimeout = defaultLockHeartbeatTimeout
	}

	switch mode {
	case LockModeLease:
		return &leaseLockManager{
			q:                gen.New(pool),
			instanceID:       instanceID,
			ttlSeconds:       durationSecondsCeil(ttl),
			heartbeatEvery:   hbInterval,
			heartbeatTimeout: hbTimeout,
		}, nil
	case LockModeAdvisory:
		return &advisoryLockManager{pool: pool}, nil
	default:
		return nil, fmt.Errorf("unknown lock mode %q", mode)
	}
}

func normalizeScope(kind, name string) (string, string, error) {
	kind = strings.ToLower(strings.TrimSpace(kind))
	name = strings.ToLower(strings.TrimSpace(name))
	if kind == "" {
		return "", "", errors.New("scope kind is required")
	}
	if name == "" {
		return "", "", errors.New("scope name is required")
	}
	return kind, name, nil
}

func durationSecondsCeil(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64((d + time.Second - 1) / time.Second)
}

type leaseLockManager struct {
	q                *gen.Queries
	instanceID       string
	ttlSeconds       int64
	heartbeatEvery   time.Duration
	heartbeatTimeout time.Duration
}

func (m *leaseLockManager) TryAcquire(ctx context.Context, scopeKind, scopeName string) (Lock, bool, error) {
	scopeKind, scopeName, err := normalizeScope(scopeKind, scopeName)
	if err != nil {
		return nil, false, err
	}
	if m == nil || m.q == nil {
		return nil, false, errors.New("lock manager is not configured")
	}

	token := pgUUID(uuid.New())
	_, err = m.q.TryAcquireSyncLockLease(ctx, gen.TryAcquireSyncLockLeaseParams{
		ScopeKind:        scopeKind,
		ScopeName:        scopeName,
		HolderInstanceID: m.instanceID,
		HolderToken:      token,
		LeaseSeconds:     m.ttlSeconds,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return &leaseLock{
		m:         m,
		scopeKind: scopeKind,
		scopeName: scopeName,
		token:     token,
	}, true, nil
}

func (m *leaseLockManager) Acquire(ctx context.Context, scopeKind, scopeName string) (Lock, error) {
	scopeKind, scopeName, err := normalizeScope(scopeKind, scopeName)
	if err != nil {
		return nil, err
	}
	if m == nil || m.q == nil {
		return nil, errors.New("lock manager is not configured")
	}

	token := pgUUID(uuid.New())

	delay := 250 * time.Millisecond
	maxDelay := 5 * time.Second
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for {
		_, err := m.q.TryAcquireSyncLockLease(ctx, gen.TryAcquireSyncLockLeaseParams{
			ScopeKind:        scopeKind,
			ScopeName:        scopeName,
			HolderInstanceID: m.instanceID,
			HolderToken:      token,
			LeaseSeconds:     m.ttlSeconds,
		})
		if err == nil {
			return &leaseLock{
				m:         m,
				scopeKind: scopeKind,
				scopeName: scopeName,
				token:     token,
			}, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return nil, err
		}

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		// Backoff with a small jitter to reduce herd effects.
		jitter := time.Duration(rng.Int63n(int64(delay/2) + 1))
		sleep := delay + jitter

		timer := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
		if delay < maxDelay {
			delay *= 2
			if delay > maxDelay {
				delay = maxDelay
			}
		}
	}
}

type leaseLock struct {
	m         *leaseLockManager
	scopeKind string
	scopeName string
	token     pgtype.UUID
}

func (l *leaseLock) ScopeKind() string { return l.scopeKind }
func (l *leaseLock) ScopeName() string { return l.scopeName }

func (l *leaseLock) StartHeartbeat(ctx context.Context, onLost func(error)) (stop func()) {
	if l == nil || l.m == nil || l.m.q == nil {
		return func() {}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if onLost == nil {
		onLost = func(error) {}
	}

	hbCtx, cancel := context.WithCancel(ctx)
	var once sync.Once
	stop = func() { once.Do(cancel) }

	// Spread initial heartbeats slightly for multiple concurrent locks.
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	initialJitter := time.Duration(rng.Int63n(int64(l.m.heartbeatEvery/3) + 1))

	go func() {
		timer := time.NewTimer(initialJitter)
		defer timer.Stop()

		select {
		case <-hbCtx.Done():
			return
		case <-timer.C:
		}

		ticker := time.NewTicker(l.m.heartbeatEvery)
		defer ticker.Stop()

		for {
			select {
			case <-hbCtx.Done():
				return
			case <-ticker.C:
			}

			queryCtx, cancel := context.WithTimeout(hbCtx, l.m.heartbeatTimeout)
			_, err := l.m.q.RenewSyncLockLease(queryCtx, gen.RenewSyncLockLeaseParams{
				LeaseSeconds: l.m.ttlSeconds,
				ScopeKind:    l.scopeKind,
				ScopeName:    l.scopeName,
				HolderToken:  l.token,
			})
			cancel()
			if err != nil {
				onLost(err)
				return
			}
		}
	}()

	return stop
}

func (l *leaseLock) Release(ctx context.Context) error {
	if l == nil || l.m == nil || l.m.q == nil {
		return errors.New("lock is not configured")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return l.m.q.ReleaseSyncLockLease(ctx, gen.ReleaseSyncLockLeaseParams{
		ScopeKind:   l.scopeKind,
		ScopeName:   l.scopeName,
		HolderToken: l.token,
	})
}

type advisoryLockManager struct {
	pool *pgxpool.Pool
}

func (m *advisoryLockManager) TryAcquire(ctx context.Context, scopeKind, scopeName string) (Lock, bool, error) {
	scopeKind, scopeName, err := normalizeScope(scopeKind, scopeName)
	if err != nil {
		return nil, false, err
	}
	if m == nil || m.pool == nil {
		return nil, false, errors.New("lock manager is not configured")
	}

	conn, err := m.pool.Acquire(ctx)
	if err != nil {
		return nil, false, err
	}
	q := gen.New(conn)
	key := registry.ConnectorLockKey(scopeKind, scopeName)

	ok, err := q.TryAcquireAdvisoryLock(ctx, key)
	if err != nil {
		conn.Release()
		return nil, false, err
	}
	if !ok {
		conn.Release()
		return nil, false, nil
	}

	return &advisoryLock{
		conn:      conn,
		q:         q,
		key:       key,
		scopeKind: scopeKind,
		scopeName: scopeName,
	}, true, nil
}

func (m *advisoryLockManager) Acquire(ctx context.Context, scopeKind, scopeName string) (Lock, error) {
	scopeKind, scopeName, err := normalizeScope(scopeKind, scopeName)
	if err != nil {
		return nil, err
	}
	if m == nil || m.pool == nil {
		return nil, errors.New("lock manager is not configured")
	}

	conn, err := m.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	q := gen.New(conn)
	key := registry.ConnectorLockKey(scopeKind, scopeName)

	if err := q.AcquireAdvisoryLock(ctx, key); err != nil {
		conn.Release()
		return nil, err
	}

	return &advisoryLock{
		conn:      conn,
		q:         q,
		key:       key,
		scopeKind: scopeKind,
		scopeName: scopeName,
	}, nil
}

type advisoryLock struct {
	conn      *pgxpool.Conn
	q         *gen.Queries
	key       int64
	scopeKind string
	scopeName string

	releaseOnce sync.Once
}

func (l *advisoryLock) ScopeKind() string { return l.scopeKind }
func (l *advisoryLock) ScopeName() string { return l.scopeName }

func (l *advisoryLock) StartHeartbeat(_ context.Context, _ func(error)) func() { return func() {} }

func (l *advisoryLock) Release(ctx context.Context) error {
	if l == nil || l.q == nil || l.conn == nil {
		return errors.New("lock is not configured")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	var unlockErr error
	l.releaseOnce.Do(func() {
		unlockErr = l.q.ReleaseAdvisoryLock(ctx, l.key)
		l.conn.Release()
	})

	return unlockErr
}

func pgUUID(id uuid.UUID) pgtype.UUID {
	return pgtype.UUID{Bytes: id, Valid: true}
}
