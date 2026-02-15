package sync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
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

type triggerRequestPayload struct {
	ConnectorKind string `json:"connector_kind,omitempty"`
	SourceName    string `json:"source_name,omitempty"`
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
	request := TriggerRequest{}
	if connectorKind, sourceName, ok := ConnectorScopeFromContext(ctx); ok {
		request = TriggerRequest{
			ConnectorKind: connectorKind,
			SourceName:    sourceName,
		}.Normalized()
	}
	if notifyChannel == ResyncNotifyChannelDiscovery && request.HasConnectorScope() && !supportsDiscoveryScopedConnectorKind(request.ConnectorKind) {
		return ErrNoConnectorsDue
	}

	lock, locked, err := r.locks.TryAcquire(ctx, legacyRunOnceScopeKind, scopeName)
	if err != nil {
		return err
	}
	if !locked {
		return ErrSyncAlreadyRunning
	}

	notifyErr, lost := runWithManagedLock(ctx, lock, func(runCtx context.Context) error {
		// When using advisory locks, TryAcquire holds a pool connection until Release.
		// Avoid deadlocking on small pools (e.g., size 1) by issuing the NOTIFY on the
		// same connection as the advisory lock rather than acquiring a second one.
		if advisory, ok := lock.(*advisoryLock); ok && advisory != nil && advisory.q != nil {
			return notifyResyncOnChannel(runCtx, advisory.q, notifyChannel, request)
		}
		q := gen.New(r.pool)
		return notifyResyncOnChannel(runCtx, q, notifyChannel, request)
	})
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

func ListenForResyncRequests(ctx context.Context, pool *pgxpool.Pool, out chan<- TriggerRequest) error {
	return ListenForResyncRequestsOnChannel(ctx, pool, ResyncNotifyChannelFull, out)
}

func ListenForResyncRequestsOnChannel(ctx context.Context, pool *pgxpool.Pool, channel string, out chan<- TriggerRequest) error {
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
		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return err
		}
		req := TriggerRequest{}
		if notification != nil {
			req = decodeTriggerRequestPayload(notification.Payload)
		}
		if !enqueueTriggerRequest(ctx, out, req) {
			return nil
		}
	}
}

func enqueueTriggerRequest(ctx context.Context, out chan<- TriggerRequest, req TriggerRequest) bool {
	if req.HasConnectorScope() {
		select {
		case out <- req:
			return true
		case <-ctx.Done():
			return false
		}
	}

	select {
	case out <- req:
	default:
	}
	return true
}

func notifyResyncOnChannel(ctx context.Context, q *gen.Queries, channel string, request TriggerRequest) error {
	if q == nil {
		return errors.New("queries is nil")
	}
	payload, hasPayload, err := encodeTriggerRequestPayload(request)
	if err != nil {
		return err
	}
	switch normalizeNotifyChannel(channel) {
	case ResyncNotifyChannelDiscovery:
		if hasPayload {
			return q.NotifyResyncDiscoveryRequestedWithPayload(ctx, payload)
		}
		return q.NotifyResyncDiscoveryRequested(ctx)
	default:
		if hasPayload {
			return q.NotifyResyncRequestedWithPayload(ctx, payload)
		}
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

func encodeTriggerRequestPayload(request TriggerRequest) (string, bool, error) {
	request = request.Normalized()
	if !request.HasConnectorScope() {
		return "", false, nil
	}
	raw, err := json.Marshal(triggerRequestPayload{
		ConnectorKind: request.ConnectorKind,
		SourceName:    request.SourceName,
	})
	if err != nil {
		return "", false, err
	}
	return string(raw), true, nil
}

func decodeTriggerRequestPayload(payload string) TriggerRequest {
	payload = strings.TrimSpace(payload)
	if payload == "" {
		return TriggerRequest{}
	}

	var decoded triggerRequestPayload
	if err := json.Unmarshal([]byte(payload), &decoded); err != nil {
		return TriggerRequest{}
	}
	return TriggerRequest{
		ConnectorKind: decoded.ConnectorKind,
		SourceName:    decoded.SourceName,
	}.Normalized()
}

func supportsDiscoveryScopedConnectorKind(kind string) bool {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case configstore.KindOkta, configstore.KindEntra:
		return true
	default:
		return false
	}
}
