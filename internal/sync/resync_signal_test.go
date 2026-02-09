package sync

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

type stubLockManager struct {
	lock Lock
}

func (m stubLockManager) TryAcquire(context.Context, string, string) (Lock, bool, error) {
	return m.lock, true, nil
}

func (m stubLockManager) Acquire(context.Context, string, string) (Lock, error) {
	return m.lock, nil
}

type trackingLockManager struct {
	tryAcquireCalls int
}

func (m *trackingLockManager) TryAcquire(context.Context, string, string) (Lock, bool, error) {
	m.tryAcquireCalls++
	return nil, false, nil
}

func (m *trackingLockManager) Acquire(context.Context, string, string) (Lock, error) {
	return nil, nil
}

type fakeDBTX struct {
	execCount int
	lastSQL   string
	execErr   error
}

func (db *fakeDBTX) Exec(_ context.Context, sql string, _ ...interface{}) (pgconn.CommandTag, error) {
	db.execCount++
	db.lastSQL = sql
	return pgconn.CommandTag{}, db.execErr
}

func (db *fakeDBTX) Query(context.Context, string, ...interface{}) (pgx.Rows, error) {
	panic("Query not expected")
}

func (db *fakeDBTX) QueryRow(context.Context, string, ...interface{}) pgx.Row {
	panic("QueryRow not expected")
}

func TestResyncSignalRunner_UsesAdvisoryLockConnectionForNotify(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("notify sentinel")
	db := &fakeDBTX{execErr: sentinel}
	lock := &advisoryLock{
		q:         gen.New(db),
		scopeKind: legacyRunOnceScopeKind,
		scopeName: RunOnceScopeNameForMode(modeForResyncChannel(ResyncNotifyChannelFull)),
	}

	runner := NewResyncSignalRunner(&pgxpool.Pool{}, stubLockManager{lock: lock})
	err := runner.RunOnce(context.Background())

	if !errors.Is(err, sentinel) {
		t.Fatalf("expected notify error, got %v", err)
	}
	if db.execCount != 1 {
		t.Fatalf("expected 1 notify exec, got %d", db.execCount)
	}
	if !strings.Contains(db.lastSQL, "pg_notify('open_sspm_resync_requested'") {
		t.Fatalf("expected pg_notify SQL, got %q", db.lastSQL)
	}
}

func TestResyncSignalRunner_UsesDiscoveryNotifyChannel(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("notify sentinel")
	db := &fakeDBTX{execErr: sentinel}
	lock := &advisoryLock{
		q:         gen.New(db),
		scopeKind: legacyRunOnceScopeKind,
		scopeName: RunOnceScopeNameForMode(modeForResyncChannel(ResyncNotifyChannelDiscovery)),
	}

	runner := NewResyncSignalRunnerWithConfig(&pgxpool.Pool{}, stubLockManager{lock: lock}, ResyncSignalConfig{
		NotifyChannel:    ResyncNotifyChannelDiscovery,
		RunOnceScopeName: RunOnceScopeNameDiscovery,
	})
	err := runner.RunOnce(context.Background())

	if !errors.Is(err, sentinel) {
		t.Fatalf("expected notify error, got %v", err)
	}
	if db.execCount != 1 {
		t.Fatalf("expected 1 notify exec, got %d", db.execCount)
	}
	if !strings.Contains(db.lastSQL, "pg_notify('open_sspm_resync_discovery_requested'") {
		t.Fatalf("expected discovery pg_notify SQL, got %q", db.lastSQL)
	}
}

func TestResyncSignalRunner_DiscoveryScopedUnsupportedConnectorReturnsNoWork(t *testing.T) {
	t.Parallel()

	locks := &trackingLockManager{}
	runner := NewResyncSignalRunnerWithConfig(&pgxpool.Pool{}, locks, ResyncSignalConfig{
		NotifyChannel:    ResyncNotifyChannelDiscovery,
		RunOnceScopeName: RunOnceScopeNameDiscovery,
	})
	ctx := WithConnectorScope(context.Background(), "github", "acme")
	err := runner.RunOnce(ctx)
	if !errors.Is(err, ErrNoConnectorsDue) {
		t.Fatalf("expected ErrNoConnectorsDue, got %v", err)
	}
	if locks.tryAcquireCalls != 0 {
		t.Fatalf("expected no lock acquisition, got %d", locks.tryAcquireCalls)
	}
}

func TestTriggerRequestPayloadRoundTrip(t *testing.T) {
	t.Parallel()

	payload, hasPayload, err := encodeTriggerRequestPayload(TriggerRequest{
		ConnectorKind: " GitHub ",
		SourceName:    " Acme ",
	})
	if err != nil {
		t.Fatalf("encode payload error = %v", err)
	}
	if !hasPayload {
		t.Fatalf("expected scoped payload")
	}

	got := decodeTriggerRequestPayload(payload)
	if got.ConnectorKind != "github" || got.SourceName != "Acme" {
		t.Fatalf("decoded payload = %+v", got)
	}
}

func TestDecodeTriggerRequestPayloadMalformedFallsBackToUnscoped(t *testing.T) {
	t.Parallel()

	got := decodeTriggerRequestPayload("{not-json")
	if got.HasConnectorScope() {
		t.Fatalf("expected malformed payload to decode as unscoped request")
	}
}

func TestEnqueueTriggerRequest_DropsUnscopedWhenChannelFull(t *testing.T) {
	t.Parallel()

	ch := make(chan TriggerRequest, 1)
	ch <- TriggerRequest{}

	if !enqueueTriggerRequest(context.Background(), ch, TriggerRequest{}) {
		t.Fatalf("expected unscoped enqueue to return true")
	}

	if len(ch) != 1 {
		t.Fatalf("expected channel to remain full with original request, got %d", len(ch))
	}
}

func TestEnqueueTriggerRequest_WaitsForScopedDelivery(t *testing.T) {
	t.Parallel()

	ch := make(chan TriggerRequest, 1)
	ch <- TriggerRequest{}
	req := TriggerRequest{ConnectorKind: "github", SourceName: "Acme"}

	done := make(chan bool, 1)
	go func() {
		done <- enqueueTriggerRequest(context.Background(), ch, req)
	}()

	select {
	case <-done:
		t.Fatalf("expected scoped enqueue to block while channel is full")
	case <-time.After(20 * time.Millisecond):
	}

	<-ch

	select {
	case ok := <-done:
		if !ok {
			t.Fatalf("expected scoped enqueue to succeed once channel drains")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timed out waiting for scoped enqueue")
	}

	delivered := <-ch
	if delivered.ConnectorKind != "github" || delivered.SourceName != "Acme" {
		t.Fatalf("delivered request = %+v", delivered)
	}
}
