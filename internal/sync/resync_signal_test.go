package sync

import (
	"context"
	"errors"
	"strings"
	"testing"

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
