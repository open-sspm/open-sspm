package registry

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

type fakeDB struct {
	execErr error
}

func (f fakeDB) Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, f.execErr
}

func (f fakeDB) Query(context.Context, string, ...interface{}) (pgx.Rows, error) {
	panic("unexpected Query call")
}

func (f fakeDB) QueryRow(context.Context, string, ...interface{}) pgx.Row {
	panic("unexpected QueryRow call")
}

func TestFailSyncRunReturnsOriginalErrorWhenPersisted(t *testing.T) {
	runErr := errors.New("sync failed")
	q := gen.New(fakeDB{})

	err := FailSyncRun(context.Background(), q, 42, runErr, SyncErrorKindAPI)
	if !errors.Is(err, runErr) {
		t.Fatalf("expected joined error to include run error")
	}
}

func TestFailSyncRunJoinsPersistError(t *testing.T) {
	runErr := errors.New("sync failed")
	q := gen.New(fakeDB{execErr: errors.New("db unavailable")})

	err := FailSyncRun(context.Background(), q, 42, runErr, SyncErrorKindAPI)
	if !errors.Is(err, runErr) {
		t.Fatalf("expected joined error to include run error")
	}
	if err == nil || err.Error() == runErr.Error() {
		t.Fatalf("expected persist failure details in returned error")
	}
}

func TestFailSyncRunMissingQueries(t *testing.T) {
	runErr := errors.New("sync failed")

	err := FailSyncRun(context.Background(), nil, 42, runErr, SyncErrorKindAPI)
	if !errors.Is(err, runErr) {
		t.Fatalf("expected joined error to include run error")
	}
	if err == nil || err.Error() == runErr.Error() {
		t.Fatalf("expected missing query details in returned error")
	}
}

func TestMarshalJSONPanicsOnUnsupportedValue(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("expected panic for unsupported json value")
		}
	}()
	_ = MarshalJSON(func() {})
}
