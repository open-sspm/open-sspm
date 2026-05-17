package tail

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

type CursorStore struct {
	pool *pgxpool.Pool
}

type CursorKey struct {
	SourceKind string
	SourceName string
	Resource   string
	CursorKind string
}

type LockedCursorFunc func(context.Context, *gen.Queries, gen.ConnectorCursorState) error

func NewCursorStore(pool *pgxpool.Pool) *CursorStore {
	return &CursorStore{pool: pool}
}

func (s *CursorStore) WithLockedCursor(ctx context.Context, key CursorKey, fn LockedCursorFunc) error {
	if s == nil || s.pool == nil {
		return errors.New("cursor store is not configured")
	}
	if fn == nil {
		return errors.New("locked cursor callback is required")
	}
	key = normalizeCursorKey(key)
	if err := validateCursorKey(key); err != nil {
		return err
	}
	baseQ := gen.New(s.pool)
	if err := ensureCursorState(ctx, baseQ, key); err != nil {
		return err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin cursor lock: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()
	qtx := gen.New(tx)
	state, err := qtx.GetConnectorCursorStateForUpdate(ctx, gen.GetConnectorCursorStateForUpdateParams{
		SourceKind: key.SourceKind,
		SourceName: key.SourceName,
		Resource:   key.Resource,
	})
	if err != nil {
		return fmt.Errorf("lock cursor state: %w", err)
	}
	if err := fn(ctx, qtx, state); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit cursor lock: %w", err)
	}
	return nil
}

func ensureCursorState(ctx context.Context, q *gen.Queries, key CursorKey) error {
	if _, err := q.GetConnectorCursorState(ctx, gen.GetConnectorCursorStateParams{
		SourceKind: key.SourceKind,
		SourceName: key.SourceName,
		Resource:   key.Resource,
	}); err == nil {
		return nil
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("get cursor state: %w", err)
	}
	if err := q.UpsertConnectorCursorState(ctx, gen.UpsertConnectorCursorStateParams{
		SourceKind:          key.SourceKind,
		SourceName:          key.SourceName,
		Resource:            key.Resource,
		CursorKind:          key.CursorKind,
		CursorJson:          []byte(`{}`),
		LastError:           "",
		LastProviderEventID: "",
		NeedsFullResync:     false,
	}); err != nil {
		return fmt.Errorf("initialize cursor state: %w", err)
	}
	return nil
}

func normalizeCursorKey(key CursorKey) CursorKey {
	key.SourceKind = strings.ToLower(strings.TrimSpace(key.SourceKind))
	key.SourceName = strings.TrimSpace(key.SourceName)
	key.Resource = strings.TrimSpace(key.Resource)
	key.CursorKind = strings.TrimSpace(key.CursorKind)
	if key.CursorKind == "" {
		key.CursorKind = "unknown"
	}
	return key
}

func validateCursorKey(key CursorKey) error {
	if key.SourceKind == "" {
		return errors.New("cursor source kind is required")
	}
	if key.SourceName == "" {
		return errors.New("cursor source name is required")
	}
	if key.Resource == "" {
		return errors.New("cursor resource is required")
	}
	return nil
}
