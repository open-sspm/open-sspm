package engine

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	runtimev2 "github.com/open-sspm/open-sspm-spec/gen/go/opensspm/runtime/v2"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

type Context struct {
	ScopeKind   string
	SourceKind  string
	SourceName  string
	SyncRunID   *int64
	EvaluatedAt time.Time
}

type Engine struct {
	Q        *gen.Queries
	DB       TxBeginner
	Datasets runtimev2.DatasetProvider
	Now      func() time.Time
}

type TxBeginner interface {
	Begin(context.Context) (pgx.Tx, error)
}
