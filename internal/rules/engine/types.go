package engine

import (
	"time"

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
	Datasets runtimev2.DatasetProvider
	Now      func() time.Time
}
