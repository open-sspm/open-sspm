package engine

import (
	"context"
	"time"

	runtimev1 "github.com/open-sspm/open-sspm-spec/gen/go/opensspm/runtime/v1"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

type Context struct {
	ScopeKind   string
	SourceKind  string
	SourceName  string
	SyncRunID   *int64
	EvaluatedAt time.Time
}

type DatasetProvider interface {
	GetDataset(ctx context.Context, datasetKey string, datasetVersion *int) ([]any, error)
}

type Engine struct {
	Q        *gen.Queries
	Datasets runtimev1.DatasetProvider
	Now      func() time.Time
}
