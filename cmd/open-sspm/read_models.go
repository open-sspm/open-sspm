package main

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/config"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/readmodels"
)

func rebuildStoredReadModels(ctx context.Context, pool *pgxpool.Pool, q *gen.Queries, cfg config.Config) error {
	return readmodels.NewProjector(pool, q, cfg).RebuildAllReadModels(ctx)
}
