package main

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/config"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

type runtimeDependencies struct {
	pool             *pgxpool.Pool
	queries          *gen.Queries
	connectorConfigs *configstore.Store
}

func openRuntimeDependencies(ctx context.Context, cfg config.Config) (*runtimeDependencies, error) {
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}
	queries := gen.New(pool)
	connectorConfigs := configstore.NewStore(pool, queries, cfg.ConnectorSecretKey)
	if err := connectorConfigs.Bootstrap(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &runtimeDependencies{
		pool:             pool,
		queries:          queries,
		connectorConfigs: connectorConfigs,
	}, nil
}
