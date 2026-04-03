package main

import (
	"context"
	"time"

	"github.com/open-sspm/open-sspm/internal/config"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/discovery"
)

func discoveryMetricsRefresh(q *gen.Queries, cfg config.Config) func(context.Context) error {
	return func(ctx context.Context) error {
		return discovery.RefreshMetrics(ctx, q, cfg, time.Now().UTC())
	}
}
