package main

import (
	"context"
	"time"

	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/discovery"
)

func discoveryMetricsRefresh(q *gen.Queries) func(context.Context) error {
	return func(ctx context.Context) error {
		return discovery.RefreshMetrics(ctx, q, time.Now().UTC())
	}
}
