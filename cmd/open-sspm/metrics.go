package main

import (
	"context"
	"time"

	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/discovery"
	"github.com/open-sspm/open-sspm/internal/nonhumanaccess"
)

func discoveryMetricsRefresh(q *gen.Queries) func(context.Context) error {
	return func(ctx context.Context) error {
		now := time.Now().UTC()
		if err := discovery.RefreshMetrics(ctx, q, now); err != nil {
			return err
		}
		return nonhumanaccess.RefreshMetrics(ctx, q, now)
	}
}
