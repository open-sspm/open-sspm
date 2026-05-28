package main

import (
	"context"
	"time"

	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/discovery"
	"github.com/open-sspm/open-sspm/internal/identity"
	"github.com/open-sspm/open-sspm/internal/ingest/inbox"
)

func backgroundMetricsRefresh(q *gen.Queries) func(context.Context) error {
	return func(ctx context.Context) error {
		now := time.Now().UTC()
		if err := discovery.RefreshMetrics(ctx, q, now); err != nil {
			return err
		}
		return identity.RefreshMetrics(ctx, q, now)
	}
}

func eventInboxMetricsRefresh(q *gen.Queries) func(context.Context) error {
	return func(ctx context.Context) error {
		return inbox.RefreshMetrics(ctx, q)
	}
}
