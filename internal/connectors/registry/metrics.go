package registry

import (
	"context"

	"github.com/open-sspm/open-sspm/internal/db/gen"
)

// MetricsProvider defines how to fetch metrics for a connector.
type MetricsProvider interface {
	// FetchMetrics returns counts for this connector
	FetchMetrics(ctx context.Context, q *gen.Queries, sourceName string) (ConnectorMetrics, error)
}

// ConnectorMetrics holds the counts for a connector.
type ConnectorMetrics struct {
	Total     int64
	Matched   int64
	Unmatched int64

	// Connector-specific extras (e.g., Okta apps count)
	Extras map[string]int64
}
