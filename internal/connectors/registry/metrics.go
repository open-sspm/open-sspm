package registry

import (
	"context"
	"strings"

	"github.com/open-sspm/open-sspm/internal/db/gen"
)

// MetricsProvider defines how to fetch metrics for a connector.
type MetricsProvider interface {
	// FetchMetrics returns counts for this connector
	FetchMetrics(ctx context.Context, q *gen.Queries, sourceName string) (ConnectorMetrics, error)
}

// ConnectorMetrics holds the counts for a connector.
type ConnectorMetrics struct {
	Total       int64
	Anchored    int64
	NeedsAnchor int64

	// Connector-specific extras (e.g., Okta apps count)
	Extras map[string]int64
}

func NewSourceMetricsProvider(sourceKind string) MetricsProvider {
	return sourceMetricsProvider{sourceKind: strings.TrimSpace(sourceKind)}
}

type sourceMetricsProvider struct {
	sourceKind string
}

func (m sourceMetricsProvider) FetchMetrics(ctx context.Context, q *gen.Queries, sourceName string) (ConnectorMetrics, error) {
	total, err := q.CountSourceAccountsBySource(ctx, gen.CountSourceAccountsBySourceParams{
		SourceKind: m.sourceKind,
		SourceName: sourceName,
	})
	if err != nil {
		return ConnectorMetrics{}, err
	}
	anchored, err := q.CountAnchoredSourceAccountsBySource(ctx, gen.CountAnchoredSourceAccountsBySourceParams{
		SourceKind: m.sourceKind,
		SourceName: sourceName,
	})
	if err != nil {
		return ConnectorMetrics{}, err
	}
	needsAnchor, err := q.CountSourceAccountsNeedingAnchorBySource(ctx, gen.CountSourceAccountsNeedingAnchorBySourceParams{
		SourceKind: m.sourceKind,
		SourceName: sourceName,
	})
	if err != nil {
		return ConnectorMetrics{}, err
	}
	return ConnectorMetrics{
		Total:       total,
		Anchored:    anchored,
		NeedsAnchor: needsAnchor,
	}, nil
}
