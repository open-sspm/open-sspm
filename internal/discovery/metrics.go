package discovery

import (
	"context"
	"strings"
	"time"

	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/metrics"
)

func RefreshMetrics(ctx context.Context, q *gen.Queries, _ time.Time) error {
	if q == nil {
		return nil
	}

	for _, state := range []string{"managed", "unmanaged"} {
		metrics.DiscoveryAppsTotal.WithLabelValues(state).Set(0)
	}
	for _, level := range []string{"low", "medium", "high", "critical"} {
		metrics.DiscoveryHotspotsTotal.WithLabelValues(level).Set(0)
	}

	managedCounts, err := q.CountSaaSAppsGroupedByManagedState(ctx)
	if err != nil {
		return err
	}
	for _, row := range managedCounts {
		state := strings.ToLower(strings.TrimSpace(row.ManagedState))
		if state != "managed" && state != "unmanaged" {
			continue
		}
		metrics.DiscoveryAppsTotal.WithLabelValues(state).Set(float64(row.AppCount))
	}

	riskCounts, err := q.CountSaaSAppsGroupedByRiskLevel(ctx)
	if err != nil {
		return err
	}
	for _, row := range riskCounts {
		level := strings.ToLower(strings.TrimSpace(row.RiskLevel))
		switch level {
		case "high", "critical":
			metrics.DiscoveryHotspotsTotal.WithLabelValues(level).Set(float64(row.AppCount))
		}
	}

	return nil
}
