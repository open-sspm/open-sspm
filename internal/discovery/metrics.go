package discovery

import (
	"context"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/open-sspm/open-sspm/internal/config"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/metrics"
)

type MetricsCutoffs struct {
	OktaFreshAfter            pgtype.Timestamptz
	EntraFreshAfter           pgtype.Timestamptz
	GoogleWorkspaceFreshAfter pgtype.Timestamptz
	GithubFreshAfter          pgtype.Timestamptz
	DatadogFreshAfter         pgtype.Timestamptz
	AwsFreshAfter             pgtype.Timestamptz
	DefaultFreshAfter         pgtype.Timestamptz
}

func RefreshMetrics(ctx context.Context, q *gen.Queries, cfg config.Config, now time.Time) error {
	if q == nil {
		return nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}

	configuredSourceKinds, configuredSourceNames, err := configuredDiscoverySourcePairs(ctx, q)
	if err != nil {
		return err
	}
	cutoffs := metricsCutoffs(cfg, now)

	for _, state := range []string{"managed", "unmanaged"} {
		metrics.DiscoveryAppsTotal.WithLabelValues(state).Set(0)
	}
	for _, level := range []string{"low", "medium", "high", "critical"} {
		metrics.DiscoveryHotspotsTotal.WithLabelValues(level).Set(0)
	}

	managedCounts, err := q.CountSaaSAppsGroupedByManagedState(ctx, gen.CountSaaSAppsGroupedByManagedStateParams{
		ConfiguredSourceKinds:     configuredSourceKinds,
		ConfiguredSourceNames:     configuredSourceNames,
		OktaFreshAfter:            cutoffs.OktaFreshAfter,
		EntraFreshAfter:           cutoffs.EntraFreshAfter,
		GoogleWorkspaceFreshAfter: cutoffs.GoogleWorkspaceFreshAfter,
		GithubFreshAfter:          cutoffs.GithubFreshAfter,
		DatadogFreshAfter:         cutoffs.DatadogFreshAfter,
		AwsFreshAfter:             cutoffs.AwsFreshAfter,
		DefaultFreshAfter:         cutoffs.DefaultFreshAfter,
	})
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

	riskCounts, err := q.CountSaaSAppsGroupedByRiskLevel(ctx, gen.CountSaaSAppsGroupedByRiskLevelParams{
		ConfiguredSourceKinds:     configuredSourceKinds,
		ConfiguredSourceNames:     configuredSourceNames,
		OktaFreshAfter:            cutoffs.OktaFreshAfter,
		EntraFreshAfter:           cutoffs.EntraFreshAfter,
		GoogleWorkspaceFreshAfter: cutoffs.GoogleWorkspaceFreshAfter,
		GithubFreshAfter:          cutoffs.GithubFreshAfter,
		DatadogFreshAfter:         cutoffs.DatadogFreshAfter,
		AwsFreshAfter:             cutoffs.AwsFreshAfter,
		DefaultFreshAfter:         cutoffs.DefaultFreshAfter,
	})
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

func configuredDiscoverySourcePairs(ctx context.Context, q *gen.Queries) ([]string, []string, error) {
	rows, err := q.ListConnectorConfigs(ctx)
	if err != nil {
		return nil, nil, err
	}

	type sourcePair struct {
		kind string
		name string
	}
	configured := map[string]string{}
	for _, row := range rows {
		kind := strings.ToLower(strings.TrimSpace(row.Kind))
		switch kind {
		case configstore.KindOkta:
			cfg, err := configstore.DecodeOktaConfig(row.Config)
			if err != nil {
				continue
			}
			cfg = cfg.Normalized()
			if cfg.Validate() == nil && cfg.Domain != "" {
				configured[kind] = cfg.Domain
			}
		case configstore.KindEntra:
			cfg, err := configstore.DecodeEntraConfig(row.Config)
			if err != nil {
				continue
			}
			cfg = cfg.Normalized()
			if cfg.Validate() == nil && cfg.TenantID != "" {
				configured[kind] = cfg.TenantID
			}
		case configstore.KindGoogleWorkspace:
			cfg, err := configstore.DecodeGoogleWorkspaceConfig(row.Config)
			if err != nil {
				continue
			}
			cfg = cfg.Normalized()
			if cfg.Validate() == nil && cfg.CustomerID != "" {
				configured[kind] = cfg.CustomerID
			}
		}
	}

	pairs := []sourcePair{}
	for _, kind := range []string{configstore.KindOkta, configstore.KindEntra, configstore.KindGoogleWorkspace} {
		if name := strings.TrimSpace(configured[kind]); name != "" {
			pairs = append(pairs, sourcePair{kind: kind, name: name})
		}
	}

	kinds := make([]string, 0, len(pairs))
	names := make([]string, 0, len(pairs))
	for _, pair := range pairs {
		kinds = append(kinds, pair.kind)
		names = append(names, pair.name)
	}
	return kinds, names, nil
}

func metricsCutoffs(cfg config.Config, now time.Time) MetricsCutoffs {
	return MetricsCutoffs{
		OktaFreshAfter:            pgtype.Timestamptz{Time: now.Add(-metricsFreshnessWindow(cfg, configstore.KindOkta)), Valid: true},
		EntraFreshAfter:           pgtype.Timestamptz{Time: now.Add(-metricsFreshnessWindow(cfg, configstore.KindEntra)), Valid: true},
		GoogleWorkspaceFreshAfter: pgtype.Timestamptz{Time: now.Add(-metricsFreshnessWindow(cfg, configstore.KindGoogleWorkspace)), Valid: true},
		GithubFreshAfter:          pgtype.Timestamptz{Time: now.Add(-metricsFreshnessWindow(cfg, configstore.KindGitHub)), Valid: true},
		DatadogFreshAfter:         pgtype.Timestamptz{Time: now.Add(-metricsFreshnessWindow(cfg, configstore.KindDatadog)), Valid: true},
		AwsFreshAfter:             pgtype.Timestamptz{Time: now.Add(-metricsFreshnessWindow(cfg, configstore.KindAWSIdentityCenter)), Valid: true},
		DefaultFreshAfter:         pgtype.Timestamptz{Time: now.Add(-metricsFreshnessWindow(cfg, "")), Valid: true},
	}
}

func metricsFreshnessWindow(cfg config.Config, kind string) time.Duration {
	interval := cfg.SyncInterval
	switch kind {
	case configstore.KindOkta:
		if cfg.SyncOktaInterval > 0 {
			interval = cfg.SyncOktaInterval
		}
	case configstore.KindEntra:
		if cfg.SyncEntraInterval > 0 {
			interval = cfg.SyncEntraInterval
		}
	case configstore.KindGoogleWorkspace:
		if cfg.SyncGoogleWorkspaceInterval > 0 {
			interval = cfg.SyncGoogleWorkspaceInterval
		}
	case configstore.KindGitHub:
		if cfg.SyncGitHubInterval > 0 {
			interval = cfg.SyncGitHubInterval
		}
	case configstore.KindDatadog:
		if cfg.SyncDatadogInterval > 0 {
			interval = cfg.SyncDatadogInterval
		}
	case configstore.KindAWSIdentityCenter:
		if cfg.SyncAWSInterval > 0 {
			interval = cfg.SyncAWSInterval
		}
	}
	if interval <= 0 {
		interval = 15 * time.Minute
	}
	return max(interval*2, 30*time.Minute)
}
