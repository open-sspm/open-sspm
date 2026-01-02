package datadog

import (
	"context"

	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

type Definition struct {
	workers int
}

func NewDefinition(workers int) *Definition {
	return &Definition{workers: workers}
}

func (d *Definition) Kind() string {
	return configstore.KindDatadog
}

func (d *Definition) DisplayName() string {
	return "Datadog"
}

func (d *Definition) Role() registry.IntegrationRole {
	return registry.RoleApp
}

func (d *Definition) DecodeConfig(raw []byte) (any, error) {
	cfg, err := configstore.DecodeDatadogConfig(raw)
	if err != nil {
		return nil, err
	}
	return cfg.Normalized(), nil
}

func (d *Definition) ValidateConfig(cfg any) error {
	return cfg.(configstore.DatadogConfig).Validate()
}

func (d *Definition) IsConfigured(cfg any) bool {
	c := cfg.(configstore.DatadogConfig)
	return c.APIKey != "" && c.AppKey != ""
}

func (d *Definition) SourceName(cfg any) string {
	return cfg.(configstore.DatadogConfig).Site
}

func (d *Definition) DefaultSubtitle() string {
	return "Users and roles."
}

func (d *Definition) ConfiguredSubtitle(cfg any) string {
	site := cfg.(configstore.DatadogConfig).Site
	if site != "" {
		return "Site " + site
	}
	return d.DefaultSubtitle()
}

func (d *Definition) SettingsHref() string {
	return "/settings/connectors?open=datadog"
}

func (d *Definition) MetricsProvider() registry.MetricsProvider {
	return &datadogMetrics{}
}

func (d *Definition) NewIntegration(cfg any) (registry.Integration, error) {
	c := cfg.(configstore.DatadogConfig)
	client, err := New(c.APIBaseURL(), c.APIKey, c.AppKey)
	if err != nil {
		return nil, err
	}
	return NewDatadogIntegration(client, c.Site, d.workers), nil
}

type datadogMetrics struct{}

func (m *datadogMetrics) FetchMetrics(ctx context.Context, q *gen.Queries, sourceName string) (registry.ConnectorMetrics, error) {
	total, err := q.CountAppUsersBySource(ctx, gen.CountAppUsersBySourceParams{
		SourceKind: "datadog",
		SourceName: sourceName,
	})
	if err != nil {
		return registry.ConnectorMetrics{}, err
	}
	matched, err := q.CountMatchedAppUsersBySource(ctx, gen.CountMatchedAppUsersBySourceParams{
		SourceKind: "datadog",
		SourceName: sourceName,
	})
	if err != nil {
		return registry.ConnectorMetrics{}, err
	}
	unmatched, err := q.CountUnmatchedAppUsersBySource(ctx, gen.CountUnmatchedAppUsersBySourceParams{
		SourceKind: "datadog",
		SourceName: sourceName,
	})
	if err != nil {
		return registry.ConnectorMetrics{}, err
	}
	return registry.ConnectorMetrics{
		Total:     total,
		Matched:   matched,
		Unmatched: unmatched,
	}, nil
}
