package entra

import (
	"context"

	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

type Definition struct{}

func (d *Definition) Kind() string {
	return configstore.KindEntra
}

func (d *Definition) DisplayName() string {
	return "Microsoft Entra ID"
}

func (d *Definition) Role() registry.IntegrationRole {
	return registry.RoleApp
}

func (d *Definition) DecodeConfig(raw []byte) (any, error) {
	cfg, err := configstore.DecodeEntraConfig(raw)
	if err != nil {
		return nil, err
	}
	return cfg.Normalized(), nil
}

func (d *Definition) ValidateConfig(cfg any) error {
	return cfg.(configstore.EntraConfig).Validate()
}

func (d *Definition) IsConfigured(cfg any) bool {
	c := cfg.(configstore.EntraConfig)
	return c.TenantID != "" && c.ClientID != "" && c.ClientSecret != ""
}

func (d *Definition) SourceName(cfg any) string {
	return cfg.(configstore.EntraConfig).TenantID
}

func (d *Definition) DefaultSubtitle() string {
	return "Users and access via Microsoft Graph."
}

func (d *Definition) ConfiguredSubtitle(cfg any) string {
	tenantID := cfg.(configstore.EntraConfig).TenantID
	if tenantID != "" {
		return "Tenant " + tenantID
	}
	return d.DefaultSubtitle()
}

func (d *Definition) SettingsHref() string {
	return "/settings/connectors?open=entra"
}

func (d *Definition) MetricsProvider() registry.MetricsProvider {
	return &entraMetrics{}
}

func (d *Definition) NewIntegration(cfg any) (registry.Integration, error) {
	c := cfg.(configstore.EntraConfig)
	client, err := New(c.TenantID, c.ClientID, c.ClientSecret)
	if err != nil {
		return nil, err
	}
	return NewEntraIntegration(client, c.TenantID, c.DiscoveryEnabled), nil
}

type entraMetrics struct{}

func (m *entraMetrics) FetchMetrics(ctx context.Context, q *gen.Queries, sourceName string) (registry.ConnectorMetrics, error) {
	total, err := q.CountAppUsersBySource(ctx, gen.CountAppUsersBySourceParams{
		SourceKind: "entra",
		SourceName: sourceName,
	})
	if err != nil {
		return registry.ConnectorMetrics{}, err
	}
	matched, err := q.CountMatchedAppUsersBySource(ctx, gen.CountMatchedAppUsersBySourceParams{
		SourceKind: "entra",
		SourceName: sourceName,
	})
	if err != nil {
		return registry.ConnectorMetrics{}, err
	}
	unmatched, err := q.CountUnmatchedAppUsersBySource(ctx, gen.CountUnmatchedAppUsersBySourceParams{
		SourceKind: "entra",
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
