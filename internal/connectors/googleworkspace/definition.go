package googleworkspace

import (
	"context"

	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

type Definition struct{}

func (d *Definition) Kind() string {
	return configstore.KindGoogleWorkspace
}

func (d *Definition) DisplayName() string {
	return "Google Workspace"
}

func (d *Definition) Role() registry.IntegrationRole {
	return registry.RoleApp
}

func (d *Definition) DecodeConfig(raw []byte) (any, error) {
	cfg, err := configstore.DecodeGoogleWorkspaceConfig(raw)
	if err != nil {
		return nil, err
	}
	return cfg.Normalized(), nil
}

func (d *Definition) ValidateConfig(cfg any) error {
	return cfg.(configstore.GoogleWorkspaceConfig).Validate()
}

func (d *Definition) IsConfigured(cfg any) bool {
	c := cfg.(configstore.GoogleWorkspaceConfig)
	if c.CustomerID == "" || c.DelegatedAdminEmail == "" {
		return false
	}
	switch c.AuthType {
	case configstore.GoogleWorkspaceAuthTypeServiceAccountJSON:
		return c.ServiceAccountJSON != ""
	case configstore.GoogleWorkspaceAuthTypeADC:
		return c.ServiceAccountEmail != ""
	default:
		return false
	}
}

func (d *Definition) SourceName(cfg any) string {
	return cfg.(configstore.GoogleWorkspaceConfig).CustomerID
}

func (d *Definition) DefaultSubtitle() string {
	return "Users, groups, OAuth grants, and token audits from Google Workspace."
}

func (d *Definition) ConfiguredSubtitle(cfg any) string {
	googleCfg := cfg.(configstore.GoogleWorkspaceConfig)
	if googleCfg.PrimaryDomain != "" {
		return "Domain " + googleCfg.PrimaryDomain
	}
	if googleCfg.CustomerID != "" {
		return "Customer " + googleCfg.CustomerID
	}
	return d.DefaultSubtitle()
}

func (d *Definition) SettingsHref() string {
	return "/settings/connectors?open=google_workspace"
}

func (d *Definition) MetricsProvider() registry.MetricsProvider {
	return &googleWorkspaceMetrics{}
}

func (d *Definition) NewIntegration(cfg any) (registry.Integration, error) {
	googleCfg := cfg.(configstore.GoogleWorkspaceConfig).Normalized()
	client, err := NewClient(googleCfg)
	if err != nil {
		return nil, err
	}
	return NewGoogleWorkspaceIntegration(client, googleCfg.CustomerID, googleCfg.PrimaryDomain, googleCfg.DiscoveryEnabled), nil
}

type googleWorkspaceMetrics struct{}

func (m *googleWorkspaceMetrics) FetchMetrics(ctx context.Context, q *gen.Queries, sourceName string) (registry.ConnectorMetrics, error) {
	total, err := q.CountAppUsersBySource(ctx, gen.CountAppUsersBySourceParams{
		SourceKind: configstore.KindGoogleWorkspace,
		SourceName: sourceName,
	})
	if err != nil {
		return registry.ConnectorMetrics{}, err
	}
	matched, err := q.CountMatchedAppUsersBySource(ctx, gen.CountMatchedAppUsersBySourceParams{
		SourceKind: configstore.KindGoogleWorkspace,
		SourceName: sourceName,
	})
	if err != nil {
		return registry.ConnectorMetrics{}, err
	}
	unmatched, err := q.CountUnmatchedAppUsersBySource(ctx, gen.CountUnmatchedAppUsersBySourceParams{
		SourceKind: configstore.KindGoogleWorkspace,
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
