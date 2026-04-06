package googleworkspace

import (
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
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
	return registry.DecodeNormalizedConfig(raw, configstore.DecodeGoogleWorkspaceConfig, configstore.GoogleWorkspaceConfig.Normalized)
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
	return registry.NewSourceMetricsProvider(configstore.KindGoogleWorkspace)
}

func (d *Definition) NewIntegration(cfg any) (registry.Integration, error) {
	googleCfg := cfg.(configstore.GoogleWorkspaceConfig).Normalized()
	client, err := NewClient(googleCfg)
	if err != nil {
		return nil, err
	}
	return NewGoogleWorkspaceIntegration(client, googleCfg.CustomerID, googleCfg.PrimaryDomain, googleCfg.DiscoveryEnabled), nil
}
