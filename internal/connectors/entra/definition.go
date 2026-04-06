package entra

import (
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
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
	return registry.DecodeNormalizedConfig(raw, configstore.DecodeEntraConfig, configstore.EntraConfig.Normalized)
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
	return registry.NewSourceMetricsProvider(configstore.KindEntra)
}

func (d *Definition) NewIntegration(cfg any) (registry.Integration, error) {
	c := cfg.(configstore.EntraConfig)
	client, err := New(c.TenantID, c.ClientID, c.ClientSecret)
	if err != nil {
		return nil, err
	}
	return NewEntraIntegration(client, c.TenantID, c.DiscoveryEnabled), nil
}
