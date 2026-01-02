package vault

import (
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

type Definition struct{}

func (d *Definition) Kind() string {
	return configstore.KindVault
}

func (d *Definition) DisplayName() string {
	return "Vault"
}

func (d *Definition) Role() registry.IntegrationRole {
	return registry.RoleApp
}

func (d *Definition) DecodeConfig(raw []byte) (any, error) {
	cfg, err := configstore.DecodeVaultConfig(raw)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

func (d *Definition) ValidateConfig(cfg any) error {
	return cfg.(configstore.VaultConfig).Validate()
}

func (d *Definition) IsConfigured(cfg any) bool {
	return true
}

func (d *Definition) SourceName(cfg any) string {
	return ""
}

func (d *Definition) DefaultSubtitle() string {
	return "Reserved for future integration."
}

func (d *Definition) ConfiguredSubtitle(cfg any) string {
	return d.DefaultSubtitle()
}

func (d *Definition) SettingsHref() string {
	return "/settings/connectors?open=vault"
}

func (d *Definition) MetricsProvider() registry.MetricsProvider {
	return nil
}

func (d *Definition) NewIntegration(cfg any) (registry.Integration, error) {
	return nil, nil
}
