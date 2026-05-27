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
	return registry.DecodeNormalizedConfig(raw, configstore.DecodeVaultConfig, configstore.VaultConfig.Normalized)
}

func (d *Definition) ValidateConfig(cfg any) error {
	return cfg.(configstore.VaultConfig).Validate()
}

func (d *Definition) IsConfigured(cfg any) bool {
	c := cfg.(configstore.VaultConfig).Normalized()
	if c.Address == "" {
		return false
	}
	switch c.AuthType {
	case configstore.VaultAuthTypeToken:
		return c.Token != ""
	case configstore.VaultAuthTypeAppRole:
		return c.AppRoleRoleID != "" && c.AppRoleSecretID != ""
	default:
		return false
	}
}

func (d *Definition) SourceName(cfg any) string {
	return cfg.(configstore.VaultConfig).SourceName()
}

func (d *Definition) MetricsProvider() registry.MetricsProvider {
	return registry.NewSourceMetricsProvider(configstore.KindVault)
}

func (d *Definition) NewIntegration(cfg any) (registry.Integration, error) {
	c := cfg.(configstore.VaultConfig).Normalized()
	client, err := New(Options{
		Address:          c.Address,
		Namespace:        c.Namespace,
		AuthType:         c.AuthType,
		Token:            c.Token,
		AppRoleMountPath: c.AppRoleMountPath,
		AppRoleRoleID:    c.AppRoleRoleID,
		AppRoleSecretID:  c.AppRoleSecretID,
		TLSSkipVerify:    c.TLSSkipVerify,
		TLSCACertPEM:     c.TLSCACertPEM,
	})
	if err != nil {
		return nil, err
	}
	return NewVaultIntegration(client, c.SourceName(), c.ScanAuthRoles), nil
}
