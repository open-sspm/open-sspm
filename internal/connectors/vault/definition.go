package vault

import (
	"context"
	"strings"

	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/db/gen"
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
	return cfg.Normalized(), nil
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

func (d *Definition) DefaultSubtitle() string {
	return "Identity entities, policies, mounts, and auth roles."
}

func (d *Definition) ConfiguredSubtitle(cfg any) string {
	c := cfg.(configstore.VaultConfig).Normalized()
	source := strings.TrimSpace(c.SourceName())
	if source == "" {
		return d.DefaultSubtitle()
	}
	return "Source " + source
}

func (d *Definition) SettingsHref() string {
	return "/settings/connectors?open=vault"
}

func (d *Definition) MetricsProvider() registry.MetricsProvider {
	return &vaultMetrics{}
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

type vaultMetrics struct{}

func (m *vaultMetrics) FetchMetrics(ctx context.Context, q *gen.Queries, sourceName string) (registry.ConnectorMetrics, error) {
	total, err := q.CountAppUsersBySource(ctx, gen.CountAppUsersBySourceParams{
		SourceKind: "vault",
		SourceName: sourceName,
	})
	if err != nil {
		return registry.ConnectorMetrics{}, err
	}
	matched, err := q.CountMatchedAppUsersBySource(ctx, gen.CountMatchedAppUsersBySourceParams{
		SourceKind: "vault",
		SourceName: sourceName,
	})
	if err != nil {
		return registry.ConnectorMetrics{}, err
	}
	unmatched, err := q.CountUnmatchedAppUsersBySource(ctx, gen.CountUnmatchedAppUsersBySourceParams{
		SourceKind: "vault",
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
