package github

import (
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

type Definition struct {
	workers int
}

func NewDefinition(workers int) *Definition {
	return &Definition{workers: workers}
}

func (d *Definition) Kind() string {
	return configstore.KindGitHub
}

func (d *Definition) DisplayName() string {
	return "GitHub"
}

func (d *Definition) Role() registry.IntegrationRole {
	return registry.RoleApp
}

func (d *Definition) DecodeConfig(raw []byte) (any, error) {
	return registry.DecodeNormalizedConfig(raw, configstore.DecodeGitHubConfig, configstore.GitHubConfig.Normalized)
}

func (d *Definition) ValidateConfig(cfg any) error {
	return cfg.(configstore.GitHubConfig).Validate()
}

func (d *Definition) IsConfigured(cfg any) bool {
	c := cfg.(configstore.GitHubConfig)
	return c.Token != "" && c.Org != ""
}

func (d *Definition) SourceName(cfg any) string {
	return cfg.(configstore.GitHubConfig).Org
}

func (d *Definition) MetricsProvider() registry.MetricsProvider {
	return registry.NewUserSourceMetricsProvider(configstore.KindGitHub)
}

func (d *Definition) NewIntegration(cfg any) (registry.Integration, error) {
	c := cfg.(configstore.GitHubConfig)
	client, err := New(c.APIBase, c.Token)
	if err != nil {
		return nil, err
	}
	return NewGitHubIntegration(client, c.Org, c.Enterprise, d.workers, c.SCIMEnabled), nil
}
