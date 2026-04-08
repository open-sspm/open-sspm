package datadog

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
	return configstore.KindDatadog
}

func (d *Definition) DisplayName() string {
	return "Datadog"
}

func (d *Definition) Role() registry.IntegrationRole {
	return registry.RoleApp
}

func (d *Definition) DecodeConfig(raw []byte) (any, error) {
	return registry.DecodeNormalizedConfig(raw, configstore.DecodeDatadogConfig, configstore.DatadogConfig.Normalized)
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
	return registry.NewSourceMetricsProvider(configstore.KindDatadog)
}

func (d *Definition) NewIntegration(cfg any) (registry.Integration, error) {
	c := cfg.(configstore.DatadogConfig)
	adapter, err := newSDKAdapter(c)
	if err != nil {
		return nil, err
	}
	return NewDatadogIntegration(adapter, c.Site, d.workers), nil
}
