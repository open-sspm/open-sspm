package okta

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
	return configstore.KindOkta
}

func (d *Definition) DisplayName() string {
	return "Okta"
}

func (d *Definition) Role() registry.IntegrationRole {
	return registry.RoleIdP
}

func (d *Definition) DecodeConfig(raw []byte) (any, error) {
	cfg, err := configstore.DecodeOktaConfig(raw)
	if err != nil {
		return nil, err
	}
	return cfg.Normalized(), nil
}

func (d *Definition) ValidateConfig(cfg any) error {
	return cfg.(configstore.OktaConfig).Validate()
}

func (d *Definition) IsConfigured(cfg any) bool {
	c := cfg.(configstore.OktaConfig)
	return c.Domain != "" && c.Token != ""
}

func (d *Definition) SourceName(cfg any) string {
	return cfg.(configstore.OktaConfig).Domain
}

func (d *Definition) DefaultSubtitle() string {
	return "Syncs Okta users and app assignments."
}

func (d *Definition) ConfiguredSubtitle(cfg any) string {
	domain := cfg.(configstore.OktaConfig).Domain
	if domain != "" {
		return "Domain " + domain
	}
	return d.DefaultSubtitle()
}

func (d *Definition) SettingsHref() string {
	return "/settings/connectors?open=okta"
}

func (d *Definition) MetricsProvider() registry.MetricsProvider {
	return &oktaMetrics{}
}

func (d *Definition) NewIntegration(cfg any) (registry.Integration, error) {
	c := cfg.(configstore.OktaConfig)
	client, err := New(c.BaseURL(), c.Token)
	if err != nil {
		return nil, err
	}
	return NewOktaIntegration(client, c.Domain, d.workers, c.DiscoveryEnabled), nil
}

type oktaMetrics struct{}

func (m *oktaMetrics) FetchMetrics(ctx context.Context, q *gen.Queries, sourceName string) (registry.ConnectorMetrics, error) {
	users, err := q.CountIdPUsers(ctx)
	if err != nil {
		return registry.ConnectorMetrics{}, err
	}
	apps, err := q.CountOktaApps(ctx)
	if err != nil {
		return registry.ConnectorMetrics{}, err
	}
	return registry.ConnectorMetrics{
		Total: users,
		Extras: map[string]int64{
			"apps": apps,
		},
	}, nil
}
