package github

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
	return configstore.KindGitHub
}

func (d *Definition) DisplayName() string {
	return "GitHub"
}

func (d *Definition) Role() registry.IntegrationRole {
	return registry.RoleApp
}

func (d *Definition) DecodeConfig(raw []byte) (any, error) {
	cfg, err := configstore.DecodeGitHubConfig(raw)
	if err != nil {
		return nil, err
	}
	return cfg.Normalized(), nil
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

func (d *Definition) DefaultSubtitle() string {
	return "Organization members and permissions."
}

func (d *Definition) ConfiguredSubtitle(cfg any) string {
	org := cfg.(configstore.GitHubConfig).Org
	if org != "" {
		return "Org " + org
	}
	return d.DefaultSubtitle()
}

func (d *Definition) SettingsHref() string {
	return "/settings/connectors?open=github"
}

func (d *Definition) MetricsProvider() registry.MetricsProvider {
	return &githubMetrics{}
}

func (d *Definition) NewIntegration(cfg any) (registry.Integration, error) {
	c := cfg.(configstore.GitHubConfig)
	client, err := New(c.APIBase, c.Token)
	if err != nil {
		return nil, err
	}
	return NewGitHubIntegration(client, c.Org, c.Enterprise, d.workers, c.SCIMEnabled), nil
}

type githubMetrics struct{}

func (m *githubMetrics) FetchMetrics(ctx context.Context, q *gen.Queries, sourceName string) (registry.ConnectorMetrics, error) {
	total, err := q.CountAppUsersBySource(ctx, gen.CountAppUsersBySourceParams{
		SourceKind: "github",
		SourceName: sourceName,
	})
	if err != nil {
		return registry.ConnectorMetrics{}, err
	}
	matched, err := q.CountMatchedAppUsersBySource(ctx, gen.CountMatchedAppUsersBySourceParams{
		SourceKind: "github",
		SourceName: sourceName,
	})
	if err != nil {
		return registry.ConnectorMetrics{}, err
	}
	unmatched, err := q.CountUnmatchedAppUsersBySource(ctx, gen.CountUnmatchedAppUsersBySourceParams{
		SourceKind: "github",
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
