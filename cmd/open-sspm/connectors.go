package main

import (
	"github.com/open-sspm/open-sspm/internal/config"
	"github.com/open-sspm/open-sspm/internal/connectors/aws"
	"github.com/open-sspm/open-sspm/internal/connectors/datadog"
	"github.com/open-sspm/open-sspm/internal/connectors/entra"
	"github.com/open-sspm/open-sspm/internal/connectors/github"
	"github.com/open-sspm/open-sspm/internal/connectors/okta"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
	"github.com/open-sspm/open-sspm/internal/connectors/vault"
)

func buildConnectorRegistry(cfg config.Config) (*registry.ConnectorRegistry, error) {
	reg := registry.NewRegistry()
	if err := reg.Register(okta.NewDefinition(cfg.SyncOktaWorkers)); err != nil {
		return nil, err
	}
	if err := reg.Register(&entra.Definition{}); err != nil {
		return nil, err
	}
	if err := reg.Register(github.NewDefinition(cfg.SyncGitHubWorkers)); err != nil {
		return nil, err
	}
	if err := reg.Register(datadog.NewDefinition(cfg.SyncDatadogWorkers)); err != nil {
		return nil, err
	}
	if err := reg.Register(&aws.Definition{}); err != nil {
		return nil, err
	}
	if err := reg.Register(&vault.Definition{}); err != nil {
		return nil, err
	}
	return reg, nil
}
