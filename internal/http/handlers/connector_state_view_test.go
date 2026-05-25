package handlers

import (
	"testing"

	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

type testConnectorSpec struct {
	kind       string
	enabled    bool
	configured bool
	sourceName string
	config     any
}

type testConnectorDefinition struct {
	kind string
}

func newTestConnectorStateView(t *testing.T, specs ...testConnectorSpec) connectorStateView {
	t.Helper()

	states := make([]registry.ConnectorState, 0, len(specs))
	for _, spec := range specs {
		states = append(states, registry.ConnectorState{
			Definition: testConnectorDefinition{kind: spec.kind},
			Config:     spec.config,
			Enabled:    spec.enabled,
			Configured: spec.configured,
			SourceName: spec.sourceName,
		})
	}

	return newConnectorStateView(states)
}

func (d testConnectorDefinition) Kind() string { return d.kind }

func (d testConnectorDefinition) DisplayName() string {
	if name := ConnectorDisplayName(d.kind); name != "" {
		return name
	}
	return d.kind
}

func (d testConnectorDefinition) Role() registry.IntegrationRole { return registry.RoleApp }

func (d testConnectorDefinition) DecodeConfig([]byte) (any, error) { return nil, nil }

func (d testConnectorDefinition) ValidateConfig(any) error { return nil }

func (d testConnectorDefinition) IsConfigured(any) bool { return false }

func (d testConnectorDefinition) SourceName(any) string { return "" }

func (d testConnectorDefinition) MetricsProvider() registry.MetricsProvider { return nil }

func (d testConnectorDefinition) NewIntegration(any) (registry.Integration, error) { return nil, nil }
