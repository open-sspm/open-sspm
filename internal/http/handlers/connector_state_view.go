package handlers

import (
	"context"
	"strings"

	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

type connectorStateView struct {
	byKind map[string]*registry.ConnectorState
}

type typedConnectorState[T any] struct {
	state *registry.ConnectorState
}

func (h *Handlers) LoadConnectorStateView(ctx context.Context) (connectorStateView, error) {
	if h.Registry == nil {
		return connectorStateView{}, nil
	}

	states, err := h.Registry.LoadStates(ctx, h.Q)
	if err != nil {
		return connectorStateView{}, err
	}

	return newConnectorStateView(states), nil
}

func newConnectorStateView(states []registry.ConnectorState) connectorStateView {
	copied := append([]registry.ConnectorState(nil), states...)
	view := connectorStateView{
		byKind: make(map[string]*registry.ConnectorState, len(copied)),
	}

	for idx := range copied {
		state := &copied[idx]
		kind := NormalizeConnectorKind(state.Definition.Kind())
		if kind == "" {
			continue
		}
		view.byKind[kind] = state
	}

	return view
}

func (v connectorStateView) Raw(kind string) *registry.ConnectorState {
	kind = NormalizeConnectorKind(kind)
	if kind == "" {
		return nil
	}
	return v.byKind[kind]
}

func (v connectorStateView) Configured(kind string) bool {
	state := v.Raw(kind)
	return state != nil && state.Configured
}

func (v connectorStateView) Enabled(kind string) bool {
	state := v.Raw(kind)
	return state != nil && state.Enabled
}

func (v connectorStateView) SourceName(kind string) string {
	state := v.Raw(kind)
	if state == nil {
		return ""
	}
	return strings.TrimSpace(state.SourceName)
}

func (v connectorStateView) Okta() typedConnectorState[configstore.OktaConfig] {
	return newTypedConnectorState[configstore.OktaConfig](v.Raw(configstore.KindOkta))
}

func (v connectorStateView) GoogleWorkspace() typedConnectorState[configstore.GoogleWorkspaceConfig] {
	return newTypedConnectorState[configstore.GoogleWorkspaceConfig](v.Raw(configstore.KindGoogleWorkspace))
}

func (v connectorStateView) GitHub() typedConnectorState[configstore.GitHubConfig] {
	return newTypedConnectorState[configstore.GitHubConfig](v.Raw(configstore.KindGitHub))
}

func (v connectorStateView) Datadog() typedConnectorState[configstore.DatadogConfig] {
	return newTypedConnectorState[configstore.DatadogConfig](v.Raw(configstore.KindDatadog))
}

func (v connectorStateView) AWSIdentityCenter() typedConnectorState[configstore.AWSIdentityCenterConfig] {
	return newTypedConnectorState[configstore.AWSIdentityCenterConfig](v.Raw(configstore.KindAWSIdentityCenter))
}

func (v connectorStateView) Entra() typedConnectorState[configstore.EntraConfig] {
	return newTypedConnectorState[configstore.EntraConfig](v.Raw(configstore.KindEntra))
}

func (v connectorStateView) Vault() typedConnectorState[configstore.VaultConfig] {
	return newTypedConnectorState[configstore.VaultConfig](v.Raw(configstore.KindVault))
}

func newTypedConnectorState[T any](state *registry.ConnectorState) typedConnectorState[T] {
	return typedConnectorState[T]{state: state}
}

func (s typedConnectorState[T]) Config() T {
	var zero T
	if s.state == nil {
		return zero
	}
	cfg, ok := s.state.Config.(T)
	if !ok {
		return zero
	}
	return cfg
}

func (s typedConnectorState[T]) Configured() bool {
	return s.state != nil && s.state.Configured
}

func (s typedConnectorState[T]) Enabled() bool {
	return s.state != nil && s.state.Enabled
}

func (s typedConnectorState[T]) SourceName() string {
	if s.state == nil {
		return ""
	}
	return strings.TrimSpace(s.state.SourceName)
}

func querySourceKind(kind string) string {
	switch NormalizeConnectorKind(kind) {
	case configstore.KindAWSIdentityCenter:
		return "aws"
	default:
		return NormalizeConnectorKind(kind)
	}
}
