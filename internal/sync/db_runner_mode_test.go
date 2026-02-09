package sync

import (
	"context"
	"errors"
	"testing"

	"github.com/open-sspm/open-sspm/internal/connectors/entra"
	"github.com/open-sspm/open-sspm/internal/connectors/okta"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

type stubIntegration struct {
	kind string
	name string
	role registry.IntegrationRole
}

func (i stubIntegration) Kind() string                   { return i.kind }
func (i stubIntegration) Name() string                   { return i.name }
func (i stubIntegration) Role() registry.IntegrationRole { return i.role }
func (i stubIntegration) InitEvents() []registry.Event   { return nil }
func (i stubIntegration) Run(context.Context, registry.IntegrationDeps) error {
	return nil
}

type stubModeAwareIntegration struct {
	stubIntegration
	supported map[registry.RunMode]bool
}

func (i stubModeAwareIntegration) SupportsRunMode(mode registry.RunMode) bool {
	return i.supported[mode.Normalize()]
}

func TestDBRunner_IntegrationRunSourceKindByMode(t *testing.T) {
	t.Parallel()

	oktaIntegration := stubIntegration{kind: "okta", name: "example.okta.com", role: registry.RoleIdP}
	entraIntegration := stubIntegration{kind: "entra", name: "tenant", role: registry.RoleApp}

	fullRunner := &DBRunner{mode: registry.RunModeFull}
	if got := fullRunner.integrationRunSourceKind(oktaIntegration); got != "okta" {
		t.Fatalf("full okta run kind = %q, want okta", got)
	}
	if got := fullRunner.integrationRunSourceKind(entraIntegration); got != "entra" {
		t.Fatalf("full entra run kind = %q, want entra", got)
	}

	discoveryRunner := &DBRunner{mode: registry.RunModeDiscovery}
	if got := discoveryRunner.integrationRunSourceKind(oktaIntegration); got != "okta_discovery" {
		t.Fatalf("discovery okta run kind = %q, want okta_discovery", got)
	}
	if got := discoveryRunner.integrationRunSourceKind(entraIntegration); got != "entra_discovery" {
		t.Fatalf("discovery entra run kind = %q, want entra_discovery", got)
	}
}

func TestDBRunner_DiscoveryModeFiltering(t *testing.T) {
	t.Parallel()

	discoveryRunner := &DBRunner{mode: registry.RunModeDiscovery}

	plain := stubIntegration{kind: "github", name: "acme", role: registry.RoleApp}
	if discoveryRunner.integrationSupportsRunMode(plain) {
		t.Fatalf("non-mode-aware integration should be skipped in discovery mode")
	}

	modeAwareEnabled := stubModeAwareIntegration{
		stubIntegration: stubIntegration{kind: "okta", name: "example.okta.com", role: registry.RoleIdP},
		supported:       map[registry.RunMode]bool{registry.RunModeDiscovery: true},
	}
	if !discoveryRunner.integrationSupportsRunMode(modeAwareEnabled) {
		t.Fatalf("mode-aware integration should run in discovery mode when enabled")
	}

	modeAwareDisabled := stubModeAwareIntegration{
		stubIntegration: stubIntegration{kind: "okta", name: "example.okta.com", role: registry.RoleIdP},
		supported:       map[registry.RunMode]bool{registry.RunModeDiscovery: false},
	}
	if discoveryRunner.integrationSupportsRunMode(modeAwareDisabled) {
		t.Fatalf("mode-aware integration should be skipped in discovery mode when disabled")
	}

	oktaDiscoveryDisabled := okta.NewOktaIntegration(nil, "example.okta.com", 1, false)
	if discoveryRunner.integrationSupportsRunMode(oktaDiscoveryDisabled) {
		t.Fatalf("okta integration with discovery disabled should be skipped")
	}

	entraDiscoveryDisabled := entra.NewEntraIntegration(nil, "tenant", false)
	if discoveryRunner.integrationSupportsRunMode(entraDiscoveryDisabled) {
		t.Fatalf("entra integration with discovery disabled should be skipped")
	}
}

func TestMatchesRequestedConnectorScope(t *testing.T) {
	t.Parallel()

	if !matchesRequestedConnectorScope("GitHub", "Acme", "github", "acme") {
		t.Fatalf("expected case-insensitive scope match")
	}
	if matchesRequestedConnectorScope("github", "acme", "okta", "acme") {
		t.Fatalf("expected connector kind mismatch")
	}
	if matchesRequestedConnectorScope("github", "acme", "github", "other") {
		t.Fatalf("expected source mismatch")
	}
}

func TestNoIntegrationRunErrorScopedReturnsNoConnectorsDue(t *testing.T) {
	t.Parallel()

	if !errors.Is(noIntegrationRunError(nil, nil, true), ErrNoConnectorsDue) {
		t.Fatalf("expected scoped no-op result to return ErrNoConnectorsDue")
	}
}

func TestConnectorKindMatchesRequestedScope(t *testing.T) {
	t.Parallel()

	if !connectorKindMatchesRequestedScope("github", "github", true) {
		t.Fatalf("expected matching connector kind")
	}
	if connectorKindMatchesRequestedScope("okta", "github", true) {
		t.Fatalf("expected non-matching connector kind to be skipped")
	}
	if !connectorKindMatchesRequestedScope("okta", "github", false) {
		t.Fatalf("expected unscoped runs to include all connector kinds")
	}
}
