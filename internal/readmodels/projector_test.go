package readmodels

import (
	"context"
	"testing"
	"time"

	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

func TestBuildSourceStateUsesSecretPresenceForConfiguredConnectors(t *testing.T) {
	t.Parallel()

	raw, err := configstore.EncodeConfig(configstore.OktaConfig{
		Domain:           "acme.okta.com",
		DiscoveryEnabled: true,
	})
	if err != nil {
		t.Fatalf("EncodeConfig(): %v", err)
	}

	state, ok, err := buildSourceState(gen.ConnectorConfig{
		Kind:    configstore.KindOkta,
		Enabled: true,
		Config:  raw,
	}, map[string]bool{"token": true})
	if err != nil {
		t.Fatalf("buildSourceState(): %v", err)
	}
	if !ok {
		t.Fatalf("buildSourceState() ok = false, want true")
	}
	if state.sourceKind != configstore.KindOkta {
		t.Fatalf("sourceKind = %q, want %q", state.sourceKind, configstore.KindOkta)
	}
	if state.sourceName != "acme.okta.com" {
		t.Fatalf("sourceName = %q, want %q", state.sourceName, "acme.okta.com")
	}
	if !state.enabled {
		t.Fatalf("enabled = false, want true")
	}
	if !state.configured {
		t.Fatalf("configured = false, want true")
	}
	if !state.discoveryEnabled {
		t.Fatalf("discoveryEnabled = false, want true")
	}
}

func TestBuildSourceStateReturnsUnconfiguredStateForInvalidConfig(t *testing.T) {
	t.Parallel()

	raw, err := configstore.EncodeConfig(configstore.GitHubConfig{
		Org: "acme",
	})
	if err != nil {
		t.Fatalf("EncodeConfig(): %v", err)
	}

	state, ok, err := buildSourceState(gen.ConnectorConfig{
		Kind:    configstore.KindGitHub,
		Enabled: false,
		Config:  raw,
	}, nil)
	if err != nil {
		t.Fatalf("buildSourceState(): %v", err)
	}
	if !ok {
		t.Fatalf("buildSourceState() ok = false, want true")
	}
	if state.sourceKind != configstore.KindGitHub {
		t.Fatalf("sourceKind = %q, want %q", state.sourceKind, configstore.KindGitHub)
	}
	if state.sourceName != "acme" {
		t.Fatalf("sourceName = %q, want %q", state.sourceName, "acme")
	}
	if state.enabled {
		t.Fatalf("enabled = true, want false")
	}
	if state.configured {
		t.Fatalf("configured = true, want false")
	}
	if state.discoveryEnabled {
		t.Fatalf("discoveryEnabled = true, want false")
	}
}

func TestBuildSourceStateNormalizesAWSSourceKind(t *testing.T) {
	t.Parallel()

	raw, err := configstore.EncodeConfig(configstore.AWSIdentityCenterConfig{
		Region: "eu-west-1",
	})
	if err != nil {
		t.Fatalf("EncodeConfig(): %v", err)
	}

	state, ok, err := buildSourceState(gen.ConnectorConfig{
		Kind:    "aws",
		Enabled: true,
		Config:  raw,
	}, nil)
	if err != nil {
		t.Fatalf("buildSourceState(): %v", err)
	}
	if !ok {
		t.Fatalf("buildSourceState() ok = false, want true")
	}
	if state.sourceKind != "aws" {
		t.Fatalf("sourceKind = %q, want %q", state.sourceKind, "aws")
	}
	if state.sourceName != "eu-west-1" {
		t.Fatalf("sourceName = %q, want %q", state.sourceName, "eu-west-1")
	}
	if !state.configured {
		t.Fatalf("configured = false, want true")
	}
}

func TestFreshnessWindowUsesConnectorSpecificOverrides(t *testing.T) {
	t.Parallel()

	cfg := RefreshConfig{
		SyncInterval:      10 * time.Minute,
		SyncEntraInterval: 20 * time.Minute,
		SyncAWSInterval:   45 * time.Minute,
	}

	if got := FreshnessWindow(cfg, configstore.KindGitHub); got != 30*time.Minute {
		t.Fatalf("FreshnessWindow(github) = %s, want %s", got, 30*time.Minute)
	}
	if got := FreshnessWindow(cfg, configstore.KindEntra); got != 40*time.Minute {
		t.Fatalf("FreshnessWindow(entra) = %s, want %s", got, 40*time.Minute)
	}
	if got := FreshnessWindow(cfg, "aws"); got != 90*time.Minute {
		t.Fatalf("FreshnessWindow(aws) = %s, want %s", got, 90*time.Minute)
	}
}

func TestProjectorFromContextReturnsNilWithoutConfig(t *testing.T) {
	t.Parallel()

	if got := ProjectorFromContext(context.Background(), &gen.Queries{}); got != nil {
		t.Fatalf("ProjectorFromContext() = %#v, want nil", got)
	}
}

func TestProjectorFromContextBuildsProjectorWithConfig(t *testing.T) {
	t.Parallel()

	cfg := RefreshConfig{
		SyncInterval: 15 * time.Minute,
	}
	projector := ProjectorFromContext(WithRefreshConfig(context.Background(), cfg), &gen.Queries{})
	if projector == nil {
		t.Fatalf("ProjectorFromContext() = nil, want projector")
	}
	if got := FreshnessWindow(projector.cfg, configstore.KindOkta); got != 30*time.Minute {
		t.Fatalf("projector freshness window = %s, want %s", got, 30*time.Minute)
	}
}
