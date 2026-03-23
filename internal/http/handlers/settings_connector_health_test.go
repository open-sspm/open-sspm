package handlers

import (
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/open-sspm/open-sspm/internal/config"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	connregistry "github.com/open-sspm/open-sspm/internal/connectors/registry"
)

type connectorHealthTestDefinition struct {
	kind        string
	displayName string
}

func (d connectorHealthTestDefinition) Kind() string        { return d.kind }
func (d connectorHealthTestDefinition) DisplayName() string { return d.displayName }
func (d connectorHealthTestDefinition) Role() connregistry.IntegrationRole {
	return connregistry.RoleApp
}
func (d connectorHealthTestDefinition) DecodeConfig([]byte) (any, error)              { return nil, nil }
func (d connectorHealthTestDefinition) ValidateConfig(any) error                      { return nil }
func (d connectorHealthTestDefinition) IsConfigured(any) bool                         { return true }
func (d connectorHealthTestDefinition) SourceName(any) string                         { return "" }
func (d connectorHealthTestDefinition) DefaultSubtitle() string                       { return "" }
func (d connectorHealthTestDefinition) ConfiguredSubtitle(any) string                 { return "" }
func (d connectorHealthTestDefinition) SettingsHref() string                          { return "/settings/connectors" }
func (d connectorHealthTestDefinition) MetricsProvider() connregistry.MetricsProvider { return nil }
func (d connectorHealthTestDefinition) NewIntegration(any) (connregistry.Integration, error) {
	return nil, nil
}

func TestSizeConnectorHealthErrorMessage(t *testing.T) {
	t.Run("empty message", func(t *testing.T) {
		preview, full, previewTruncated, fullTruncated := sizeConnectorHealthErrorMessage("   ")
		if preview != "" || full != "" {
			t.Fatalf("expected empty preview/full, got preview=%q full=%q", preview, full)
		}
		if previewTruncated || fullTruncated {
			t.Fatalf("expected no truncation flags, got preview=%v full=%v", previewTruncated, fullTruncated)
		}
	})

	t.Run("preview truncation only", func(t *testing.T) {
		message := strings.Repeat("a", connectorHealthErrorPreviewRunes+5)
		preview, full, previewTruncated, fullTruncated := sizeConnectorHealthErrorMessage(message)

		if utf8.RuneCountInString(preview) != connectorHealthErrorPreviewRunes {
			t.Fatalf("preview rune count = %d, want %d", utf8.RuneCountInString(preview), connectorHealthErrorPreviewRunes)
		}
		if utf8.RuneCountInString(full) != connectorHealthErrorPreviewRunes+5 {
			t.Fatalf("full rune count = %d, want %d", utf8.RuneCountInString(full), connectorHealthErrorPreviewRunes+5)
		}
		if !previewTruncated {
			t.Fatalf("previewTruncated = false, want true")
		}
		if fullTruncated {
			t.Fatalf("fullTruncated = true, want false")
		}
	})

	t.Run("full truncation", func(t *testing.T) {
		message := strings.Repeat("b", connectorHealthErrorFullRunes+17)
		preview, full, previewTruncated, fullTruncated := sizeConnectorHealthErrorMessage(message)

		if utf8.RuneCountInString(full) != connectorHealthErrorFullRunes {
			t.Fatalf("full rune count = %d, want %d", utf8.RuneCountInString(full), connectorHealthErrorFullRunes)
		}
		if utf8.RuneCountInString(preview) != connectorHealthErrorPreviewRunes {
			t.Fatalf("preview rune count = %d, want %d", utf8.RuneCountInString(preview), connectorHealthErrorPreviewRunes)
		}
		if !previewTruncated {
			t.Fatalf("previewTruncated = false, want true")
		}
		if !fullTruncated {
			t.Fatalf("fullTruncated = false, want true")
		}
	})

	t.Run("preserves utf8 and line breaks", func(t *testing.T) {
		message := "line 1\n" + strings.Repeat("界", connectorHealthErrorPreviewRunes+1)
		preview, full, previewTruncated, fullTruncated := sizeConnectorHealthErrorMessage(message)

		if !utf8.ValidString(preview) {
			t.Fatalf("preview is not valid UTF-8")
		}
		if !utf8.ValidString(full) {
			t.Fatalf("full is not valid UTF-8")
		}
		if !strings.Contains(full, "\n") {
			t.Fatalf("expected line break to be preserved in full message")
		}
		if !previewTruncated {
			t.Fatalf("previewTruncated = false, want true")
		}
		if fullTruncated {
			t.Fatalf("fullTruncated = true, want false")
		}
	})
}

func TestConnectorHealthErrorDetailsURL(t *testing.T) {
	url := connectorHealthErrorDetailsURL([]string{"github", "github_discovery", "github"}, "acme org", "GitHub")
	if !strings.HasPrefix(url, "/settings/connector-health/errors?") {
		t.Fatalf("unexpected url prefix: %q", url)
	}
	if !strings.Contains(url, "source_kind=github") {
		t.Fatalf("url missing source_kind: %q", url)
	}
	if !strings.Contains(url, "source_kind=github_discovery") {
		t.Fatalf("url missing discovery source_kind: %q", url)
	}
	if !strings.Contains(url, "source_name=acme+org") {
		t.Fatalf("url missing encoded source_name: %q", url)
	}
	if !strings.Contains(url, "connector_name=GitHub") {
		t.Fatalf("url missing connector_name: %q", url)
	}
}

func TestConnectorHealthLanes_IncludeDiscoveryWhenEnabled(t *testing.T) {
	cfg := config.Config{
		SyncDiscoveryEnabled:  true,
		SyncInterval:          15 * time.Minute,
		SyncDiscoveryInterval: 30 * time.Minute,
	}
	state := connregistry.ConnectorState{
		Definition: connectorHealthTestDefinition{kind: configstore.KindGoogleWorkspace, displayName: "Google Workspace"},
		Config: configstore.GoogleWorkspaceConfig{
			DiscoveryEnabled: true,
		},
	}

	lanes := connectorHealthLanes(cfg, state)
	if len(lanes) != 2 {
		t.Fatalf("lane count = %d, want 2", len(lanes))
	}
	if lanes[0].syncKind != "google_workspace" || lanes[1].syncKind != "google_workspace_discovery" {
		t.Fatalf("lanes = %#v", lanes)
	}
}

func TestConnectorHealthRequestedRollupKeys_IncludeVault(t *testing.T) {
	cfg := config.Config{
		SyncDiscoveryEnabled:  true,
		SyncInterval:          15 * time.Minute,
		SyncDiscoveryInterval: 30 * time.Minute,
	}
	states := []connregistry.ConnectorState{
		{
			Definition: connectorHealthTestDefinition{kind: configstore.KindVault, displayName: "Vault"},
			Configured: true,
			SourceName: "prod-vault",
		},
		{
			Definition: connectorHealthTestDefinition{kind: configstore.KindGoogleWorkspace, displayName: "Google Workspace"},
			Configured: true,
			SourceName: "C0123",
			Config: configstore.GoogleWorkspaceConfig{
				DiscoveryEnabled: true,
			},
		},
	}

	keys := connectorHealthRequestedRollupKeys(cfg, states)
	if len(keys) != 3 {
		t.Fatalf("key count = %d, want 3", len(keys))
	}

	want := map[syncRollupKey]struct{}{
		{kind: configstore.KindVault, name: "prod-vault"}:                     {},
		{kind: configstore.KindGoogleWorkspace, name: "C0123"}:                {},
		{kind: configstore.KindGoogleWorkspace + "_discovery", name: "C0123"}: {},
	}
	for _, key := range keys {
		if _, ok := want[key]; !ok {
			t.Fatalf("unexpected key = %#v", key)
		}
		delete(want, key)
	}
	if len(want) != 0 {
		t.Fatalf("missing keys = %#v", want)
	}
}
