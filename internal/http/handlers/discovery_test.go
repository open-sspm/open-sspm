package handlers

import (
	"testing"

	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
)

func TestNormalizeDiscoverySourceSelection(t *testing.T) {
	t.Parallel()

	options := []viewmodels.DiscoverySourceOption{
		{SourceKind: "okta", SourceName: "acme.okta.com", Label: "Okta"},
		{SourceKind: "entra", SourceName: "tenant-1", Label: "Microsoft Entra"},
	}

	t.Run("keeps source kind and clears source name", func(t *testing.T) {
		t.Parallel()

		kind, name := normalizeDiscoverySourceSelection("okta", "acme.okta.com", options)
		if kind != "okta" || name != "" {
			t.Fatalf("unexpected selection kind=%q name=%q", kind, name)
		}
	})

	t.Run("maps source name to kind for compatibility", func(t *testing.T) {
		t.Parallel()

		kind, name := normalizeDiscoverySourceSelection("", "tenant-1", options)
		if kind != "entra" || name != "" {
			t.Fatalf("unexpected selection kind=%q name=%q", kind, name)
		}
	})

	t.Run("drops unknown selection", func(t *testing.T) {
		t.Parallel()

		kind, name := normalizeDiscoverySourceSelection("unknown", "missing", options)
		if kind != "" || name != "" {
			t.Fatalf("unexpected selection kind=%q name=%q", kind, name)
		}
	})
}

func TestDiscoverySourceOptionsUsePrimaryLabels(t *testing.T) {
	t.Parallel()

	options := discoverySourceOptions(ConnectorSnapshot{
		Okta:                      configstore.OktaConfig{Domain: "acme.okta.com"},
		OktaConfigured:            true,
		Entra:                     configstore.EntraConfig{TenantID: "tenant-1"},
		EntraConfigured:           true,
		GoogleWorkspace:           configstore.GoogleWorkspaceConfig{CustomerID: "C0123"},
		GoogleWorkspaceConfigured: true,
	})

	if len(options) != 3 {
		t.Fatalf("options length = %d, want 3", len(options))
	}
	if options[0].Label != "Okta" {
		t.Fatalf("okta label = %q, want %q", options[0].Label, "Okta")
	}
	if options[1].Label != "Microsoft Entra" {
		t.Fatalf("entra label = %q, want %q", options[1].Label, "Microsoft Entra")
	}
	if options[2].Label != "Google Workspace" {
		t.Fatalf("google label = %q, want %q", options[2].Label, "Google Workspace")
	}
}

func TestDiscoveryAppSecondaryLabels(t *testing.T) {
	t.Parallel()

	t.Run("returns domain and distinct vendor", func(t *testing.T) {
		t.Parallel()
		domain, vendor := discoveryAppSecondaryLabels("Jira Cloud for Excel", "jira.com", "Atlassian")
		if domain != "jira.com" {
			t.Fatalf("domain = %q, want %q", domain, "jira.com")
		}
		if vendor != "Atlassian" {
			t.Fatalf("vendor = %q, want %q", vendor, "Atlassian")
		}
	})

	t.Run("suppresses vendor when it matches app name", func(t *testing.T) {
		t.Parallel()
		_, vendor := discoveryAppSecondaryLabels("IRS account", "", "IRS account")
		if vendor != "" {
			t.Fatalf("vendor = %q, want empty", vendor)
		}
	})

	t.Run("suppresses vendor when it matches domain token", func(t *testing.T) {
		t.Parallel()
		_, vendor := discoveryAppSecondaryLabels("Jira Cloud for Excel", "jira.com", "Jira")
		if vendor != "" {
			t.Fatalf("vendor = %q, want empty", vendor)
		}
	})
}
