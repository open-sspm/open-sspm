package handlers

import (
	"reflect"
	"testing"

	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
)

func TestCommandSearchShellData(t *testing.T) {
	data := commandSearchShellData("  ")

	if data.Query != "" {
		t.Fatalf("Query = %q, want empty", data.Query)
	}
	if data.Placeholder != commandSearchPlaceholder {
		t.Fatalf("Placeholder = %q, want %q", data.Placeholder, commandSearchPlaceholder)
	}
	if data.AriaLabel != commandSearchAriaLabel {
		t.Fatalf("AriaLabel = %q, want %q", data.AriaLabel, commandSearchAriaLabel)
	}
	if len(data.Notices) != 0 {
		t.Fatalf("Notices len = %d, want 0", len(data.Notices))
	}
	if len(data.Sections) != 0 {
		t.Fatalf("Sections len = %d, want 0", len(data.Sections))
	}
}

func TestCommandActionSection(t *testing.T) {
	t.Run("gates actions by available surfaces", func(t *testing.T) {
		stateView := newTestConnectorStateView(t, testConnectorSpec{
			kind:       configstore.KindGitHub,
			config:     configstore.GitHubConfig{Org: "acme"},
			configured: true,
			enabled:    true,
			sourceName: "acme",
		})

		section := commandActionSection(stateView, "github")
		if section.Key != "actions" || section.Title != "Actions" {
			t.Fatalf("unexpected section metadata: %#v", section)
		}

		gotIDs := make([]string, 0, len(section.Items))
		gotHrefs := make([]string, 0, len(section.Items))
		for _, item := range section.Items {
			gotIDs = append(gotIDs, item.ID)
			gotHrefs = append(gotHrefs, item.Href)
			if !item.ForceVisible {
				t.Fatalf("item %s ForceVisible = false, want true", item.ID)
			}
		}

		wantIDs := []string{
			"cmd-action-identities",
			"cmd-action-app-assets",
		}
		if !reflect.DeepEqual(gotIDs, wantIDs) {
			t.Fatalf("action IDs = %v, want %v", gotIDs, wantIDs)
		}

		wantHrefs := []string{
			"/identities?q=github",
			"/app-assets?q=github",
		}
		if !reflect.DeepEqual(gotHrefs, wantHrefs) {
			t.Fatalf("action hrefs = %v, want %v", gotHrefs, wantHrefs)
		}
	})

	t.Run("uses merged app assets action when google workspace is enabled and configured", func(t *testing.T) {
		stateView := newTestConnectorStateView(t, testConnectorSpec{
			kind:       configstore.KindGoogleWorkspace,
			config:     configstore.GoogleWorkspaceConfig{CustomerID: "C0123", DiscoveryEnabled: true},
			configured: true,
			enabled:    true,
			sourceName: "C0123",
		})

		section := commandActionSection(stateView, "oauth")
		gotIDs := make([]string, 0, len(section.Items))
		for _, item := range section.Items {
			gotIDs = append(gotIDs, item.ID)
		}

		wantIDs := []string{
			"cmd-action-identities",
			"cmd-action-app-assets",
			"cmd-action-discovery-apps",
		}
		if !reflect.DeepEqual(gotIDs, wantIDs) {
			t.Fatalf("action IDs = %v, want %v", gotIDs, wantIDs)
		}
	})
}

func TestCommandSearchErrorData(t *testing.T) {
	stateView := newTestConnectorStateView(t, testConnectorSpec{
		kind:       configstore.KindEntra,
		config:     configstore.EntraConfig{TenantID: "tenant-1"},
		configured: true,
		enabled:    true,
		sourceName: "tenant-1",
	})

	data := commandSearchErrorData("azure", stateView)
	if len(data.Notices) != 1 {
		t.Fatalf("Notices len = %d, want 1", len(data.Notices))
	}
	if data.Notices[0].Primary != "Search unavailable. Open an inventory below." {
		t.Fatalf("notice text = %q", data.Notices[0].Primary)
	}
	if len(data.Sections) != 1 {
		t.Fatalf("Sections len = %d, want 1", len(data.Sections))
	}
	if data.Sections[0].Key != "actions" {
		t.Fatalf("section key = %q, want actions", data.Sections[0].Key)
	}
}
