package handlers

import (
	"net/http"
	"testing"

	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
)

func TestProgrammaticSourceSelectionRules(t *testing.T) {
	t.Parallel()

	multiKindSources := []viewmodels.ProgrammaticSourceOption{
		{SourceKind: "github", SourceName: "acme", Label: "GitHub"},
		{SourceKind: "github", SourceName: "beta", Label: "GitHub"},
		{SourceKind: "entra", SourceName: "tenant-1", Label: "Microsoft Entra"},
	}
	singleKindSources := []viewmodels.ProgrammaticSourceOption{
		{SourceKind: "github", SourceName: "acme", Label: "GitHub"},
		{SourceKind: "entra", SourceName: "tenant-1", Label: "Microsoft Entra"},
	}
	ambiguousNameSources := []viewmodels.ProgrammaticSourceOption{
		{SourceKind: "github", SourceName: "shared-source", Label: "GitHub"},
		{SourceKind: "entra", SourceName: "shared-source", Label: "Microsoft Entra"},
	}

	tests := []struct {
		name     string
		target   string
		sources  []viewmodels.ProgrammaticSourceOption
		wantKind string
		wantName string
	}{
		{
			name:     "exact pair match",
			target:   "/credentials?source_kind=github&source_name=acme",
			sources:  multiKindSources,
			wantKind: "github",
			wantName: "acme",
		},
		{
			name:     "kind only single match",
			target:   "/credentials?source_kind=entra",
			sources:  singleKindSources,
			wantKind: "entra",
			wantName: "tenant-1",
		},
		{
			name:     "kind only multi match returns kind aggregate",
			target:   "/credentials?source_kind=github",
			sources:  multiKindSources,
			wantKind: "github",
			wantName: "",
		},
		{
			name:     "name only unique match",
			target:   "/credentials?source_name=tenant-1",
			sources:  multiKindSources,
			wantKind: "entra",
			wantName: "tenant-1",
		},
		{
			name:     "name only ambiguous match falls back to all configured",
			target:   "/credentials?source_name=shared-source",
			sources:  ambiguousNameSources,
			wantKind: "",
			wantName: "",
		},
		{
			name:     "invalid pair falls back to all configured",
			target:   "/credentials?source_kind=github&source_name=tenant-1",
			sources:  multiKindSources,
			wantKind: "",
			wantName: "",
		},
		{
			name:     "unknown kind falls back to all configured",
			target:   "/credentials?source_kind=unknown",
			sources:  multiKindSources,
			wantKind: "",
			wantName: "",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c, _ := newTestContext(http.MethodGet, tc.target)
			selected, ok := selectProgrammaticSource(c, tc.sources)
			if !ok {
				t.Fatalf("expected source selection to be available")
			}
			if selected.SourceKind != tc.wantKind || selected.SourceName != tc.wantName {
				t.Fatalf("selection kind=%q name=%q, want kind=%q name=%q", selected.SourceKind, selected.SourceName, tc.wantKind, tc.wantName)
			}
		})
	}
}

func TestAvailableProgrammaticSourcesIncludesConfiguredDisabled(t *testing.T) {
	t.Parallel()

	sources := availableProgrammaticSources(ConnectorSnapshot{
		Entra:            configstore.EntraConfig{TenantID: "tenant-a"},
		EntraEnabled:     false,
		EntraConfigured:  true,
		GitHub:           configstore.GitHubConfig{Org: "acme-org"},
		GitHubEnabled:    false,
		GitHubConfigured: true,
	})

	if len(sources) != 2 {
		t.Fatalf("sources length = %d, want 2", len(sources))
	}

	seen := make(map[string]string, len(sources))
	for _, source := range sources {
		seen[source.SourceKind] = source.SourceName
	}
	if seen["github"] != "acme-org" {
		t.Fatalf("github source name = %q, want %q", seen["github"], "acme-org")
	}
	if seen["entra"] != "tenant-a" {
		t.Fatalf("entra source name = %q, want %q", seen["entra"], "tenant-a")
	}
}
