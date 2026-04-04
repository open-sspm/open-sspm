package handlers

import (
	"net/http"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
)

func TestNormalizeCredentialRiskFilter(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		raw  string
		want string
	}{
		{name: "critical", raw: "critical", want: "critical"},
		{name: "mixed case", raw: "High", want: "high"},
		{name: "trimmed", raw: "  medium  ", want: "medium"},
		{name: "invalid", raw: "urgent", want: ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := normalizeCredentialRiskFilter(tc.raw); got != tc.want {
				t.Fatalf("normalizeCredentialRiskFilter(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}

func TestFormatProgrammaticDate(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		value pgtype.Timestamptz
		want  string
	}{
		{
			name:  "valid",
			value: timestamptz(time.Date(2026, 2, 14, 18, 45, 0, 0, time.UTC)),
			want:  "Feb 14, 2026",
		},
		{
			name:  "invalid",
			value: pgtype.Timestamptz{},
			want:  "—",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := formatProgrammaticDate(tc.value); got != tc.want {
				t.Fatalf("formatProgrammaticDate() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestCredentialRiskReasons(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	credential := gen.CredentialArtifact{
		Status:           "active",
		CredentialKind:   "github_pat_fine_grained",
		ExpiresAtSource:  timestamptz(now.Add(2 * 24 * time.Hour)),
		LastUsedAtSource: timestamptz(now.Add(-120 * 24 * time.Hour)),
	}

	reasons := credentialRiskReasons(credential, now)
	if len(reasons) < 3 {
		t.Fatalf("expected multiple reasons, got %v", reasons)
	}
}

func TestEmailCandidate(t *testing.T) {
	t.Parallel()

	cases := []struct {
		raw  string
		want string
	}{
		{raw: "alice@example.com", want: "alice@example.com"},
		{raw: "Alice <alice@example.com>", want: "alice@example.com"},
		{raw: "not-an-email", want: ""},
	}
	for _, tc := range cases {
		if got := emailCandidate(tc.raw); got != tc.want {
			t.Fatalf("emailCandidate(%q) = %q, want %q", tc.raw, got, tc.want)
		}
	}
}

func TestSelectProgrammaticSource(t *testing.T) {
	t.Parallel()

	sources := []viewmodels.ProgrammaticSourceOption{
		{SourceKind: "entra", SourceName: "tenant-1", Label: "Microsoft Entra"},
		{SourceKind: "github", SourceName: "acme", Label: "GitHub"},
	}

	t.Run("defaults to all configured when no source query is present", func(t *testing.T) {
		t.Parallel()

		c, _ := newTestContext(http.MethodGet, "/credentials")
		selected, ok := selectProgrammaticSource(c, sources)
		if !ok {
			t.Fatalf("expected source selection to be available")
		}
		if selected.SourceKind != "" || selected.SourceName != "" {
			t.Fatalf("expected empty selection for all configured, got kind=%q name=%q", selected.SourceKind, selected.SourceName)
		}
	})

	t.Run("selects source kind when kind and name are provided", func(t *testing.T) {
		t.Parallel()

		c, _ := newTestContext(http.MethodGet, "/credentials?source_kind=github&source_name=acme")
		selected, ok := selectProgrammaticSource(c, sources)
		if !ok {
			t.Fatalf("expected source selection to be available")
		}
		if selected.SourceKind != "github" || selected.SourceName != "" {
			t.Fatalf("unexpected selection kind=%q name=%q", selected.SourceKind, selected.SourceName)
		}
	})

	t.Run("maps source name to source kind for backward compatibility", func(t *testing.T) {
		t.Parallel()

		c, _ := newTestContext(http.MethodGet, "/credentials?source_name=acme")
		selected, ok := selectProgrammaticSource(c, sources)
		if !ok {
			t.Fatalf("expected source selection to be available")
		}
		if selected.SourceKind != "github" || selected.SourceName != "" {
			t.Fatalf("unexpected selection kind=%q name=%q", selected.SourceKind, selected.SourceName)
		}
	})
}

func TestAvailableProgrammaticSourcesUsesPrimaryLabels(t *testing.T) {
	t.Parallel()

	sources := availableProgrammaticSources(newTestConnectorStateView(t,
		testConnectorSpec{
			kind:       configstore.KindEntra,
			config:     configstore.EntraConfig{TenantID: "tenant-a"},
			enabled:    true,
			configured: true,
			sourceName: "tenant-a",
		},
		testConnectorSpec{
			kind:       configstore.KindGitHub,
			config:     configstore.GitHubConfig{Org: "acme-org"},
			enabled:    true,
			configured: true,
			sourceName: "acme-org",
		},
	))
	if len(sources) != 2 {
		t.Fatalf("sources length = %d, want 2", len(sources))
	}
	if sources[0].Label != "GitHub" || sources[1].Label != "Microsoft Entra" {
		t.Fatalf("unexpected labels = [%q, %q]", sources[0].Label, sources[1].Label)
	}
	if sources[0].SourceName != "acme-org" || sources[1].SourceName != "tenant-a" {
		t.Fatalf("unexpected source names = [%q, %q]", sources[0].SourceName, sources[1].SourceName)
	}
}

func TestAvailableProgrammaticSourcesIncludesVault(t *testing.T) {
	t.Parallel()

	sources := availableProgrammaticSources(newTestConnectorStateView(t, testConnectorSpec{
		kind: configstore.KindVault,
		config: configstore.VaultConfig{
			Address: "https://vault.example.com",
			Name:    "prod-vault",
		},
		enabled:    true,
		configured: true,
		sourceName: "prod-vault",
	}))
	if len(sources) != 1 {
		t.Fatalf("sources length = %d, want 1", len(sources))
	}
	if sources[0].SourceKind != "vault" {
		t.Fatalf("source kind = %q, want vault", sources[0].SourceKind)
	}
	if sources[0].SourceName != "prod-vault" {
		t.Fatalf("source name = %q, want prod-vault", sources[0].SourceName)
	}
	if sources[0].Label != "Vault" {
		t.Fatalf("source label = %q, want Vault", sources[0].Label)
	}
}

func TestAvailableProgrammaticSourcesIncludesGoogleWorkspace(t *testing.T) {
	t.Parallel()

	sources := availableProgrammaticSources(newTestConnectorStateView(t, testConnectorSpec{
		kind:       configstore.KindGoogleWorkspace,
		config:     configstore.GoogleWorkspaceConfig{CustomerID: "C0123"},
		configured: true,
		enabled:    true,
		sourceName: "C0123",
	}))
	if len(sources) != 1 {
		t.Fatalf("sources length = %d, want 1", len(sources))
	}
	if sources[0].SourceKind != configstore.KindGoogleWorkspace {
		t.Fatalf("source kind = %q, want %q", sources[0].SourceKind, configstore.KindGoogleWorkspace)
	}
	if sources[0].SourceName != "C0123" {
		t.Fatalf("source name = %q, want C0123", sources[0].SourceName)
	}
	if sources[0].Label != "Google Workspace" {
		t.Fatalf("source label = %q, want Google Workspace", sources[0].Label)
	}
}

func TestConfiguredProgrammaticSourcesIncludesDisabledConfiguredConnector(t *testing.T) {
	t.Parallel()

	sources := configuredProgrammaticSources(newTestConnectorStateView(t, testConnectorSpec{
		kind:       configstore.KindGitHub,
		config:     configstore.GitHubConfig{Org: "acme-org"},
		configured: true,
		enabled:    false,
		sourceName: "acme-org",
	}))
	if len(sources) != 1 {
		t.Fatalf("sources length = %d, want 1", len(sources))
	}
	if sources[0].SourceKind != "github" {
		t.Fatalf("source kind = %q, want github", sources[0].SourceKind)
	}
	if sources[0].SourceName != "acme-org" {
		t.Fatalf("source name = %q, want acme-org", sources[0].SourceName)
	}
}

func TestAppAssetCredentialRefGoogleWorkspace(t *testing.T) {
	t.Parallel()

	refKind, refExternalID := appAssetCredentialRef(gen.AppAsset{
		SourceKind: configstore.KindGoogleWorkspace,
		AssetKind:  "google_oauth_client",
		ExternalID: "client-123",
	})
	if refKind != "google_oauth_client" {
		t.Fatalf("ref kind = %q, want google_oauth_client", refKind)
	}
	if refExternalID != "google_oauth_client:client-123" {
		t.Fatalf("ref external id = %q, want google_oauth_client:client-123", refExternalID)
	}
}

func TestAppAssetCredentialRefsGoogleWorkspaceIncludesGoogleRef(t *testing.T) {
	t.Parallel()

	refs := appAssetCredentialRefs(gen.AppAsset{
		SourceKind: configstore.KindGoogleWorkspace,
		SourceName: "C0123",
		AssetKind:  "google_oauth_client",
		ExternalID: "client-123",
	})
	if len(refs) == 0 {
		t.Fatalf("expected refs for google workspace asset")
	}

	seen := map[string]struct{}{}
	for _, ref := range refs {
		seen[ref.AssetRefKind+"|"+ref.AssetRefExternalID] = struct{}{}
	}
	if _, ok := seen["google_oauth_client|google_oauth_client:client-123"]; !ok {
		t.Fatalf("missing google workspace-specific credential ref")
	}
}

func TestCredentialAssetLookupKey(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		credential gen.CredentialArtifact
		wantKind   string
		wantID     string
		wantOK     bool
	}{
		{
			name: "app asset ref with embedded asset kind",
			credential: gen.CredentialArtifact{
				AssetRefKind:       "app_asset",
				AssetRefExternalID: "google_oauth_client:client-123",
			},
			wantKind: "google_oauth_client",
			wantID:   "client-123",
			wantOK:   true,
		},
		{
			name: "google workspace specific asset ref kind",
			credential: gen.CredentialArtifact{
				AssetRefKind:       "google_oauth_client",
				AssetRefExternalID: "google_oauth_client:client-123",
			},
			wantKind: "google_oauth_client",
			wantID:   "client-123",
			wantOK:   true,
		},
		{
			name: "concrete asset ref kind without embedded prefix",
			credential: gen.CredentialArtifact{
				AssetRefKind:       "google_oauth_client",
				AssetRefExternalID: "client-123",
			},
			wantKind: "google_oauth_client",
			wantID:   "client-123",
			wantOK:   true,
		},
		{
			name: "app asset ref without asset kind stays unresolved",
			credential: gen.CredentialArtifact{
				AssetRefKind:       "app_asset",
				AssetRefExternalID: "client-123",
			},
			wantOK: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			gotKind, gotID, gotOK := credentialAssetLookupKey(tc.credential)
			if gotOK != tc.wantOK {
				t.Fatalf("credentialAssetLookupKey() ok = %v, want %v", gotOK, tc.wantOK)
			}
			if gotKind != tc.wantKind {
				t.Fatalf("credentialAssetLookupKey() kind = %q, want %q", gotKind, tc.wantKind)
			}
			if gotID != tc.wantID {
				t.Fatalf("credentialAssetLookupKey() external id = %q, want %q", gotID, tc.wantID)
			}
		})
	}
}

func timestamptz(ts time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: ts.UTC(), Valid: true}
}
