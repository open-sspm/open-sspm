package handlers

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

func TestCalendarDateDisplay(t *testing.T) {
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
			if got := calendarDateDisplay(tc.value); got.Label != tc.want {
				t.Fatalf("calendarDateDisplay() label = %q, want %q", got.Label, tc.want)
			}
		})
	}
}

func TestCredentialRiskReasons(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	expiresAt := timestamptz(now.Add(2 * 24 * time.Hour))
	lastUsedAt := timestamptz(now.Add(-120 * 24 * time.Hour))
	signals := credentialRiskSignalsFromStoredJSON([]byte(`[
		{"id":"expiring_within_7_days","severity":"high","title":"Credential expires within 7 days"},
		{"id":"unused_over_90_days","severity":"high","title":"Credential has not been used in over 90 days"},
		{"id":"missing_creator","severity":"high","title":"Creator attribution is missing"}
	]`))
	findings := credentialRiskFindingsFromSignals(signals, expiresAt, lastUsedAt, now)
	if len(findings) < 3 {
		t.Fatalf("expected multiple findings, got %v", findings)
	}
	for _, f := range findings {
		if f.Severity == "" || f.Title == "" {
			t.Fatalf("finding missing severity or title: %+v", f)
		}
	}
}

func TestCredentialRiskReasonsUseFutureAwareExpiryLabel(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)

	expiresAt := timestamptz(now.Add(4 * 24 * time.Hour))
	signals := credentialRiskSignalsFromStoredJSON([]byte(`[
		{"id":"expiring_within_7_days","severity":"high","title":"Credential expires within 7 days"}
	]`))
	findings := credentialRiskFindingsFromSignals(signals, expiresAt, pgtype.Timestamptz{}, now)

	for _, finding := range findings {
		if finding.Title == "Credential expires within 7 days" {
			if finding.Evidence != "Expires in 4d" {
				t.Fatalf("evidence = %q, want %q", finding.Evidence, "Expires in 4d")
			}
			return
		}
	}

	t.Fatalf("missing expiry finding: %+v", findings)
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

func TestAvailableProgrammaticSourcesIncludesConfiguredSources(t *testing.T) {
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
	if sources[0].SourceKind != configstore.KindGitHub || sources[1].SourceKind != configstore.KindEntra {
		t.Fatalf("unexpected source kinds = [%q, %q]", sources[0].SourceKind, sources[1].SourceKind)
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
}

func TestAvailableIdentitySourcePairsRequiresEnabledConnector(t *testing.T) {
	t.Parallel()

	sources := availableIdentitySourcePairs(newTestConnectorStateView(t,
		testConnectorSpec{
			kind:       configstore.KindOkta,
			config:     configstore.OktaConfig{Domain: "acme.okta.com"},
			configured: true,
			enabled:    true,
			sourceName: "acme.okta.com",
		},
		testConnectorSpec{
			kind:       configstore.KindGitHub,
			config:     configstore.GitHubConfig{Org: "disabled-org"},
			configured: true,
			enabled:    false,
			sourceName: "disabled-org",
		},
	))
	if len(sources) != 1 {
		t.Fatalf("sources length = %d, want 1", len(sources))
	}
	if sources[0].SourceKind != "okta" {
		t.Fatalf("source kind = %q, want okta", sources[0].SourceKind)
	}
	if sources[0].SourceName != "acme.okta.com" {
		t.Fatalf("source name = %q, want acme.okta.com", sources[0].SourceName)
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

			gotKind, gotID, gotOK := credentialAssetLookupKeyValues(tc.credential.AssetRefKind, tc.credential.AssetRefExternalID)
			if gotOK != tc.wantOK {
				t.Fatalf("credentialAssetLookupKeyValues() ok = %v, want %v", gotOK, tc.wantOK)
			}
			if gotKind != tc.wantKind {
				t.Fatalf("credentialAssetLookupKeyValues() kind = %q, want %q", gotKind, tc.wantKind)
			}
			if gotID != tc.wantID {
				t.Fatalf("credentialAssetLookupKeyValues() external id = %q, want %q", gotID, tc.wantID)
			}
		})
	}
}

func timestamptz(ts time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: ts.UTC(), Valid: true}
}
