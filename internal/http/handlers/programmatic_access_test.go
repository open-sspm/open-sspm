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

func TestCredentialRiskLevel(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 2, 7, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name       string
		credential gen.CredentialArtifact
		want       string
	}{
		{
			name: "critical when expired and active",
			credential: gen.CredentialArtifact{
				Status:              "active",
				CredentialKind:      "entra_client_secret",
				CreatedByExternalID: "owner@example.com",
				ExpiresAtSource:     timestamptz(now.Add(-1 * time.Hour)),
			},
			want: "critical",
		},
		{
			name: "critical when high privilege has no attribution",
			credential: gen.CredentialArtifact{
				Status:         "active",
				CredentialKind: "github_pat_fine_grained",
			},
			want: "critical",
		},
		{
			name: "high when expiring within seven days",
			credential: gen.CredentialArtifact{
				Status:              "active",
				CredentialKind:      "entra_certificate",
				CreatedByExternalID: "owner@example.com",
				ExpiresAtSource:     timestamptz(now.Add(3 * 24 * time.Hour)),
			},
			want: "high",
		},
		{
			name: "medium when expiring within thirty days",
			credential: gen.CredentialArtifact{
				Status:              "active",
				CredentialKind:      "entra_certificate",
				CreatedByExternalID: "owner@example.com",
				ExpiresAtSource:     timestamptz(now.Add(20 * 24 * time.Hour)),
			},
			want: "medium",
		},
		{
			name: "low when healthy",
			credential: gen.CredentialArtifact{
				Status:               "active",
				CredentialKind:       "entra_certificate",
				CreatedByExternalID:  "owner@example.com",
				ApprovedByExternalID: "approver@example.com",
				ExpiresAtSource:      timestamptz(now.Add(60 * 24 * time.Hour)),
				LastUsedAtSource:     timestamptz(now.Add(-10 * 24 * time.Hour)),
			},
			want: "low",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := credentialRiskLevel(tc.credential, now); got != tc.want {
				t.Fatalf("credentialRiskLevel() = %q, want %q", got, tc.want)
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

	sources := availableProgrammaticSources(ConnectorSnapshot{
		Entra:            configstore.EntraConfig{TenantID: "tenant-a"},
		EntraEnabled:     true,
		EntraConfigured:  true,
		GitHub:           configstore.GitHubConfig{Org: "acme-org"},
		GitHubEnabled:    true,
		GitHubConfigured: true,
	})
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

func timestamptz(ts time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: ts.UTC(), Valid: true}
}
