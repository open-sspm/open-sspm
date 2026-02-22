package handlers

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
)

func TestIdentityNamePrimary(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name         string
		displayName  string
		primaryEmail string
		id           int64
		want         string
	}{
		{
			name:         "uses display name when present",
			displayName:  "Alice Doe",
			primaryEmail: "alice@example.com",
			id:           101,
			want:         "Alice Doe",
		},
		{
			name:         "falls back to email",
			displayName:  "",
			primaryEmail: "alice@example.com",
			id:           101,
			want:         "alice@example.com",
		},
		{
			name:         "falls back to identity id label",
			displayName:  " ",
			primaryEmail: " ",
			id:           101,
			want:         "Identity 101",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := identityNamePrimary(tc.displayName, tc.primaryEmail, tc.id); got != tc.want {
				t.Fatalf("identityNamePrimary(%q, %q, %d) = %q, want %q", tc.displayName, tc.primaryEmail, tc.id, got, tc.want)
			}
		})
	}
}

func TestIdentityNameSecondary(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name         string
		displayName  string
		primaryEmail string
		want         string
	}{
		{
			name:         "shows email when distinct from display name",
			displayName:  "Alice Doe",
			primaryEmail: "alice@example.com",
			want:         "alice@example.com",
		},
		{
			name:         "hides email when display name missing",
			displayName:  "",
			primaryEmail: "alice@example.com",
			want:         "",
		},
		{
			name:         "hides email when equal ignoring case",
			displayName:  "Alice@example.com",
			primaryEmail: "alice@example.com",
			want:         "",
		},
		{
			name:         "hides email when empty",
			displayName:  "Alice Doe",
			primaryEmail: "",
			want:         "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := identityNameSecondary(tc.displayName, tc.primaryEmail); got != tc.want {
				t.Fatalf("identityNameSecondary(%q, %q) = %q, want %q", tc.displayName, tc.primaryEmail, got, tc.want)
			}
		})
	}
}

func TestIdentityCalendarDate(t *testing.T) {
	t.Parallel()

	got := identityCalendarDate(pgtype.Timestamptz{
		Time:  time.Date(2026, time.February, 8, 23, 11, 0, 0, time.FixedZone("PST", -8*60*60)),
		Valid: true,
	})
	if got != "Feb 9, 2026" {
		t.Fatalf("identityCalendarDate(valid) = %q, want %q", got, "Feb 9, 2026")
	}

	if got := identityCalendarDate(pgtype.Timestamptz{}); got != "—" {
		t.Fatalf("identityCalendarDate(invalid) = %q, want %q", got, "—")
	}
}

func TestAvailableIdentitySourcePairsIncludesGoogleWorkspace(t *testing.T) {
	t.Parallel()

	sources := availableIdentitySourcePairs(ConnectorSnapshot{
		GoogleWorkspace:           configstore.GoogleWorkspaceConfig{CustomerID: "C0123"},
		GoogleWorkspaceConfigured: true,
	})
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
