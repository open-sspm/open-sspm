package handlers

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

func TestIdentityDormancyHandlesInvalidTimestamps(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	if isDormantAt(now, pgtype.Timestamptz{}, 60*24*time.Hour) {
		t.Fatal("invalid timestamp should not be dormant")
	}
}

func TestIdentityEntitlementDormancyUsesAccountLastLogin(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name             string
		accountLastLogin pgtype.Timestamptz
		entObservedAt    pgtype.Timestamptz
		wantDormant      bool
	}{
		{
			name:             "stale account is dormant even when grant was synced recently",
			accountLastLogin: validTimestamptz(now.Add(-61 * 24 * time.Hour)),
			entObservedAt:    validTimestamptz(now.Add(-1 * time.Hour)),
			wantDormant:      true,
		},
		{
			name:             "recent account is active even when grant sync timestamp is old",
			accountLastLogin: validTimestamptz(now.Add(-24 * time.Hour)),
			entObservedAt:    validTimestamptz(now.Add(-90 * 24 * time.Hour)),
			wantDormant:      false,
		},
		{
			name:             "missing account activity is not treated as dormant",
			accountLastLogin: pgtype.Timestamptz{},
			entObservedAt:    validTimestamptz(now.Add(-90 * 24 * time.Hour)),
			wantDormant:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			account := gen.Account{
				ID:          42,
				SourceKind:  "github",
				SourceName:  "acme",
				ExternalID:  "github-user-1",
				LastLoginAt: tt.accountLastLogin,
			}
			entitlement := gen.ListEntitlementsForAccountIDsRow{
				AccountID:      account.ID,
				Kind:           "github_team_repo_permission",
				Resource:       "github_repo:acme/private-repo",
				Permission:     "read",
				LastObservedAt: tt.entObservedAt,
			}

			view := identityEntitlementView(account, entitlement, now)
			if view.Dormant != tt.wantDormant {
				t.Fatalf("Dormant = %v, want %v", view.Dormant, tt.wantDormant)
			}
			if got, want := view.AccountActivityUnix, timestamptzUnix(tt.accountLastLogin); got != want {
				t.Fatalf("AccountActivityUnix = %d, want account last-login unix %d", got, want)
			}
		})
	}
}

func validTimestamptz(t time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: t, Valid: true}
}

func TestMaxIdentityActivityIgnoresSyncObservation(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	accounts := []gen.Account{
		{
			ID:             1,
			LastObservedAt: validTimestamptz(now.Add(-1 * time.Hour)),
		},
		{
			ID:          2,
			LastLoginAt: validTimestamptz(now.Add(-3 * 24 * time.Hour)),
		},
	}

	got := maxIdentityActivity(accounts)
	if !got.Valid {
		t.Fatal("maxIdentityActivity() returned invalid timestamp")
	}
	if want := accounts[1].LastLoginAt.Time; !got.Time.Equal(want) {
		t.Fatalf("maxIdentityActivity() = %s, want last login %s", got.Time, want)
	}

	got = maxIdentityActivity([]gen.Account{{ID: 3, LastObservedAt: validTimestamptz(now)}})
	if got.Valid {
		t.Fatalf("maxIdentityActivity() should ignore sync-only observation, got %s", got.Time)
	}
}

func TestIdentityBreadcrumbKindRoot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		kind          string
		wantRootLabel string
		wantRootHref  string
		wantKindLabel string
		wantKindHref  string
	}{
		{
			kind:          "human",
			wantRootLabel: "Identities",
			wantRootHref:  "/identities",
			wantKindLabel: "Human",
			wantKindHref:  "/identities",
		},
		{
			kind:          "service",
			wantRootLabel: "Non-human identities",
			wantRootHref:  "/non-human-identities",
			wantKindLabel: "Service",
			wantKindHref:  "/non-human-identities",
		},
		{
			kind:          "bot",
			wantRootLabel: "Non-human identities",
			wantRootHref:  "/non-human-identities",
			wantKindLabel: "Bot",
			wantKindHref:  "/non-human-identities",
		},
	}

	for _, tt := range tests {
		t.Run(tt.kind, func(t *testing.T) {
			t.Parallel()

			rootLabel, rootHref, kindLabel, kindHref := identityBreadcrumbKind(tt.kind)
			if rootLabel != tt.wantRootLabel || rootHref != tt.wantRootHref || kindLabel != tt.wantKindLabel || kindHref != tt.wantKindHref {
				t.Fatalf("identityBreadcrumbKind(%q) = (%q, %q, %q, %q), want (%q, %q, %q, %q)",
					tt.kind,
					rootLabel,
					rootHref,
					kindLabel,
					kindHref,
					tt.wantRootLabel,
					tt.wantRootHref,
					tt.wantKindLabel,
					tt.wantKindHref,
				)
			}
		})
	}
}
