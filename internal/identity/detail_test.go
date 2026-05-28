package identity

import (
	"testing"
	"time"
)

type testEntitlementItem struct {
	fields EntitlementGroupFields
}

func (i testEntitlementItem) EntitlementGroupFields() EntitlementGroupFields {
	return i.fields
}

func TestIsDormantAt(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	threshold := 60 * 24 * time.Hour

	tests := []struct {
		name       string
		observedAt time.Time
		want       bool
	}{
		{name: "recent", observedAt: now.Add(-24 * time.Hour), want: false},
		{name: "at threshold", observedAt: now.Add(-threshold), want: true},
		{name: "past threshold", observedAt: now.Add(-threshold - time.Hour), want: true},
		{name: "zero observed", observedAt: time.Time{}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := IsDormantAt(now, tt.observedAt, threshold); got != tt.want {
				t.Fatalf("IsDormantAt() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsPrivilegedEntitlement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		permission string
		rawJSON    string
		want       bool
	}{
		{name: "direct admin", permission: "admin", want: true},
		{name: "github maintain", permission: "maintain", want: true},
		{name: "aws administrator access", permission: "AdministratorAccess", want: true},
		{name: "raw role name", rawJSON: `{"role_name":"Global Administrator"}`, want: true},
		{name: "raw permission set", rawJSON: `{"permissionSetName":"PowerUserAccess"}`, want: true},
		{name: "admin view only is not privileged", permission: "admin_view_only", want: false},
		{name: "audit admin is not privileged", permission: "site_admin_audit", want: false},
		{name: "admin contact is not privileged", rawJSON: `{"role_name":"admin_contact"}`, want: false},
		{name: "write is not privileged", permission: "write", want: false},
		{name: "raw write role is not privileged", rawJSON: `{"role_name":"write"}`, want: false},
		{name: "ordinary user", permission: "User", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := IsPrivilegedEntitlement(tt.permission, []byte(tt.rawJSON)); got != tt.want {
				t.Fatalf("IsPrivilegedEntitlement() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBuildEntitlementGroupsResourceModeKeepsSourceInstancesSeparate(t *testing.T) {
	t.Parallel()

	items := []testEntitlementItem{
		{fields: EntitlementGroupFields{
			AccountSourceKind: "entra",
			AccountSourceName: "tenant-a",
			Kind:              "entra_app_role",
			ResourceKind:      "entra_service_principal",
			ResourceID:        "sp-1",
			ResourceLabel:     "Example App",
			ResourceHref:      "/resources/entra/tenant-a/entra_service_principal/sp-1",
			Permission:        "User",
		}},
		{fields: EntitlementGroupFields{
			AccountSourceKind: "entra",
			AccountSourceName: "tenant-b",
			Kind:              "entra_app_role",
			ResourceKind:      "entra_service_principal",
			ResourceID:        "sp-1",
			ResourceLabel:     "Example App",
			ResourceHref:      "/resources/entra/tenant-b/entra_service_principal/sp-1",
			Permission:        "User",
		}},
	}

	groups := BuildEntitlementGroups(items, EntitlementGroupResource)
	if len(groups) != 2 {
		t.Fatalf("groups len = %d, want 2: %+v", len(groups), groups)
	}
	hrefs := map[string]bool{}
	for _, group := range groups {
		hrefs[group.TitleHref] = true
		if group.Subtitle == "" || group.SourceKind != "entra" {
			t.Fatalf("group should preserve source context: %+v", group)
		}
	}
	for _, want := range []string{
		"/resources/entra/tenant-a/entra_service_principal/sp-1",
		"/resources/entra/tenant-b/entra_service_principal/sp-1",
	} {
		if !hrefs[want] {
			t.Fatalf("missing group href %q in %+v", want, groups)
		}
	}
}

func TestBuildEntitlementGroupsResourceModeOmitsSourceScopeWhenUnambiguous(t *testing.T) {
	t.Parallel()

	groups := BuildEntitlementGroups([]testEntitlementItem{
		{fields: EntitlementGroupFields{
			AccountSourceKind: "entra",
			AccountSourceName: "tenant-a",
			Kind:              "entra_app_role",
			ResourceKind:      "entra_service_principal",
			ResourceID:        "sp-1",
			ResourceLabel:     "Example App",
			Permission:        "User",
		}},
	}, EntitlementGroupResource)

	if len(groups) != 1 {
		t.Fatalf("groups len = %d, want 1", len(groups))
	}
	if got, want := groups[0].Subtitle, "entra_app_role"; got != want {
		t.Fatalf("subtitle = %q, want %q", got, want)
	}
}

func TestBuildEntitlementGroupsPreviewOverflow(t *testing.T) {
	t.Parallel()

	items := make([]testEntitlementItem, 12)
	for i := range items {
		items[i] = testEntitlementItem{fields: EntitlementGroupFields{
			AccountSourceKind: "github",
			AccountSourceName: "acme",
			ResourceKind:      "github_repo",
			ResourceID:        "acme/platform",
			ResourceLabel:     "acme/platform",
			Permission:        "read",
		}}
	}

	groups := BuildEntitlementGroups(items, EntitlementGroupResource)
	if len(groups) != 1 {
		t.Fatalf("groups len = %d, want 1", len(groups))
	}
	if got, want := len(groups[0].PreviewItems), 10; got != want {
		t.Fatalf("preview len = %d, want %d", got, want)
	}
	if got, want := groups[0].OverflowCount, 2; got != want {
		t.Fatalf("overflow count = %d, want %d", got, want)
	}
}

func TestBuildSummaryTiles(t *testing.T) {
	t.Parallel()

	tiles := BuildSummaryTiles(2, 7, 3, 1, []string{"entra", "github"}, 2, map[string]int{
		"entra_directory_role": 2,
		"github_repo":          1,
	})
	byLabel := map[string]SummaryTile{}
	for _, tile := range tiles {
		byLabel[tile.Label] = tile
	}

	if byLabel["Source accounts"].Value != "2" || byLabel["Source accounts"].Sublabel != "Microsoft Entra · GitHub" {
		t.Fatalf("unexpected source tile: %+v", byLabel["Source accounts"])
	}
	if byLabel["Access grants"].Sublabel != "across 2 sources" {
		t.Fatalf("unexpected access tile: %+v", byLabel["Access grants"])
	}
	if !byLabel["Admin scopes"].Danger || byLabel["Admin scopes"].Value != "3" {
		t.Fatalf("unexpected admin tile: %+v", byLabel["Admin scopes"])
	}
	if !byLabel["Dormant accounts"].Warn || byLabel["Dormant accounts"].Sublabel != "60d+ inactivity" {
		t.Fatalf("unexpected dormant tile: %+v", byLabel["Dormant accounts"])
	}
}
