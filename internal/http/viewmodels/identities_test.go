package viewmodels

import "testing"

func TestBuildIdentityShowHrefOmitsDefaults(t *testing.T) {
	t.Parallel()

	got := BuildIdentityShowHref("/identities/42", IdentityShowQuery{
		Group:       IdentityEntitlementGroupResource,
		AccountSort: IdentityLinkedAccountSortGrants,
	})
	if got != "/identities/42" {
		t.Fatalf("href = %q, want base path", got)
	}
}

func TestBuildIdentityShowHrefRoundTripsFilters(t *testing.T) {
	t.Parallel()

	got := BuildIdentityShowHref("/identities/42", IdentityShowQuery{
		Group:              IdentityEntitlementGroupKind,
		AccountQuery:       "alice@example.com",
		EntitlementQuery:   "admin role",
		AccountSort:        IdentityLinkedAccountSortActivity,
		EntitlementAdmin:   true,
		EntitlementDormant: true,
		EntitlementSource:  "entra",
	})
	want := "/identities/42?account_q=alice%40example.com&account_sort=activity&admin=1&dormant=1&entitlement_q=admin+role&group=kind&source_kind=entra"
	if got != want {
		t.Fatalf("href = %q, want %q", got, want)
	}
}

func TestParseLinkedAccountSortMode(t *testing.T) {
	t.Parallel()

	tests := map[string]IdentityLinkedAccountSortMode{
		"":         IdentityLinkedAccountSortGrants,
		"grants":   IdentityLinkedAccountSortGrants,
		"source":   IdentityLinkedAccountSortSource,
		"activity": IdentityLinkedAccountSortActivity,
		"unknown":  IdentityLinkedAccountSortGrants,
	}
	for raw, want := range tests {
		raw, want := raw, want
		t.Run(raw, func(t *testing.T) {
			t.Parallel()
			if got := ParseLinkedAccountSortMode(raw); got != want {
				t.Fatalf("ParseLinkedAccountSortMode(%q) = %q, want %q", raw, got, want)
			}
		})
	}
}
