package handlers

import (
	"reflect"
	"testing"

	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
)

func TestOktaAppListItem(t *testing.T) {
	t.Run("uses fallbacks and infers suggested integration", func(t *testing.T) {
		got := oktaAppListItem(" app-123 ", " ", " Datadog Sandbox ", " ", " ", "")

		want := viewmodels.AppListItem{
			ExternalID:     "app-123",
			Label:          "app-123",
			Name:           "Datadog Sandbox",
			Status:         "—",
			SignOnMode:     "—",
			IntegratedHref: "",
			SuggestedKind:  configstore.KindDatadog,
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("oktaAppListItem() = %#v, want %#v", got, want)
		}
	})

	t.Run("clears suggestions for mapped apps", func(t *testing.T) {
		got := oktaAppListItem("github-sso", "GitHub SSO", "", "active", "saml", configstore.KindGitHub)

		want := viewmodels.AppListItem{
			ExternalID:     "github-sso",
			Label:          "GitHub SSO",
			Name:           "",
			Status:         "active",
			SignOnMode:     "saml",
			IntegratedHref: "/accounts/github",
			SuggestedKind:  "",
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("oktaAppListItem() = %#v, want %#v", got, want)
		}
	})
}

func TestOktaAppSummaryView(t *testing.T) {
	got := oktaAppSummaryView(gen.GetOktaAppByExternalIDWithIntegrationRow{
		ExternalID: " app-123 ",
		Label:      " ",
		Name:       " My App ",
		Status:     " ",
		SignOnMode: "",
	})

	want := viewmodels.OktaAppSummaryView{
		ExternalID: "app-123",
		Label:      "app-123",
		Name:       "My App",
		Status:     "—",
		SignOnMode: "—",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("oktaAppSummaryView() = %#v, want %#v", got, want)
	}
}

func TestOktaAppAssignedAccountView(t *testing.T) {
	t.Run("uses direct assignment fallbacks and summarized permissions", func(t *testing.T) {
		got := oktaAppAssignedAccountView(gen.ListOktaAppAssignedAccountsPageByQueryRow{
			OktaAccountID:         42,
			OktaAccountEmail:      " admin@example.com ",
			OktaAccountExternalID: " user-42 ",
			OktaAccountStatus:     " ",
			Scope:                 " user ",
			ProfileJson:           []byte(`{"role":"admin","active":true,"token":"secret","nested":{"ignored":true}}`),
		}, nil)

		want := viewmodels.OktaAppAssignedAccountView{
			OktaAccountID:         42,
			AccountHref:           "/accounts/okta/42",
			AccountDisplayName:    "admin@example.com",
			AccountEmail:          "admin@example.com",
			OktaAccountExternalID: "user-42",
			OktaAccountStatus:     "—",
			AssignedVia:           "Direct",
			Groups:                nil,
			Permissions: []viewmodels.PermissionBadge{
				{Text: "active: true"},
				{Text: "role: admin"},
			},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("oktaAppAssignedAccountView() = %#v, want %#v", got, want)
		}
	})

	t.Run("sorts granting groups and falls back to external id", func(t *testing.T) {
		got := oktaAppAssignedAccountView(gen.ListOktaAppAssignedAccountsPageByQueryRow{
			OktaAccountID:         7,
			OktaAccountExternalID: " user-07 ",
			Scope:                 "GROUP",
			ProfileJson:           []byte(`{}`),
		}, []string{"Zeta", "Alpha"})

		want := viewmodels.OktaAppAssignedAccountView{
			OktaAccountID:         7,
			AccountHref:           "/accounts/okta/7",
			AccountDisplayName:    "user-07",
			AccountEmail:          "",
			OktaAccountExternalID: "user-07",
			OktaAccountStatus:     "—",
			AssignedVia:           "Group",
			Groups:                []string{"Alpha", "Zeta"},
			Permissions:           nil,
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("oktaAppAssignedAccountView() = %#v, want %#v", got, want)
		}
	})

	t.Run("uses unknown group placeholder when group source is missing", func(t *testing.T) {
		got := oktaAppAssignedAccountView(gen.ListOktaAppAssignedAccountsPageByQueryRow{
			OktaAccountID: 3,
			Scope:         "GROUP",
		}, nil)

		if !reflect.DeepEqual(got.Groups, []string{"(unknown)"}) {
			t.Fatalf("groups = %#v, want %#v", got.Groups, []string{"(unknown)"})
		}
		if got.AccountDisplayName != "—" {
			t.Fatalf("AccountDisplayName = %q, want %q", got.AccountDisplayName, "—")
		}
	})
}
