package viewmodels

import "github.com/open-sspm/open-sspm/internal/db/gen"

type LinkedAppView struct {
	AppUser      gen.AppUser
	Entitlements []gen.Entitlement
}

type IdPUserShowViewData struct {
	Layout          LayoutData
	User            gen.IdpUser
	OktaAssignments []OktaAssignmentView
	OktaAppCount    int
	LinkedApps      []LinkedAppView
	LinkedAppsCount int
	HasLinkedApps   bool
}
