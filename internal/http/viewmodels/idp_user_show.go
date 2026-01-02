package viewmodels

import "github.com/open-sspm/open-sspm/internal/db/gen"

type LinkedEntitlementView struct {
	Kind          string
	ResourceKind  string
	ResourceID    string
	ResourceLabel string
	ResourceHref  string
	Permission    string
}

type LinkedAppView struct {
	AppUser      gen.AppUser
	Entitlements []LinkedEntitlementView
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
