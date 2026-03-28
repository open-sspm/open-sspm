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

type LinkedAccountView struct {
	Account      gen.Account
	Entitlements []LinkedEntitlementView
}

type OktaAccountShowViewData struct {
	Layout          LayoutData
	User            gen.Account
	OktaAssignments []OktaAssignmentView
	OktaAppCount    int
	LinkedAccounts  []LinkedAccountView
	LinkedAccountsCount int
	HasLinkedAccounts   bool
}
