package viewmodels

import "github.com/open-sspm/open-sspm/internal/db/gen"

type IdentitiesViewData struct {
	Layout        LayoutData
	Identities    []gen.ListIdentitiesPageByQueryRow
	Query         string
	ShowingCount  int
	ShowingFrom   int
	ShowingTo     int
	TotalCount    int64
	Page          int
	PerPage       int
	TotalPages    int
	HasIdentities bool
	EmptyStateMsg string
}

type IdentityLinkedAccountView struct {
	Account          gen.Account
	EntitlementCount int
	DetailHref       string
}

type IdentityShowViewData struct {
	Layout            LayoutData
	Identity          gen.GetIdentitySummaryByIDRow
	LinkedAccounts    []IdentityLinkedAccountView
	HasLinkedAccounts bool
}
