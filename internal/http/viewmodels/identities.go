package viewmodels

import "github.com/open-sspm/open-sspm/internal/db/gen"

type IdentityListItem struct {
	ID                int64
	NamePrimary       string
	NameSecondary     string
	IntegrationsCount int64
	PrivilegedRoles   int64
	LastSeenOn        string
	FirstCreatedOn    string
}

type IdentitiesViewData struct {
	Layout        LayoutData
	Items         []IdentityListItem
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
	Layout                 LayoutData
	Identity               gen.GetIdentitySummaryByIDRow
	LinkedAccounts         []IdentityLinkedAccountView
	ProgrammaticAccessHref string
	HasLinkedAccounts      bool
}
