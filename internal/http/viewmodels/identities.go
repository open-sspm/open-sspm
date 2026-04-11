package viewmodels

import (
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/querystate"
)

type IdentityListItem struct {
	ID                int64
	Initials          string
	NamePrimary       string
	NameSecondary     string
	IdentityType      string
	Managed           bool
	SourceKind        string
	SourceName        string
	IntegrationsCount int64
	PrivilegedRoles   int64
	Status            string
	ActivityState     string
	LastSeen          TimeDisplay
	FirstSeen         TimeDisplay
	RowState          string
}

type IdentitiesViewData struct {
	PaginatedListPageData
	Items             []IdentityListItem
	Sources           []ProgrammaticSourceOption
	SourceNameOptions []ProgrammaticSourceOption
	Query             querystate.IdentitiesQuery
	HasIdentities     bool
}

type IdentityLinkedAccountView struct {
	Account          gen.Account
	EntitlementCount int
	DetailHref       string
}

type IdentityShowViewData struct {
	Layout             LayoutData
	Identity           gen.GetIdentitySummaryByIDRow
	NamePrimary        string
	NameSecondary      string
	CreatedOn          TimeDisplay
	UpdatedOn          TimeDisplay
	TotalEntitlements  int
	LinkedAccounts     []IdentityLinkedAccountView
	NonHumanAccessHref string
	HasLinkedAccounts  bool
}
