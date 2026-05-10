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

// IdentitiesSummary holds pre-filter bucketed counts for the identity
// inventory. It is scoped to the user's source, search, and type filters but
// ignores segment-like filters (managed/status/activity/privileged) so the
// stat strip and segment chips can show the shape of the population a user is
// actually looking at, independent of which segment they have clicked.
type IdentitiesSummary struct {
	Total               int64
	ActionRequired      int64
	Review              int64
	Privileged          int64
	PrivilegedUnmanaged int64
	StalePrivileged     int64
	Unmanaged           int64
	Suspended           int64
	Stale               int64
}

type IdentitiesViewData struct {
	PaginatedListPageData
	Items             []IdentityListItem
	Sources           []ProgrammaticSourceOption
	SourceNameOptions []ProgrammaticSourceOption
	Query             querystate.IdentitiesQuery
	Summary           IdentitiesSummary
	HasIdentities     bool
	OverviewMap       OverviewMapGraph
}

type IdentityLinkedAccountView struct {
	Account          gen.Account
	EntitlementCount int
	DetailHref       string
}

type IdentityEntitlementView struct {
	AccountLabel      string
	AccountHref       string
	AccountSourceKind string
	AccountSourceName string
	Kind              string
	ResourceKind      string
	ResourceID        string
	ResourceLabel     string
	ResourceHref      string
	Permission        string
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
	Entitlements       []IdentityEntitlementView
	NonHumanAccessHref string
	HasLinkedAccounts  bool
	HasEntitlements    bool
	OverviewMap        OverviewMapGraph
}
