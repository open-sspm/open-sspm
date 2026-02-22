package viewmodels

import "github.com/open-sspm/open-sspm/internal/db/gen"

type IdentityListItem struct {
	ID                int64
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
	LastSeenOn        string
	FirstSeenOn       string
	LinkQuality       string
	LinkReason        string
	MinLinkConfidence float32
	RowState          string
}

type IdentitiesViewData struct {
	Layout             LayoutData
	Items              []IdentityListItem
	Sources            []ProgrammaticSourceOption
	SourceNameOptions  []ProgrammaticSourceOption
	SelectedSourceKind string
	SelectedSourceName string
	Query              string
	IdentityType       string
	ManagedState       string
	PrivilegedOnly     bool
	Status             string
	ActivityState      string
	LinkQuality        string
	SortBy             string
	SortDir            string
	ShowFirstSeen      bool
	ShowLinkQuality    bool
	ShowLinkReason     bool
	ShowingCount       int
	ShowingFrom        int
	ShowingTo          int
	TotalCount         int64
	Page               int
	PerPage            int
	TotalPages         int
	HasIdentities      bool
	EmptyStateMsg      string
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
