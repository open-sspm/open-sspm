package viewmodels

import (
	"strings"

	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/querystate"
	identitydomain "github.com/open-sspm/open-sspm/internal/identitydetail"
)

type IdentityListItem struct {
	ID                int64
	Initials          string
	NamePrimary       string
	NameSecondary     string
	IdentityType      string
	AnchorState       string
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
// ignores segment-like filters (anchor/status/activity/privileged) so the
// stat strip and segment chips can show the shape of the population a user is
// actually looking at, independent of which segment they have clicked.
type IdentitiesSummary struct {
	Total           int64
	ActionRequired  int64
	Review          int64
	Privileged      int64
	StalePrivileged int64
	MissingAnchor   int64
	Suspended       int64
	Stale           int64
}

type IdentitiesViewData struct {
	PaginatedListPageData
	Items             []IdentityListItem
	Sources           []ProgrammaticSourceOption
	SourceNameOptions []ProgrammaticSourceOption
	Query             querystate.IdentitiesQuery
	Summary           IdentitiesSummary
	HasIdentities     bool
}

type IdentityLinkedAccountView struct {
	Account          gen.Account
	EntitlementCount int
	DetailHref       string
	StatusActive     bool
	LastSignIn       TimeDisplay
	LastSignInUnix   int64
	Dormant          bool
}

type IdentityEntitlementView struct {
	AccountLabel        string
	AccountHref         string
	AccountSourceKind   string
	AccountSourceName   string
	Kind                string
	ResourceKind        string
	ResourceID          string
	ResourceLabel       string
	ResourceHref        string
	Permission          string
	IsAdmin             bool
	AccountLastSignIn   TimeDisplay
	AccountActivityUnix int64
	Dormant             bool
}

func (v IdentityEntitlementView) EntitlementGroupFields() identitydomain.EntitlementGroupFields {
	return identitydomain.EntitlementGroupFields{
		AccountSourceKind: v.AccountSourceKind,
		AccountSourceName: v.AccountSourceName,
		Kind:              v.Kind,
		ResourceKind:      v.ResourceKind,
		ResourceID:        v.ResourceID,
		ResourceLabel:     v.ResourceLabel,
		ResourceHref:      v.ResourceHref,
		Permission:        v.Permission,
		IsAdmin:           v.IsAdmin,
		LastActivityUnix:  v.AccountActivityUnix,
		Dormant:           v.Dormant,
	}
}

// IdentityEntitlementGroupMode selects how entitlements are bucketed in the
// identity detail page. It is bound to the ?group= query param.
type IdentityEntitlementGroupMode = identitydomain.EntitlementGroupMode

const (
	IdentityEntitlementGroupSource   = identitydomain.EntitlementGroupSource
	IdentityEntitlementGroupKind     = identitydomain.EntitlementGroupKind
	IdentityEntitlementGroupResource = identitydomain.EntitlementGroupResource
	IdentityEntitlementGroupNone     = identitydomain.EntitlementGroupNone
)

// IdentityLinkedAccountSortMode selects the sort order for the linked-account
// table on the identity detail page.
type IdentityLinkedAccountSortMode = identitydomain.LinkedAccountSortMode

const (
	IdentityLinkedAccountSortGrants   = identitydomain.LinkedAccountSortGrants
	IdentityLinkedAccountSortSource   = identitydomain.LinkedAccountSortSource
	IdentityLinkedAccountSortActivity = identitydomain.LinkedAccountSortActivity
)

// IdentityEntitlementGroup is one bucket inside the entitlements section.
type IdentityEntitlementGroup = identitydomain.EntitlementGroup[IdentityEntitlementView]

// IdentityReviewSummary is the compact verdict shown in the identity header.
// It should answer whether the reviewer needs to act before they inspect rows.
type IdentityReviewSummary struct {
	Label  string
	Detail string
	Tone   string
}

// IdentitySummaryTile is one card in the four-up summary row.
type IdentitySummaryTile = identitydomain.SummaryTile

// IdentityProfileFact is one compact fact in the profile header.
type IdentityProfileFact struct {
	Label string
	Value string
}

// IdentityFilterChip is a visible active filter with a link that removes it.
type IdentityFilterChip struct {
	Label     string
	ClearHref string
}

type IdentityShowViewData struct {
	Layout         LayoutData
	Breadcrumb     IdentityShowBreadcrumb
	Profile        IdentityShowProfile
	Summary        IdentityShowSummary
	LinkedAccounts IdentityShowLinkedAccountsPanel
	Entitlements   IdentityShowEntitlementsPanel
}

type IdentityShowBreadcrumb struct {
	RootLabel string
	RootHref  string
	KindLabel string
	KindHref  string
	Current   string
}

type IdentityShowProfile struct {
	Identity               gen.GetIdentitySummaryByIDRow
	NamePrimary            string
	NameSecondary          string
	Initials               string
	AvatarClass            string
	StatusLabel            string
	StatusTone             string
	IdentityTypeLabel      string
	Tags                   []string
	AdminScopeSummary      string
	ReviewSummary          IdentityReviewSummary
	Facts                  []IdentityProfileFact
	CreatedOn              TimeDisplay
	UpdatedOn              TimeDisplay
	NonHumanIdentitiesHref string
}

type IdentityShowSummary struct {
	Tiles []IdentitySummaryTile
}

type IdentityShowLinkedAccountsPanel struct {
	Total          int
	Active         int
	Dormant        int
	Visible        int
	Items          []IdentityLinkedAccountView
	Query          string
	QueryClearHref string
	SortMode       IdentityLinkedAccountSortMode
	HasItems       bool
	HasFilter      bool
}

type IdentityShowEntitlementsPanel struct {
	Total            int
	Visible          int
	Items            []IdentityEntitlementView
	Query            string
	QueryClearHref   string
	AdminOnly        bool
	DormantOnly      bool
	SourceFilter     string
	SourceOptions    []IdentitySourceFilterOption
	FilterCount      int
	FilterChips      []IdentityFilterChip
	ClearFiltersHref string
	FormHiddenInputs []IdentityFormHiddenInput
	Groups           []IdentityEntitlementGroup
	HasItems         bool
	HasFilter        bool
}

// IdentitySourceFilterOption represents one entry in the source-kind select
// inside the entitlements filter dropdown.
type IdentitySourceFilterOption struct {
	Value    string
	Label    string
	Selected bool
}

// IdentityFormHiddenInput is a single hidden input that must be carried across
// GET form submissions to preserve unrelated query state.
type IdentityFormHiddenInput struct {
	Name  string
	Value string
}

func ParseLinkedAccountSortMode(raw string) IdentityLinkedAccountSortMode {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case string(IdentityLinkedAccountSortSource):
		return IdentityLinkedAccountSortSource
	case string(IdentityLinkedAccountSortActivity):
		return IdentityLinkedAccountSortActivity
	default:
		return IdentityLinkedAccountSortGrants
	}
}

// IdentityShowQuery carries every query-string knob the identity detail page
// understands. Builders take it by value so callers can clone-and-tweak when
// emitting links for toggles or "clear" actions.
type IdentityShowQuery = identitydomain.ShowQuery

func BuildIdentityShowHref(basePath string, q IdentityShowQuery) string {
	return identitydomain.BuildShowHref(basePath, q)
}
