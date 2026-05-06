package viewmodels

import "github.com/open-sspm/open-sspm/internal/http/querystate"

type OktaAppSummaryView struct {
	ExternalID string
	Label      string
	Name       string
	Status     string
	SignOnMode string
}

type ProfileAttributeBadge struct {
	Text string
}

type OktaAppAssignedAccountView struct {
	OktaAccountID         int64
	AccountHref           string
	AccountDisplayName    string
	AccountEmail          string
	OktaAccountExternalID string
	OktaAccountStatus     string
	AssignedVia           string
	Groups                []string
	Attributes            []ProfileAttributeBadge
}

// OktaAppAssignmentSummary describes the shape of the current result set so
// the UI can replace chrome (badges, repetitive columns) with a single
// informative line. Counts describe the current page unless SinglePage is
// true, in which case they describe every assigned account.
type OktaAppAssignmentSummary struct {
	SinglePage         bool
	ActiveCount        int
	InactiveCount      int
	UniformAssignedVia bool
	AssignedViaLabel   string
	AnyGroups          bool
	AnyAttributes      bool
}

type OktaAppShowViewData struct {
	PaginatedListPageData
	App         OktaAppSummaryView
	Accounts    []OktaAppAssignedAccountView
	Query       querystate.BasicListQuery
	HasAccounts bool
	Summary     OktaAppAssignmentSummary
}

type OktaAssignmentView struct {
	AppLabel    string
	AppName     string
	AppHref     string
	AssignedVia string
	Groups      []string
	Attributes  []ProfileAttributeBadge
}
