package viewmodels

type OktaAppSummaryView struct {
	ExternalID string
	Label      string
	Name       string
	Status     string
	SignOnMode string
}

type PermissionBadge struct {
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
	Permissions           []PermissionBadge
}

type OktaAppShowViewData struct {
	PaginatedListPageData
	App         OktaAppSummaryView
	Accounts    []OktaAppAssignedAccountView
	Query       string
	State       string
	HasAccounts bool
}

type OktaAssignmentView struct {
	AppLabel    string
	AppName     string
	AppHref     string
	AssignedVia string
	Groups      []string
	Permissions []PermissionBadge
}
