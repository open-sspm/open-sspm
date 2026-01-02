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

type OktaAppAssignedUserView struct {
	IdpUserID       int64
	UserHref        string
	UserDisplayName string
	UserEmail       string
	UserExternalID  string
	UserStatus      string
	AssignedVia     string
	Groups          []string
	Permissions     []PermissionBadge
}

type OktaAppShowViewData struct {
	Layout        LayoutData
	App           OktaAppSummaryView
	Users         []OktaAppAssignedUserView
	Query         string
	State         string
	ShowingCount  int
	ShowingFrom   int
	ShowingTo     int
	TotalCount    int64
	Page          int
	PerPage       int
	TotalPages    int
	HasUsers      bool
	EmptyStateMsg string
}

type OktaAssignmentView struct {
	AppLabel    string
	AppName     string
	AppHref     string
	AssignedVia string
	Groups      []string
	Permissions []PermissionBadge
}
