package viewmodels

type GoogleWorkspaceUserListItem struct {
	ID             int64
	ExternalID     string
	Email          string
	DisplayName    string
	IdentityID     int64
	GroupCount     int
	AdminRoleCount int
}

type GoogleWorkspaceUsersViewData struct {
	SourceAccountInventoryPageData
	Users    []GoogleWorkspaceUserListItem
	HasUsers bool
}

type GoogleWorkspaceGroupListItem struct {
	ID           int64
	ExternalID   string
	Email        string
	DisplayName  string
	MemberCount  int
	OwnerCount   int
	ManagerCount int
}

type GoogleWorkspaceGroupsViewData struct {
	PaginatedListPageData
	Groups    []GoogleWorkspaceGroupListItem
	Query     string
	HasGroups bool
}

type UnmatchedGoogleWorkspaceViewData struct {
	UnmatchedSourceAccountsPageData
}
