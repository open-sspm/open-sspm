package viewmodels

type GitHubUserListItem struct {
	ID          int64
	ExternalID  string
	DisplayName string
	IdentityID  int64
}

type GitHubUsersViewData struct {
	SourceAccountInventoryPageData
	Users    []GitHubUserListItem
	HasUsers bool
}

type GitHubAccountsNeedingAnchorViewData struct {
	SourceAccountsNeedingAnchorPageData
}
