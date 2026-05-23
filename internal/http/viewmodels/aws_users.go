package viewmodels

type AWSUserListItem struct {
	ID              int64
	ExternalID      string
	Email           string
	DisplayName     string
	IdentityID      int64
	AccountCount    int
	AssignmentCount int
}

type AWSUsersViewData struct {
	SourceAccountInventoryPageData
	Users    []AWSUserListItem
	HasUsers bool
}

type AWSAccountsNeedingAnchorViewData struct {
	SourceAccountsNeedingAnchorPageData
}
