package viewmodels

type EntraUserListItem struct {
	ID                 int64
	ExternalID         string
	Email              string
	DisplayName        string
	IdentityID         int64
	DirectoryRoleCount int
	EnterpriseAppCount int
}

type EntraUsersViewData struct {
	SourceAccountInventoryPageData
	Users    []EntraUserListItem
	HasUsers bool
}

type UnmatchedEntraViewData struct {
	UnmatchedSourceAccountsPageData
}
