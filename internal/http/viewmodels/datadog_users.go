package viewmodels

type DatadogUserListItem struct {
	UserName     string
	Status       string
	RolesDisplay string
}

type DatadogUsersViewData struct {
	PaginatedListPageData
	Users    []DatadogUserListItem
	Query    string
	State    string
	HasUsers bool
}

type UnmatchedDatadogViewData struct {
	UnmatchedSourceAccountsPageData
}
