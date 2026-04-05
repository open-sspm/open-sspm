package viewmodels

import "github.com/open-sspm/open-sspm/internal/http/querystate"

type DatadogUserListItem struct {
	UserName     string
	Status       string
	RolesDisplay string
}

type DatadogUsersViewData struct {
	PaginatedListPageData
	Users    []DatadogUserListItem
	Query    querystate.BasicListQuery
	HasUsers bool
}

type UnmatchedDatadogViewData struct {
	UnmatchedSourceAccountsPageData
}
