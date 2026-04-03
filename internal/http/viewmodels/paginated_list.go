package viewmodels

import "github.com/open-sspm/open-sspm/internal/db/gen"

type PaginatedListPageData struct {
	Layout         LayoutData
	ShowingCount   int
	ShowingFrom    int
	ShowingTo      int
	TotalCount     int64
	Page           int
	PerPage        int
	TotalPages     int
	EmptyStateMsg  string
	EmptyStateHref string
}

type UnmatchedSourceAccountsPageData struct {
	PaginatedListPageData
	Users    []gen.Account
	Query    string
	HasUsers bool
}
