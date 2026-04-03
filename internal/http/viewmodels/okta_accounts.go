package viewmodels

import "github.com/open-sspm/open-sspm/internal/db/gen"

type OktaAccountsViewData struct {
	PaginatedListPageData
	Users    []gen.Account
	Query    string
	State    string
	HasUsers bool
}
