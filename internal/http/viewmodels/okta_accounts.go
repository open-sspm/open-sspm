package viewmodels

import (
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/querystate"
)

type OktaAccountsViewData struct {
	PaginatedListPageData
	Users    []gen.Account
	Query    querystate.BasicListQuery
	HasUsers bool
}
