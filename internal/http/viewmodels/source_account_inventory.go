package viewmodels

import "github.com/open-sspm/open-sspm/internal/http/querystate"

type SourceAccountInventoryPageData struct {
	PaginatedListPageData
	Query       querystate.BasicListQuery
	HasAccounts bool
}
