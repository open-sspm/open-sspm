package viewmodels

type SourceAccountInventoryPageData struct {
	PaginatedListPageData
	Query       string
	HasAccounts bool
}
