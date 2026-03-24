package viewmodels

type SourceAccountInventoryPageData struct {
	Layout         LayoutData
	Query          string
	ShowingCount   int
	ShowingFrom    int
	ShowingTo      int
	TotalCount     int64
	Page           int
	PerPage        int
	TotalPages     int
	HasAccounts    bool
	EmptyStateMsg  string
	EmptyStateHref string
}
