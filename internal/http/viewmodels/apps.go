package viewmodels

type AppListItem struct {
	ExternalID     string
	Label          string
	Name           string
	Status         string
	SignOnMode     string
	IntegratedHref string
	SuggestedKind  string
}

type AppsViewData struct {
	Layout        LayoutData
	Apps          []AppListItem
	Query         string
	ShowingCount  int
	ShowingFrom   int
	ShowingTo     int
	TotalCount    int64
	Page          int
	PerPage       int
	TotalPages    int
	HasApps       bool
	EmptyStateMsg string
}
