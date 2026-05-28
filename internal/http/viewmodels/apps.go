package viewmodels

import "github.com/open-sspm/open-sspm/internal/http/querystate"

type AppListItem struct {
	SourceName     string
	ExternalID     string
	Label          string
	Name           string
	Status         string
	SignOnMode     string
	IntegratedHref string
	SuggestedKind  string
}

type AppsViewData struct {
	PaginatedListPageData
	Apps          []AppListItem
	Query         querystate.AppsQuery
	StatusOptions []string
	HasApps       bool
}
