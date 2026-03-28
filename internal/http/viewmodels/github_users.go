package viewmodels

import "github.com/open-sspm/open-sspm/internal/db/gen"

type GitHubUserListItem struct {
	ID          int64
	ExternalID  string
	DisplayName string
	IdentityID  int64
}

type GitHubUsersViewData struct {
	SourceAccountInventoryPageData
	Users    []GitHubUserListItem
	HasUsers bool
}

type UnmatchedGitHubViewData struct {
	Layout         LayoutData
	Users          []gen.Account
	Query          string
	ShowingCount   int
	ShowingFrom    int
	ShowingTo      int
	TotalCount     int64
	Page           int
	PerPage        int
	TotalPages     int
	HasUsers       bool
	EmptyStateMsg  string
	EmptyStateHref string
}
