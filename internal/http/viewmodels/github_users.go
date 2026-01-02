package viewmodels

import "github.com/open-sspm/open-sspm/internal/db/gen"

type GitHubUsersViewData struct {
	Layout         LayoutData
	Users          []gen.ListAppUsersWithLinkPageBySourceAndQueryRow
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

type UnmatchedGitHubViewData struct {
	Layout         LayoutData
	Users          []gen.AppUser
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
