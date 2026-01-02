package viewmodels

import "github.com/open-sspm/open-sspm/internal/db/gen"

type DatadogUserListItem struct {
	UserName     string
	Status       string
	RolesDisplay string
}

type DatadogUsersViewData struct {
	Layout         LayoutData
	Users          []DatadogUserListItem
	Query          string
	State          string
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

type UnmatchedDatadogViewData struct {
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
