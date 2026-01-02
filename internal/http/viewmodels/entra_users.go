package viewmodels

import "github.com/open-sspm/open-sspm/internal/db/gen"

type EntraUserListItem struct {
	ID                 int64
	ExternalID         string
	Email              string
	DisplayName        string
	IdpUserID          int64
	DirectoryRoleCount int
	EnterpriseAppCount int
}

type EntraUsersViewData struct {
	Layout         LayoutData
	Users          []EntraUserListItem
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

type UnmatchedEntraViewData struct {
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
