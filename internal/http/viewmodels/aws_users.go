package viewmodels

import "github.com/open-sspm/open-sspm/internal/db/gen"

type AWSUserListItem struct {
	ID              int64
	ExternalID      string
	Email           string
	DisplayName     string
	IdpUserID       int64
	AccountCount    int
	AssignmentCount int
}

type AWSUsersViewData struct {
	Layout         LayoutData
	Users          []AWSUserListItem
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

type UnmatchedAWSViewData struct {
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
