package viewmodels

import "github.com/open-sspm/open-sspm/internal/db/gen"

type AWSUserListItem struct {
	ID              int64
	ExternalID      string
	Email           string
	DisplayName     string
	IdentityID      int64
	AccountCount    int
	AssignmentCount int
}

type AWSUsersViewData struct {
	SourceAccountInventoryPageData
	Users    []AWSUserListItem
	HasUsers bool
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
