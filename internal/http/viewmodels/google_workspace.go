package viewmodels

import "github.com/open-sspm/open-sspm/internal/db/gen"

type GoogleWorkspaceUserListItem struct {
	ID             int64
	ExternalID     string
	Email          string
	DisplayName    string
	IdpUserID      int64
	GroupCount     int
	AdminRoleCount int
}

type GoogleWorkspaceUsersViewData struct {
	Layout         LayoutData
	Users          []GoogleWorkspaceUserListItem
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

type GoogleWorkspaceGroupListItem struct {
	ID           int64
	ExternalID   string
	Email        string
	DisplayName  string
	MemberCount  int
	OwnerCount   int
	ManagerCount int
}

type GoogleWorkspaceGroupsViewData struct {
	Layout         LayoutData
	Groups         []GoogleWorkspaceGroupListItem
	Query          string
	ShowingCount   int
	ShowingFrom    int
	ShowingTo      int
	TotalCount     int64
	Page           int
	PerPage        int
	TotalPages     int
	HasGroups      bool
	EmptyStateMsg  string
	EmptyStateHref string
}

type GoogleWorkspaceOAuthAppListItem struct {
	ID          int64
	ExternalID  string
	DisplayName string
	Status      string
	OwnerCount  int
	GrantCount  int
	LastSeenAt  string
}

type GoogleWorkspaceOAuthAppsViewData struct {
	Layout         LayoutData
	Apps           []GoogleWorkspaceOAuthAppListItem
	Query          string
	ShowingCount   int
	ShowingFrom    int
	ShowingTo      int
	TotalCount     int64
	Page           int
	PerPage        int
	TotalPages     int
	HasApps        bool
	EmptyStateMsg  string
	EmptyStateHref string
}

type UnmatchedGoogleWorkspaceViewData struct {
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
