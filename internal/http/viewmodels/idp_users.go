package viewmodels

import "github.com/open-sspm/open-sspm/internal/db/gen"

type IdPUsersViewData struct {
	Layout        LayoutData
	Users         []gen.Account
	Query         string
	State         string
	ShowingCount  int
	ShowingFrom   int
	ShowingTo     int
	TotalCount    int64
	Page          int
	PerPage       int
	TotalPages    int
	HasUsers      bool
	EmptyStateMsg string
}
