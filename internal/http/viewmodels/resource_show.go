package viewmodels

import "github.com/open-sspm/open-sspm/internal/http/querystate"

type ResourceAccessRow struct {
	IdentityID          int64
	IdentityHref        string
	IdentityEmail       string
	IdentityDisplayName string
	IdentityStatus      string

	AccountExternalID  string
	AccountEmail       string
	AccountDisplayName string

	EntitlementKind       string
	EntitlementPermission string
	LinkReason            string
}

type ResourceShowViewData struct {
	Layout LayoutData

	SourceKind  string
	SourceName  string
	SourceLabel string
	SourceHref  string

	ResourceKind      string
	ResourceKindLabel string
	ExternalID        string
	DisplayName       string

	ExternalConsoleHref string

	EntitlementCount    int64
	AccountCount        int64
	LinkedIdentityCount int64

	Query        querystate.ResourceAccessQuery
	Page         int
	TotalPages   int
	ShowingFrom  int
	ShowingTo    int
	MatchingRows int64

	Rows    []ResourceAccessRow
	HasRows bool
}
