package viewmodels

type ResourceAccessRow struct {
	IdpUserID          int64
	IdpUserHref        string
	IdpUserEmail       string
	IdpUserDisplayName string
	IdpUserStatus      string

	AppUserExternalID  string
	AppUserEmail       string
	AppUserDisplayName string

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

	EntitlementCount   int
	AppAccountCount    int
	LinkedIdpUserCount int

	Rows    []ResourceAccessRow
	HasRows bool
}
