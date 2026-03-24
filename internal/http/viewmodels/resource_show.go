package viewmodels

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

	EntitlementCount    int
	AccountCount        int
	LinkedIdentityCount int

	Rows    []ResourceAccessRow
	HasRows bool
}
