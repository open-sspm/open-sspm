package viewmodels

import "github.com/open-sspm/open-sspm/internal/http/querystate"

type ConnectedAppsReviewCount struct {
	ReviewState string
	Label       string
	Count       int64
	Href        string
	IsActive    bool
}

type ConnectedAppListItem struct {
	ID                     int64
	DisplayName            string
	ExternalID             string
	Status                 string
	ReviewState            string
	ReviewOwner            string
	ReviewOwnerEmail       string
	LikelyOwnerCount       int
	GrantCount             int
	ActorCount             int64
	DiscoveryEventCount30d int64
	Freshness              string
	Confidence             string
	ConfidenceReason       string
	LastSeenAt             string
	TicketRef              string
}

type ConnectedAppsViewData struct {
	PaginatedListPageData
	Query        querystate.ConnectedAppsQuery
	ReviewCounts []ConnectedAppsReviewCount
	Items        []ConnectedAppListItem
	HasItems     bool
}

type ConnectedAppsAlert struct {
	Title       string
	Message     string
	Destructive bool
}

type ConnectedAppSummaryView struct {
	ID                     int64
	DisplayName            string
	ExternalID             string
	SourceKind             string
	SourceName             string
	Status                 string
	ReviewState            string
	ReviewOwner            string
	ReviewOwnerEmail       string
	ReviewOwnerKind        string
	TicketRef              string
	Notes                  string
	LikelyOwnerCount       int
	GrantCount             int
	ActorCount             int64
	DiscoverySourceCount   int64
	DiscoveryEventCount30d int64
	Freshness              string
	Confidence             string
	ConfidenceReason       string
	LastSeenAt             string
	ExportHref             string
}

type ConnectedAppGrantItem struct {
	CredentialID   int64
	DisplayName    string
	UserLabel      string
	UserEmail      string
	UserExternalID string
	UserHref       string
	Status         string
	RiskLevel      string
	ScopeSummary   string
	ScopeCount     int
	LastUsedAt     string
	CanRevoke      bool
}

type ConnectedAppDiscoverySourceItem struct {
	DiscoveryDisplayName string
	CanonicalKey         string
	Domain               string
	VendorName           string
	ManagedState         string
	RiskLevel            string
	SourceName           string
	LastObservedAt       string
}

type ConnectedAppDiscoveryEventItem struct {
	SignalKind    string
	ObservedAt    string
	Actor         string
	ScopesSummary string
}

type ConnectedAppShowViewData struct {
	Layout           LayoutData
	App              ConnectedAppSummaryView
	LikelyOwners     []AppAssetOwnerItem
	Grants           []ConnectedAppGrantItem
	DiscoverySources []ConnectedAppDiscoverySourceItem
	Events           []ConnectedAppDiscoveryEventItem
	Alert            *ConnectedAppsAlert
	OwnerEmailInput  string
	ReviewStateInput string
	TicketRefInput   string
	NotesInput       string
	HasLikelyOwners  bool
	HasGrants        bool
	HasEvidence      bool
	HasEvents        bool
}
