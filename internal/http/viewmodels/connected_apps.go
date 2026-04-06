package viewmodels

import "github.com/open-sspm/open-sspm/internal/http/querystate"

type ConnectedAppsGovernanceCount struct {
	GovernanceState string
	Label           string
	Count           int64
	Href            string
	IsActive        bool
}

type ConnectedAppListItem struct {
	ID                       int64
	DisplayName              string
	ExternalID               string
	Status                   string
	GovernanceState          string
	GovernanceOwner          string
	GovernanceOwnerEmail     string
	LikelyOwnerCount         int
	GrantCount               int
	ActorCount               int64
	DiscoveryEventCount30d   int64
	EvidenceFreshness        string
	EvidenceConfidence       string
	EvidenceConfidenceReason string
	LastSeenAt               string
	TicketRef                string
}

type ConnectedAppsViewData struct {
	PaginatedListPageData
	Query            querystate.ConnectedAppsQuery
	GovernanceCounts []ConnectedAppsGovernanceCount
	Items            []ConnectedAppListItem
	HasItems         bool
}

type ConnectedAppsAlert struct {
	Title       string
	Message     string
	Destructive bool
}

type ConnectedAppSummaryView struct {
	ID                       int64
	DisplayName              string
	ExternalID               string
	SourceKind               string
	SourceName               string
	Status                   string
	GovernanceState          string
	GovernanceOwner          string
	GovernanceOwnerEmail     string
	GovernanceOwnerKind      string
	TicketRef                string
	Notes                    string
	LikelyOwnerCount         int
	GrantCount               int
	ActorCount               int64
	DiscoverySourceCount     int64
	DiscoveryEventCount30d   int64
	EvidenceFreshness        string
	EvidenceConfidence       string
	EvidenceConfidenceReason string
	LastSeenAt               string
	ExportHref               string
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
	Layout               LayoutData
	App                  ConnectedAppSummaryView
	LikelyOwners         []AppAssetOwnerItem
	Grants               []ConnectedAppGrantItem
	DiscoverySources     []ConnectedAppDiscoverySourceItem
	Events               []ConnectedAppDiscoveryEventItem
	Alert                *ConnectedAppsAlert
	OwnerEmailInput      string
	GovernanceStateInput string
	TicketRefInput       string
	NotesInput           string
	HasLikelyOwners      bool
	HasGrants            bool
	HasEvidence          bool
	HasEvents            bool
}
