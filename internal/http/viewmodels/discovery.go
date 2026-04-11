package viewmodels

import "github.com/open-sspm/open-sspm/internal/http/querystate"

type DiscoverySourceOption struct {
	SourceKind string
	SourceName string
	Label      string
}

type DiscoveryAppListItem struct {
	ID                     int64
	DisplayName            string
	Domain                 string
	VendorName             string
	ManagedState           string
	ManagedReason          string
	RiskScore              int32
	RiskLevel              string
	Owner                  string
	ReviewOwner            string
	ReviewDisposition      string
	FollowUpDueDate        string
	IsFollowUpOverdue      bool
	ReplacementDisplayName string
	TicketRef              string
	Actors30d              int64
	LastSeenAt             string
}

type DiscoveryAppsViewData struct {
	PaginatedListPageData
	SourceOptions     []DiscoverySourceOption
	SourceNameOptions []DiscoverySourceOption
	Query             querystate.DiscoveryAppsQuery
	Items             []DiscoveryAppListItem
	HasItems          bool
}

type DiscoveryHotspotItem struct {
	ID                     int64
	DisplayName            string
	Domain                 string
	ManagedState           string
	RiskScore              int32
	RiskLevel              string
	Owner                  string
	ReviewOwner            string
	ReviewDisposition      string
	FollowUpDueDate        string
	IsFollowUpOverdue      bool
	ReplacementDisplayName string
	TicketRef              string
	Actors30d              int64
}

type DiscoveryHotspotsViewData struct {
	Layout            LayoutData
	SourceOptions     []DiscoverySourceOption
	SourceNameOptions []DiscoverySourceOption
	Query             querystate.DiscoveryHotspotsQuery
	Items             []DiscoveryHotspotItem
	HasItems          bool
	EmptyStateMsg     string
}

type DiscoverySourceEvidenceItem struct {
	SourceKind      string
	SourceName      string
	SourceAppID     string
	SourceAppName   string
	SourceAppDomain string
	LastObservedAt  string
}

type DiscoveryActorItem struct {
	ActorLabel      string
	ActorEmail      string
	ActorExternalID string
	EventCount      int64
	LastObservedAt  string
}

type DiscoveryEventItem struct {
	SignalKind    string
	ObservedAt    string
	Actor         string
	SourceApp     string
	ScopesSummary string
}

type DiscoveryAppSummaryView struct {
	ID                           int64
	DisplayName                  string
	CanonicalKey                 string
	PrimaryDomain                string
	VendorName                   string
	ManagedState                 string
	ManagedReason                string
	RiskScore                    int32
	RiskLevel                    string
	SuggestedBusinessCriticality string
	SuggestedDataClassification  string
	Owner                        string
	ReviewOwner                  string
	ReviewDisposition            string
	FollowUpDueDate              string
	IsFollowUpOverdue            bool
	TicketRef                    string
	Notes                        string
	ReplacementDisplayName       string
	ReplacementPrimaryDomain     string
	FirstSeenAt                  string
	LastSeenAt                   string
}

type DiscoveryReplacementCandidateItem struct {
	ID           int64
	DisplayName  string
	Domain       string
	VendorName   string
	ManagedState string
	RiskLevel    string
	IsSelected   bool
}

type DiscoveryReplacementCandidatesViewData struct {
	SelectedCandidate *DiscoveryReplacementCandidateItem
	Candidates        []DiscoveryReplacementCandidateItem
	HasCandidates     bool
	EmptyStateMsg     string
}

type DiscoveryReviewDecisionItem struct {
	ChangedAt                string
	ChangedBy                string
	Owner                    string
	ReviewOwner              string
	ReviewDisposition        string
	FollowUpDueDate          string
	IsFollowUpOverdue        bool
	TicketRef                string
	Notes                    string
	ReplacementDisplayName   string
	ReplacementPrimaryDomain string
}

type DiscoveryAppShowViewData struct {
	Layout                     LayoutData
	App                        DiscoveryAppSummaryView
	Sources                    []DiscoverySourceEvidenceItem
	TopActors                  []DiscoveryActorItem
	Events                     []DiscoveryEventItem
	DecisionHistory            []DiscoveryReviewDecisionItem
	Alert                      *AlertViewData
	AccountableOwnerEmailInput string
	ReviewOwnerEmailInput      string
	ReviewDispositionInput     string
	FollowUpDueDateInput       string
	TicketRefInput             string
	NotesInput                 string
	ReplacementQueryInput      string
	ReplacementPicker          DiscoveryReplacementCandidatesViewData
	HasSources                 bool
	HasTopActors               bool
	HasEvents                  bool
	HasDecisionHistory         bool
}
