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
	FollowUpDueDate        TimeDisplay
	IsFollowUpOverdue      bool
	ReplacementDisplayName string
	TicketRef              string
	Actors30d              int64
	LastSeen               TimeDisplay
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
	FollowUpDueDate        TimeDisplay
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
	LastObservedAt  TimeDisplay
}

type DiscoveryActorItem struct {
	ActorLabel      string
	ActorEmail      string
	ActorExternalID string
	EventCount      int64
	LastObservedAt  TimeDisplay
}

type DiscoveryEventItem struct {
	SignalKind    string
	ObservedAt    TimeDisplay
	Actor         string
	SourceApp     string
	ScopesSummary string
}

type DiscoveryRiskSignalItem struct {
	Severity string
	Title    string
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
	FollowUpDueDate              TimeDisplay
	IsFollowUpOverdue            bool
	TicketRef                    string
	Notes                        string
	ReplacementDisplayName       string
	ReplacementPrimaryDomain     string
	FirstSeen                    TimeDisplay
	LastSeen                     TimeDisplay
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
	ChangedAt                TimeDisplay
	ChangedBy                string
	Owner                    string
	ReviewOwner              string
	ReviewDisposition        string
	FollowUpDueDate          TimeDisplay
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
	RiskSignals                []DiscoveryRiskSignalItem
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
	OpenGovernanceDialog       bool
	HasSources                 bool
	HasTopActors               bool
	HasEvents                  bool
	HasRiskSignals             bool
	HasDecisionHistory         bool
}
