package viewmodels

type DiscoverySourceOption struct {
	SourceKind string
	SourceName string
	Label      string
}

type DiscoveryAppListItem struct {
	ID            int64
	DisplayName   string
	Domain        string
	VendorName    string
	ManagedState  string
	ManagedReason string
	RiskScore     int32
	RiskLevel     string
	Owner         string
	Actors30d     int64
	LastSeenAt    string
}

type DiscoveryAppsViewData struct {
	Layout             LayoutData
	SourceOptions      []DiscoverySourceOption
	SourceNameOptions  []DiscoverySourceOption
	SelectedSourceKind string
	SelectedSourceName string
	Query              string
	ManagedState       string
	RiskLevel          string
	Items              []DiscoveryAppListItem
	ShowingCount       int
	ShowingFrom        int
	ShowingTo          int
	TotalCount         int64
	Page               int
	PerPage            int
	TotalPages         int
	HasItems           bool
	EmptyStateMsg      string
}

type DiscoveryHotspotItem struct {
	ID           int64
	DisplayName  string
	Domain       string
	ManagedState string
	RiskScore    int32
	RiskLevel    string
	Owner        string
	Actors30d    int64
}

type DiscoveryHotspotsViewData struct {
	Layout             LayoutData
	SourceOptions      []DiscoverySourceOption
	SourceNameOptions  []DiscoverySourceOption
	SelectedSourceKind string
	SelectedSourceName string
	Items              []DiscoveryHotspotItem
	HasItems           bool
	EmptyStateMsg      string
}

type DiscoveryOwnerOption struct {
	ID    int64
	Label string
}

type DiscoveryBindingOption struct {
	ConnectorKind       string
	ConnectorSourceName string
	Label               string
}

type DiscoveryBindingItem struct {
	ConnectorKind       string
	ConnectorSourceName string
	BindingSource       string
	ConfidenceLabel     string
	IsPrimary           bool
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
	FirstSeenAt                  string
	LastSeenAt                   string
}

type DiscoveryGovernanceView struct {
	OwnerIdentityID     int64
	OwnerDisplayName    string
	BusinessCriticality string
	DataClassification  string
	Notes               string
	UpdatedAt           string
	UpdatedByUserID     int64
}

type DiscoveryAppShowViewData struct {
	Layout             LayoutData
	App                DiscoveryAppSummaryView
	Governance         DiscoveryGovernanceView
	OwnerOptions       []DiscoveryOwnerOption
	BindingOptions     []DiscoveryBindingOption
	Bindings           []DiscoveryBindingItem
	Sources            []DiscoverySourceEvidenceItem
	TopActors          []DiscoveryActorItem
	Events             []DiscoveryEventItem
	SelectedBindKind   string
	SelectedBindSource string
	HasBindings        bool
	HasSources         bool
	HasTopActors       bool
	HasEvents          bool
}
