package viewmodels

type IdentityResolutionViewData struct {
	PaginatedListPageData
	Items        []IdentityResolutionCandidateItem
	HasItems     bool
	Status       string
	StatusLabel  string
	Group        string
	StatusTabs   []IdentityResolutionFilterOption
	GroupFilters []IdentityResolutionFilterOption
}

type IdentityResolutionFilterOption struct {
	Label    string
	Value    string
	Href     string
	Selected bool
}

type IdentityResolutionCandidateItem struct {
	ID                          int64
	AccountID                   int64
	AccountSourceKind           string
	AccountSourceName           string
	AccountExternalID           string
	AccountEmail                string
	AccountDisplayName          string
	AccountKind                 string
	EntityCategory              string
	CandidateIdentityID         int64
	CandidateDisplayName        string
	CandidatePrimaryEmail       string
	CandidateKind               string
	CandidateResolutionState    string
	CandidateIdentityKind       string
	CandidateHref               string
	ProvisionalIdentityID       int64
	ProvisionalDisplayName      string
	ProvisionalPrimaryEmail     string
	ProvisionalHref             string
	CurrentIdentityID           int64
	CurrentIdentityDisplayName  string
	CurrentIdentityPrimaryEmail string
	CurrentIdentityHref         string
	CurrentLinkState            string
	CurrentLinkReason           string
	Status                      string
	ConfidenceBand              string
	Score                       int32
	MatchReason                 string
	AmbiguityKey                string
	ResolverVersion             string
	ResolverFingerprint         string
	CreatedAt                   TimeDisplay
	Evidence                    []IdentityResolutionEvidenceItem
	HasEvidence                 bool
	AcceptHref                  string
	AcceptMergeHref             string
	CanMergeProvisional         bool
	RejectHref                  string
	MarkServiceHref             string
	MarkServiceCustodianHref    string
	MarkSharedHref              string
	RelationshipCount           int64
}

type IdentityResolutionEvidenceItem struct {
	ID            int64
	EvidenceType  string
	EvidenceKey   string
	AccountValue  string
	IdentityValue string
	SourceKind    string
	SourceName    string
	Strength      int32
	IsPositive    bool
	ObservedAt    TimeDisplay
	Metadata      string
}
