package viewmodels

import "github.com/open-sspm/open-sspm/internal/http/querystate"

type NonHumanIdentitiesListItem struct {
	PrincipalRef           string
	IdentityID             int64
	AppAssetID             int64
	PrincipalType          string
	SourceKind             string
	SourceName             string
	DisplayName            string
	SecondaryName          string
	LinkedAssetsCount      int64
	LinkedCredentialsCount int64
	LastSeen               TimeDisplay
	ActivityState          string
	FreshnessState         string
	GovernanceState        string
	AccountableOwner       string
	AccountableOwnerHref   string
	OwnerPresence          string
	RiskLevel              string
}

type NonHumanIdentitiesViewData struct {
	PaginatedListPageData
	Items    []NonHumanIdentitiesListItem
	Sources  []ProgrammaticSourceOption
	Query    querystate.NonHumanIdentitiesQuery
	HasItems bool
}

type NonHumanIdentitiesSummaryView struct {
	PrincipalRef                 string
	IdentityID                   int64
	IdentityHref                 string
	AppAssetID                   int64
	AppAssetHref                 string
	PrincipalType                string
	DisplayName                  string
	SecondaryName                string
	SourceKind                   string
	SourceName                   string
	LinkedAssetsCount            int64
	LinkedCredentialsCount       int64
	LastSeen                     TimeDisplay
	ActivityState                string
	FreshnessState               string
	GovernanceState              string
	AccountableOwner             string
	AccountableOwnerHref         string
	BestAvailableAttribution     string
	BestAvailableAttributionHref string
	OwnerPresence                string
	RiskLevel                    string
	HasCriticalCredential        bool
	HasHighRiskCredential        bool
	HasExpiredCredential         bool
	HasExpiringCredential        bool
	HasUnusedCredential          bool
	HasStaleEvidence             bool
}

type NonHumanIdentitiesRelatedAssetItem struct {
	ID                  int64
	Href                string
	SourceKind          string
	SourceName          string
	AssetKind           string
	DisplayName         string
	ExternalID          string
	Status              string
	GovernanceState     string
	GovernanceOwner     string
	GovernanceOwnerHref string
	LinkedCredentials   int64
	EvidenceFreshness   string
	EvidenceConfidence  string
	EvidenceSeen        TimeDisplay
}

type NonHumanIdentitiesRelatedCredentialItem struct {
	ID              int64
	Href            string
	SourceKind      string
	SourceName      string
	CredentialKind  string
	DisplayName     string
	ExternalID      string
	Status          string
	RiskLevel       string
	ExpiresAt       TimeDisplay
	LastUsedAt      TimeDisplay
	CreatedBy       string
	CreatedByHref   string
	ApprovedBy      string
	ApprovedByHref  string
	AppAssetID      int64
	AppAssetDisplay string
	AppAssetHref    string
}

type NonHumanIdentitiesRiskSignal struct {
	Severity string
	Title    string
	Evidence string
}

type NonHumanIdentityRelationshipItem struct {
	ID                          int64
	AccountID                   int64
	AccountDisplayName          string
	AccountExternalID           string
	AccountSourceKind           string
	AccountSourceName           string
	IdentityID                  int64
	IdentityHref                string
	IdentityDisplayName         string
	IdentityPrimaryEmail        string
	RelationshipType            string
	RelationshipIdentityKind    string
	RelationshipResolutionState string
	Confidence                  int32
	LastSeen                    TimeDisplay
}

type NonHumanIdentitiesShowViewData struct {
	Layout                    LayoutData
	Principal                 NonHumanIdentitiesSummaryView
	RiskSignals               []NonHumanIdentitiesRiskSignal
	HasRiskSignals            bool
	Relationships             []NonHumanIdentityRelationshipItem
	HasRelationships          bool
	CanAssignRelationships    bool
	RelationshipAction        string
	RelationshipIdentityEmail string
	RelationshipTypeInput     string
	RelatedAssets             []NonHumanIdentitiesRelatedAssetItem
	Credentials               []NonHumanIdentitiesRelatedCredentialItem
	HasAssets                 bool
	HasCredentials            bool
}
