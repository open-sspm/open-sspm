package viewmodels

import "github.com/open-sspm/open-sspm/internal/http/querystate"

type NonHumanAccessListItem struct {
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
	LastSeenOn             string
	LastSeenRelative       string
	ActivityState          string
	FreshnessState         string
	GovernanceState        string
	AccountableOwner       string
	AccountableOwnerHref   string
	OwnerPresence          string
	RiskLevel              string
}

type NonHumanAccessViewData struct {
	PaginatedListPageData
	Items             []NonHumanAccessListItem
	Sources           []ProgrammaticSourceOption
	SourceNameOptions []ProgrammaticSourceOption
	Query             querystate.NonHumanAccessQuery
	HasItems          bool
}

type NonHumanAccessSummaryView struct {
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
	LastSeenOn                   string
	LastSeenRelative             string
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

type NonHumanAccessRelatedAssetItem struct {
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
	EvidenceSeenOn      string
}

type NonHumanAccessRelatedCredentialItem struct {
	ID              int64
	Href            string
	SourceKind      string
	SourceName      string
	CredentialKind  string
	DisplayName     string
	ExternalID      string
	Status          string
	RiskLevel       string
	ExpiresAt       string
	LastUsedAt      string
	CreatedBy       string
	CreatedByHref   string
	ApprovedBy      string
	ApprovedByHref  string
	AppAssetID      int64
	AppAssetDisplay string
	AppAssetHref    string
}

type NonHumanAccessShowViewData struct {
	Layout         LayoutData
	Principal      NonHumanAccessSummaryView
	RiskReasons    []string
	RelatedAssets  []NonHumanAccessRelatedAssetItem
	Credentials    []NonHumanAccessRelatedCredentialItem
	HasAssets      bool
	HasCredentials bool
}
