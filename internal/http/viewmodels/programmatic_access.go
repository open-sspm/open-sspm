package viewmodels

import "github.com/open-sspm/open-sspm/internal/http/querystate"

type ProgrammaticSourceOption struct {
	SourceKind string
	SourceName string
	Label      string
}

type AppAssetListItem struct {
	ID               int64
	SourceKind       string
	SourceName       string
	AssetKind        string
	DisplayName      string
	ExternalID       string
	Status           string
	OwnersCount      int
	CredentialsCount int
	LastSeen         TimeDisplay
}

type AppAssetsViewData struct {
	PaginatedListPageData
	Sources         []ProgrammaticSourceOption
	Query           querystate.AppAssetsQuery
	Items           []AppAssetListItem
	GoogleOAuthView *ConnectedAppsViewData
	HasItems        bool
}

type ProgrammaticAuditEventItem struct {
	EventType             string
	EventTime             TimeDisplay
	Actor                 string
	Target                string
	CredentialKind        string
	CredentialExternalID  string
	CredentialDisplayName string
}

type AppAssetOwnerItem struct {
	OwnerKind         string
	OwnerDisplayName  string
	OwnerEmail        string
	OwnerExternalID   string
	OwnerIdentityHref string
}

type AppAssetCredentialItem struct {
	ID             int64
	Href           string
	CredentialKind string
	DisplayName    string
	Status         string
	RiskLevel      string
	ExpiresAt      TimeDisplay
	LastUsedAt     TimeDisplay
	CreatedBy      string
	CreatedByHref  string
}

type AppAssetSummaryView struct {
	ID               int64
	SourceKind       string
	SourceName       string
	AssetKind        string
	DisplayName      string
	ExternalID       string
	ParentExternalID string
	Status           string
	CreatedAtSource  TimeDisplay
	UpdatedAtSource  TimeDisplay
	LastObservedAt   TimeDisplay
}

type AppAssetShowViewData struct {
	Layout          LayoutData
	Asset           AppAssetSummaryView
	Owners          []AppAssetOwnerItem
	Credentials     []AppAssetCredentialItem
	AuditEvents     []ProgrammaticAuditEventItem
	GoogleOAuthView *ConnectedAppShowViewData
	HasOwners       bool
	HasCredentials  bool
	HasAuditEvents  bool
}

type CredentialArtifactListItem struct {
	ID             int64
	SourceKind     string
	SourceName     string
	CredentialKind string
	DisplayName    string
	NamePrimary    string
	NameSecondary  string
	ExternalID     string
	AssetRef       string
	AssetRefKind   string
	AssetRefID     string
	AssetName      string
	Status         string
	RiskLevel      string
	RowStateTone   string
	RowStateLabel  string
	ExpiresAt      TimeDisplay
	ExpiresTone    string
	LastUsedAt     TimeDisplay
	CreatedBy      string
	CreatedByHref  string
	ApprovedBy     string
	ApprovedByHref string
	VersionCount   int64
}

// CredentialsSummary holds lineage-grouped operator counts for the stat strip
// and segment chips on /credentials. Scoped by source/q/credential_kind so the
// numbers reflect the population the user is actually browsing.
type CredentialsSummary struct {
	Total           int64
	Active          int64
	Expired         int64
	ExpiringSoon    int64
	Critical        int64
	High            int64
	Warning         int64
	PendingApproval int64
	Revoked         int64
	AssetCount      int64
}

type CredentialsViewData struct {
	PaginatedListPageData
	Sources         []ProgrammaticSourceOption
	Query           querystate.CredentialsQuery
	Items           []CredentialArtifactListItem
	Summary         CredentialsSummary
	HasItems        bool
	HasLastUsedData bool
}

type CredentialArtifactSummaryView struct {
	ID                 int64
	SourceKind         string
	SourceName         string
	CredentialKind     string
	DisplayName        string
	ExternalID         string
	AssetRefKind       string
	AssetRefExternalID string
	Status             string
	RiskLevel          string
	CreatedAtSource    TimeDisplay
	ExpiresAtSource    TimeDisplay
	LastUsedAtSource   TimeDisplay
	CreatedBy          string
	CreatedByHref      string
	ApprovedBy         string
	ApprovedByHref     string
	AssetHref          string
}

type CredentialRiskFinding struct {
	Severity string // "critical", "high", "medium", "low", "info"
	Title    string
	Evidence string
}

type CredentialShowViewData struct {
	Layout       LayoutData
	Credential   CredentialArtifactSummaryView
	ScopeJSON    string
	AuditEvents  []ProgrammaticAuditEventItem
	RiskFindings []CredentialRiskFinding
	HasEvents    bool
	HasFindings  bool
}
