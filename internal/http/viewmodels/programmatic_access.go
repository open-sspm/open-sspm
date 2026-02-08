package viewmodels

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
	LastSeenAt       string
}

type AppAssetsViewData struct {
	Layout             LayoutData
	Sources            []ProgrammaticSourceOption
	SelectedSourceKind string
	SelectedSourceName string
	Query              string
	AssetKind          string
	Items              []AppAssetListItem
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

type ProgrammaticAuditEventItem struct {
	EventType             string
	EventTime             string
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
	ExpiresAt      string
	LastUsedAt     string
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
	CreatedAtSource  string
	UpdatedAtSource  string
	LastObservedAt   string
}

type AppAssetShowViewData struct {
	Layout         LayoutData
	Asset          AppAssetSummaryView
	Owners         []AppAssetOwnerItem
	Credentials    []AppAssetCredentialItem
	AuditEvents    []ProgrammaticAuditEventItem
	HasOwners      bool
	HasCredentials bool
	HasAuditEvents bool
}

type CredentialArtifactListItem struct {
	ID             int64
	SourceKind     string
	SourceName     string
	CredentialKind string
	DisplayName    string
	ExternalID     string
	AssetRef       string
	Status         string
	RiskLevel      string
	ExpiresAt      string
	ExpiresIn      string
	LastUsedAt     string
	CreatedBy      string
	CreatedByHref  string
	ApprovedBy     string
	ApprovedByHref string
}

type CredentialsViewData struct {
	Layout             LayoutData
	Sources            []ProgrammaticSourceOption
	SelectedSourceKind string
	SelectedSourceName string
	Query              string
	CredentialKind     string
	Status             string
	RiskLevel          string
	ExpiryState        string
	ExpiresInDays      int
	Items              []CredentialArtifactListItem
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
	CreatedAtSource    string
	ExpiresAtSource    string
	LastUsedAtSource   string
	CreatedBy          string
	CreatedByHref      string
	ApprovedBy         string
	ApprovedByHref     string
	AssetHref          string
}

type CredentialShowViewData struct {
	Layout      LayoutData
	Credential  CredentialArtifactSummaryView
	ScopeJSON   string
	AuditEvents []ProgrammaticAuditEventItem
	RiskReasons []string
	HasEvents   bool
}
