package viewmodels

type ConnectorAlert struct {
	Class   string
	Title   string
	Message string
}

type OktaConnectorViewData struct {
	Enabled          bool
	Configured       bool
	Domain           string
	TokenMasked      string
	HasToken         bool
	DiscoveryEnabled bool
	Authoritative    bool
}

type GitHubConnectorViewData struct {
	Enabled                bool
	Configured             bool
	Org                    string
	APIBase                string
	Enterprise             string
	SCIMEnabled            bool
	TokenMasked            string
	HasToken               bool
	ProgrammaticAssets     int64
	ExpiringCredentials30d int64
	HasProgrammaticSummary bool
}

type DatadogConnectorViewData struct {
	Enabled      bool
	Configured   bool
	Site         string
	APIKeyMasked string
	AppKeyMasked string
	HasAPIKey    bool
	HasAppKey    bool
}

type AWSIdentityCenterConnectorViewData struct {
	Enabled          bool
	Configured       bool
	Region           string
	Name             string
	InstanceARN      string
	IdentityStoreID  string
	AuthType         string
	AccessKeyIDMask  string
	HasAccessKeyID   bool
	SecretKeyMask    string
	HasSecretKey     bool
	SessionTokenMask string
	HasSessionToken  bool
}

type VaultConnectorViewData struct {
	Enabled             bool
	Configured          bool
	Address             string
	Namespace           string
	Name                string
	AuthType            string
	TokenMasked         string
	HasToken            bool
	AppRoleMountPath    string
	AppRoleRoleID       string
	HasAppRoleRoleID    bool
	AppRoleSecretMasked string
	HasAppRoleSecretID  bool
	ScanAuthRoles       bool
	TLSSkipVerify       bool
	HasTLSCACert        bool
}

type EntraConnectorViewData struct {
	Enabled                bool
	Configured             bool
	TenantID               string
	ClientID               string
	ClientSecretMasked     string
	HasClientSecret        bool
	DiscoveryEnabled       bool
	Authoritative          bool
	ProgrammaticAssets     int64
	ExpiringCredentials30d int64
	HasProgrammaticSummary bool
}

type ConnectorsViewData struct {
	Layout            LayoutData
	Alert             *ConnectorAlert
	SavedName         string
	OpenKind          string
	Okta              OktaConnectorViewData
	GitHub            GitHubConnectorViewData
	Datadog           DatadogConnectorViewData
	AWSIdentityCenter AWSIdentityCenterConnectorViewData
	Entra             EntraConnectorViewData
	Vault             VaultConnectorViewData
}
