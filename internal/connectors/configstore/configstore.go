package configstore

import (
	"crypto/x509"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
)

const (
	KindOkta              = "okta"
	KindGitHub            = "github"
	KindDatadog           = "datadog"
	KindAWSIdentityCenter = "aws_identity_center"
	KindEntra             = "entra"
	KindVault             = "vault"
	KindGoogleWorkspace   = "google_workspace"
)

const (
	defaultGitHubAPIBase = "https://api.github.com"
	defaultDatadogSite   = "datadoghq.com"
)

const (
	AWSIdentityCenterAuthTypeDefaultChain     = "default_chain"
	AWSIdentityCenterAuthTypeAccessKey        = "access_key"
	VaultAuthTypeToken                        = "token"
	VaultAuthTypeAppRole                      = "approle"
	GoogleWorkspaceAuthTypeServiceAccountJSON = "service_account_json"
	GoogleWorkspaceAuthTypeADC                = "adc"
)

type OktaConfig struct {
	Domain           string `json:"domain"`
	Token            string `json:"token"`
	DiscoveryEnabled bool   `json:"discovery_enabled"`
}

func (c OktaConfig) Normalized() OktaConfig {
	out := c
	out.Domain = strings.TrimSpace(out.Domain)
	out.Token = strings.TrimSpace(out.Token)
	return out
}

func (c OktaConfig) BaseURL() string {
	base := strings.TrimSpace(c.Domain)
	if base == "" {
		return ""
	}
	if !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://") {
		base = "https://" + base
	}
	return strings.TrimRight(base, "/")
}

func (c OktaConfig) Validate() error {
	c = c.Normalized()
	if c.Domain == "" {
		return errors.New("Okta domain is required")
	}
	if c.Token == "" {
		return errors.New("Okta token is required")
	}
	return nil
}

type GitHubConfig struct {
	Token      string `json:"token"`
	Org        string `json:"org"`
	APIBase    string `json:"api_base"`
	Enterprise string `json:"enterprise"`
	// SCIMEnabled toggles whether to use the SCIM API for identity/email resolution.
	SCIMEnabled bool `json:"scim_enabled"`
}

func (c GitHubConfig) Normalized() GitHubConfig {
	out := c
	out.Token = strings.TrimSpace(out.Token)
	out.Org = strings.TrimSpace(out.Org)
	out.APIBase = strings.TrimSpace(out.APIBase)
	out.Enterprise = strings.TrimSpace(out.Enterprise)
	if out.APIBase == "" {
		out.APIBase = defaultGitHubAPIBase
	}
	out.APIBase = strings.TrimRight(out.APIBase, "/")
	return out
}

func (c GitHubConfig) Validate() error {
	c = c.Normalized()
	if c.Token == "" {
		return errors.New("GitHub token is required")
	}
	if c.Org == "" {
		return errors.New("GitHub org is required")
	}
	if c.APIBase == "" {
		return errors.New("GitHub API base is required")
	}
	return nil
}

type DatadogConfig struct {
	APIKey string `json:"api_key"`
	AppKey string `json:"app_key"`
	Site   string `json:"site"`
}

func (c DatadogConfig) Normalized() DatadogConfig {
	out := c
	out.APIKey = strings.TrimSpace(out.APIKey)
	out.AppKey = strings.TrimSpace(out.AppKey)
	out.Site = normalizeDatadogSite(out.Site)
	if out.Site == "" {
		out.Site = defaultDatadogSite
	}
	return out
}

func (c DatadogConfig) APIBaseURL() string {
	site := normalizeDatadogSite(c.Site)
	if site == "" {
		site = defaultDatadogSite
	}
	return "https://api." + site
}

func (c DatadogConfig) Validate() error {
	c = c.Normalized()
	if c.APIKey == "" {
		return errors.New("Datadog API key is required")
	}
	if c.AppKey == "" {
		return errors.New("Datadog app key is required")
	}
	if c.Site == "" {
		return errors.New("Datadog site is required")
	}
	return nil
}

type AWSIdentityCenterConfig struct {
	Region          string `json:"region"`
	Name            string `json:"name"`
	InstanceARN     string `json:"instance_arn"`
	IdentityStoreID string `json:"identity_store_id"`
	AuthType        string `json:"auth_type"`
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	SessionToken    string `json:"session_token"`
}

func (c AWSIdentityCenterConfig) Normalized() AWSIdentityCenterConfig {
	out := c
	out.Region = strings.TrimSpace(out.Region)
	out.Name = strings.TrimSpace(out.Name)
	out.InstanceARN = strings.TrimSpace(out.InstanceARN)
	out.IdentityStoreID = strings.TrimSpace(out.IdentityStoreID)
	out.AuthType = strings.ToLower(strings.TrimSpace(out.AuthType))
	if out.AuthType == "" {
		out.AuthType = AWSIdentityCenterAuthTypeDefaultChain
	}
	out.AccessKeyID = strings.TrimSpace(out.AccessKeyID)
	out.SecretAccessKey = strings.TrimSpace(out.SecretAccessKey)
	out.SessionToken = strings.TrimSpace(out.SessionToken)
	return out
}

func (c AWSIdentityCenterConfig) Validate() error {
	c = c.Normalized()
	if c.Region == "" {
		return errors.New("AWS Identity Center region is required")
	}
	switch c.AuthType {
	case AWSIdentityCenterAuthTypeDefaultChain:
		return nil
	case AWSIdentityCenterAuthTypeAccessKey:
		if c.AccessKeyID == "" {
			return errors.New("AWS access key ID is required")
		}
		if c.SecretAccessKey == "" {
			return errors.New("AWS secret access key is required")
		}
		return nil
	default:
		return errors.New("AWS credentials type is invalid")
	}
}

type VaultConfig struct {
	Address          string `json:"address"`
	Namespace        string `json:"namespace"`
	Name             string `json:"name"`
	AuthType         string `json:"auth_type"`
	Token            string `json:"token"`
	AppRoleMountPath string `json:"approle_mount_path"`
	AppRoleRoleID    string `json:"approle_role_id"`
	AppRoleSecretID  string `json:"approle_secret_id"`
	ScanAuthRoles    bool   `json:"scan_auth_roles"`
	TLSSkipVerify    bool   `json:"tls_skip_verify"`
	TLSCACertPEM     string `json:"tls_ca_cert_pem"`
}

type EntraConfig struct {
	TenantID         string `json:"tenant_id"`
	ClientID         string `json:"client_id"`
	ClientSecret     string `json:"client_secret"`
	DiscoveryEnabled bool   `json:"discovery_enabled"`
}

type GoogleWorkspaceConfig struct {
	CustomerID          string `json:"customer_id"`
	PrimaryDomain       string `json:"primary_domain"`
	DelegatedAdminEmail string `json:"delegated_admin_email"`
	AuthType            string `json:"auth_type"`
	ServiceAccountJSON  string `json:"service_account_json"`
	ServiceAccountEmail string `json:"service_account_email"`
	DiscoveryEnabled    bool   `json:"discovery_enabled"`
}

func (c EntraConfig) Normalized() EntraConfig {
	out := c
	out.TenantID = normalizeGUID(out.TenantID)
	out.ClientID = normalizeGUID(out.ClientID)
	out.ClientSecret = strings.TrimSpace(out.ClientSecret)
	return out
}

func (c EntraConfig) Validate() error {
	c = c.Normalized()
	if c.TenantID == "" {
		return errors.New("Entra tenant ID is required")
	}
	if c.ClientID == "" {
		return errors.New("Entra client ID is required")
	}
	if c.ClientSecret == "" {
		return errors.New("Entra client secret is required")
	}
	return nil
}

func (c GoogleWorkspaceConfig) Normalized() GoogleWorkspaceConfig {
	out := c
	out.CustomerID = strings.TrimSpace(out.CustomerID)
	out.PrimaryDomain = strings.TrimSpace(out.PrimaryDomain)
	out.DelegatedAdminEmail = strings.TrimSpace(out.DelegatedAdminEmail)
	out.AuthType = strings.ToLower(strings.TrimSpace(out.AuthType))
	if out.AuthType == "" {
		out.AuthType = GoogleWorkspaceAuthTypeServiceAccountJSON
	}
	out.ServiceAccountJSON = strings.TrimSpace(out.ServiceAccountJSON)
	out.ServiceAccountEmail = strings.TrimSpace(out.ServiceAccountEmail)
	return out
}

func (c GoogleWorkspaceConfig) Validate() error {
	c = c.Normalized()
	if c.CustomerID == "" {
		return errors.New("Google Workspace customer ID is required")
	}
	if c.DelegatedAdminEmail == "" {
		return errors.New("Google Workspace delegated admin email is required")
	}
	switch c.AuthType {
	case GoogleWorkspaceAuthTypeServiceAccountJSON:
		if c.ServiceAccountJSON == "" {
			return errors.New("Google Workspace service account JSON is required")
		}
		var payload struct {
			ClientEmail string `json:"client_email"`
			PrivateKey  string `json:"private_key"`
			TokenURI    string `json:"token_uri"`
		}
		if err := json.Unmarshal([]byte(c.ServiceAccountJSON), &payload); err != nil {
			return errors.New("Google Workspace service account JSON is invalid")
		}
		if strings.TrimSpace(payload.ClientEmail) == "" || strings.TrimSpace(payload.PrivateKey) == "" {
			return errors.New("Google Workspace service account JSON must include client_email and private_key")
		}
	case GoogleWorkspaceAuthTypeADC:
		if c.ServiceAccountEmail == "" {
			return errors.New("Google Workspace service account email is required for ADC auth")
		}
	default:
		return errors.New("Google Workspace auth type is invalid")
	}
	return nil
}

func (c VaultConfig) Normalized() VaultConfig {
	out := c
	out.Address = normalizeVaultAddress(out.Address)
	out.Namespace = strings.TrimSpace(out.Namespace)
	out.Name = strings.TrimSpace(out.Name)
	out.AuthType = strings.ToLower(strings.TrimSpace(out.AuthType))
	if out.AuthType == "" {
		out.AuthType = VaultAuthTypeToken
	}
	out.Token = strings.TrimSpace(out.Token)
	out.AppRoleMountPath = normalizeVaultMountPath(out.AppRoleMountPath)
	if out.AppRoleMountPath == "" {
		out.AppRoleMountPath = "approle"
	}
	out.AppRoleRoleID = strings.TrimSpace(out.AppRoleRoleID)
	out.AppRoleSecretID = strings.TrimSpace(out.AppRoleSecretID)
	out.TLSCACertPEM = strings.TrimSpace(out.TLSCACertPEM)
	return out
}

func (c VaultConfig) SourceName() string {
	c = c.Normalized()
	if c.Name != "" {
		return c.Name
	}
	if c.Address == "" {
		return ""
	}
	u, err := url.Parse(c.Address)
	if err != nil {
		return ""
	}
	if host := strings.TrimSpace(u.Hostname()); host != "" {
		return host
	}
	return strings.TrimSpace(u.Host)
}

func (c VaultConfig) Validate() error {
	c = c.Normalized()
	if c.Address == "" {
		return errors.New("Vault address is required")
	}
	parsed, err := url.Parse(c.Address)
	if err != nil {
		return errors.New("Vault address is invalid")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return errors.New("Vault address must use http or https")
	}
	if strings.TrimSpace(parsed.Hostname()) == "" {
		return errors.New("Vault address host is required")
	}
	switch c.AuthType {
	case VaultAuthTypeToken:
		if c.Token == "" {
			return errors.New("Vault token is required")
		}
	case VaultAuthTypeAppRole:
		if c.AppRoleMountPath == "" {
			return errors.New("Vault AppRole mount path is required")
		}
		if c.AppRoleRoleID == "" {
			return errors.New("Vault AppRole role ID is required")
		}
		if c.AppRoleSecretID == "" {
			return errors.New("Vault AppRole secret ID is required")
		}
	default:
		return errors.New("Vault auth type is invalid")
	}
	if c.TLSCACertPEM != "" {
		pool := x509.NewCertPool()
		if ok := pool.AppendCertsFromPEM([]byte(c.TLSCACertPEM)); !ok {
			return errors.New("Vault CA certificate PEM is invalid")
		}
	}
	return nil
}

func DecodeOktaConfig(raw []byte) (OktaConfig, error) {
	var cfg OktaConfig
	return cfg, decodeJSON(raw, &cfg)
}

func DecodeGitHubConfig(raw []byte) (GitHubConfig, error) {
	var cfg GitHubConfig
	return cfg, decodeJSON(raw, &cfg)
}

func DecodeDatadogConfig(raw []byte) (DatadogConfig, error) {
	var cfg DatadogConfig
	return cfg, decodeJSON(raw, &cfg)
}

func DecodeAWSIdentityCenterConfig(raw []byte) (AWSIdentityCenterConfig, error) {
	var cfg AWSIdentityCenterConfig
	return cfg, decodeJSON(raw, &cfg)
}

func DecodeVaultConfig(raw []byte) (VaultConfig, error) {
	cfg := VaultConfig{
		AuthType:      VaultAuthTypeToken,
		ScanAuthRoles: true,
	}
	return cfg, decodeJSON(raw, &cfg)
}

func DecodeEntraConfig(raw []byte) (EntraConfig, error) {
	var cfg EntraConfig
	return cfg, decodeJSON(raw, &cfg)
}

func DecodeGoogleWorkspaceConfig(raw []byte) (GoogleWorkspaceConfig, error) {
	cfg := GoogleWorkspaceConfig{
		AuthType: GoogleWorkspaceAuthTypeServiceAccountJSON,
	}
	return cfg, decodeJSON(raw, &cfg)
}

func EncodeConfig(v any) ([]byte, error) {
	return json.Marshal(v)
}

func MergeOktaConfig(existing OktaConfig, update OktaConfig) OktaConfig {
	merged := existing
	replaceTrimmed(&merged.Domain, update.Domain)
	merged.DiscoveryEnabled = update.DiscoveryEnabled
	replaceIfNonEmptyTrimmed(&merged.Token, update.Token)
	return merged
}

func MergeGitHubConfig(existing GitHubConfig, update GitHubConfig) GitHubConfig {
	merged := existing
	replaceTrimmed(&merged.Org, update.Org)
	replaceTrimmed(&merged.APIBase, update.APIBase)
	replaceTrimmed(&merged.Enterprise, update.Enterprise)
	merged.SCIMEnabled = update.SCIMEnabled
	replaceIfNonEmptyTrimmed(&merged.Token, update.Token)
	return merged
}

func MergeDatadogConfig(existing DatadogConfig, update DatadogConfig) DatadogConfig {
	merged := existing
	replaceTrimmed(&merged.Site, update.Site)
	replaceIfNonEmptyTrimmed(&merged.APIKey, update.APIKey)
	replaceIfNonEmptyTrimmed(&merged.AppKey, update.AppKey)
	return merged
}

func MergeAWSIdentityCenterConfig(existing AWSIdentityCenterConfig, update AWSIdentityCenterConfig) AWSIdentityCenterConfig {
	merged := existing
	replaceTrimmed(&merged.Region, update.Region)
	replaceTrimmed(&merged.Name, update.Name)
	replaceTrimmed(&merged.InstanceARN, update.InstanceARN)
	replaceTrimmed(&merged.IdentityStoreID, update.IdentityStoreID)
	replaceLowerTrimmed(&merged.AuthType, update.AuthType)
	if merged.AuthType == "" {
		merged.AuthType = AWSIdentityCenterAuthTypeDefaultChain
	}

	switch merged.AuthType {
	case AWSIdentityCenterAuthTypeDefaultChain:
		merged.AccessKeyID = ""
		merged.SecretAccessKey = ""
		merged.SessionToken = ""
	case AWSIdentityCenterAuthTypeAccessKey:
		replaceIfNonEmptyTrimmed(&merged.AccessKeyID, update.AccessKeyID)
		replaceIfNonEmptyTrimmed(&merged.SecretAccessKey, update.SecretAccessKey)
		replaceIfNonEmptyTrimmed(&merged.SessionToken, update.SessionToken)
	}
	return merged
}

func MergeEntraConfig(existing EntraConfig, update EntraConfig) EntraConfig {
	merged := existing
	replaceNormalized(&merged.TenantID, update.TenantID, normalizeGUID)
	replaceNormalized(&merged.ClientID, update.ClientID, normalizeGUID)
	merged.DiscoveryEnabled = update.DiscoveryEnabled
	replaceIfNonEmptyTrimmed(&merged.ClientSecret, update.ClientSecret)
	return merged
}

func MergeGoogleWorkspaceConfig(existing GoogleWorkspaceConfig, update GoogleWorkspaceConfig) GoogleWorkspaceConfig {
	merged := existing
	replaceTrimmed(&merged.CustomerID, update.CustomerID)
	replaceTrimmed(&merged.PrimaryDomain, update.PrimaryDomain)
	replaceTrimmed(&merged.DelegatedAdminEmail, update.DelegatedAdminEmail)
	merged.DiscoveryEnabled = update.DiscoveryEnabled
	replaceLowerTrimmed(&merged.AuthType, update.AuthType)
	if merged.AuthType == "" {
		merged.AuthType = GoogleWorkspaceAuthTypeServiceAccountJSON
	}

	switch merged.AuthType {
	case GoogleWorkspaceAuthTypeServiceAccountJSON:
		merged.ServiceAccountEmail = ""
		replaceIfNonEmptyTrimmed(&merged.ServiceAccountJSON, update.ServiceAccountJSON)
	case GoogleWorkspaceAuthTypeADC:
		merged.ServiceAccountJSON = ""
		replaceIfNonEmptyTrimmed(&merged.ServiceAccountEmail, update.ServiceAccountEmail)
	}

	return merged
}

func MergeVaultConfig(existing VaultConfig, update VaultConfig) VaultConfig {
	merged := existing
	replaceTrimmed(&merged.Address, update.Address)
	replaceTrimmed(&merged.Namespace, update.Namespace)
	replaceTrimmed(&merged.Name, update.Name)
	replaceLowerTrimmed(&merged.AuthType, update.AuthType)
	if merged.AuthType == "" {
		merged.AuthType = VaultAuthTypeToken
	}
	merged.ScanAuthRoles = update.ScanAuthRoles
	merged.TLSSkipVerify = update.TLSSkipVerify
	replaceIfNonEmptyNormalized(&merged.AppRoleMountPath, update.AppRoleMountPath, normalizeVaultMountPath)
	replaceIfNonEmptyTrimmed(&merged.TLSCACertPEM, update.TLSCACertPEM)
	switch merged.AuthType {
	case VaultAuthTypeToken:
		merged.AppRoleRoleID = ""
		merged.AppRoleSecretID = ""
		replaceIfNonEmptyTrimmed(&merged.Token, update.Token)
	case VaultAuthTypeAppRole:
		merged.Token = ""
		if merged.AppRoleMountPath == "" {
			merged.AppRoleMountPath = "approle"
		}
		replaceIfNonEmptyTrimmed(&merged.AppRoleRoleID, update.AppRoleRoleID)
		replaceIfNonEmptyTrimmed(&merged.AppRoleSecretID, update.AppRoleSecretID)
	}
	return merged
}

func MaskSecret(secret string) string {
	s := strings.TrimSpace(secret)
	if s == "" {
		return ""
	}
	if len(s) <= 4 {
		return "****"
	}
	tail := s[len(s)-4:]
	prefix := ""
	if idx := strings.Index(s, "_"); idx > 0 && idx <= 6 {
		prefix = s[:idx+1]
	}
	return prefix + "****" + tail
}

func decodeJSON(raw []byte, dst any) error {
	if len(raw) == 0 {
		return nil
	}
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" {
		return nil
	}
	return json.Unmarshal(raw, dst)
}

func replaceTrimmed(dst *string, raw string) {
	*dst = strings.TrimSpace(raw)
}

func replaceLowerTrimmed(dst *string, raw string) {
	*dst = strings.ToLower(strings.TrimSpace(raw))
}

func replaceNormalized(dst *string, raw string, normalize func(string) string) {
	*dst = normalize(raw)
}

func replaceIfNonEmptyTrimmed(dst *string, raw string) {
	if value := strings.TrimSpace(raw); value != "" {
		*dst = value
	}
}

func replaceIfNonEmptyNormalized(dst *string, raw string, normalize func(string) string) {
	if value := normalize(raw); value != "" {
		*dst = value
	}
}

func normalizeGUID(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.TrimPrefix(s, "{")
	s = strings.TrimSuffix(s, "}")
	return strings.TrimSpace(s)
}

func normalizeDatadogSite(raw string) string {
	site := strings.TrimSpace(raw)
	if site == "" {
		return ""
	}
	if strings.Contains(site, "://") {
		if u, err := url.Parse(site); err == nil && u.Host != "" {
			site = u.Host
		}
	}
	site = strings.Trim(site, "/")
	site = strings.TrimPrefix(site, "api.")
	return site
}

func normalizeVaultAddress(raw string) string {
	addr := strings.TrimSpace(raw)
	if addr == "" {
		return ""
	}
	if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
		addr = "https://" + addr
	}
	parsed, err := url.Parse(addr)
	if err != nil {
		return strings.TrimRight(addr, "/")
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return strings.TrimSpace(parsed.String())
}

func normalizeVaultMountPath(raw string) string {
	return strings.Trim(strings.TrimSpace(raw), "/")
}
