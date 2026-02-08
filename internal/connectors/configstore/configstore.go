package configstore

import (
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
)

const (
	defaultGitHubAPIBase = "https://api.github.com"
	defaultDatadogSite   = "datadoghq.com"
)

const (
	AWSIdentityCenterAuthTypeDefaultChain = "default_chain"
	AWSIdentityCenterAuthTypeAccessKey    = "access_key"
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

type VaultConfig struct{}

type EntraConfig struct {
	TenantID         string `json:"tenant_id"`
	ClientID         string `json:"client_id"`
	ClientSecret     string `json:"client_secret"`
	DiscoveryEnabled bool   `json:"discovery_enabled"`
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

func (c VaultConfig) Validate() error {
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
	var cfg VaultConfig
	return cfg, decodeJSON(raw, &cfg)
}

func DecodeEntraConfig(raw []byte) (EntraConfig, error) {
	var cfg EntraConfig
	return cfg, decodeJSON(raw, &cfg)
}

func EncodeConfig(v any) ([]byte, error) {
	return json.Marshal(v)
}

func MergeOktaConfig(existing OktaConfig, update OktaConfig) OktaConfig {
	merged := existing
	merged.Domain = strings.TrimSpace(update.Domain)
	merged.DiscoveryEnabled = update.DiscoveryEnabled
	if token := strings.TrimSpace(update.Token); token != "" {
		merged.Token = token
	}
	return merged
}

func MergeGitHubConfig(existing GitHubConfig, update GitHubConfig) GitHubConfig {
	merged := existing
	merged.Org = strings.TrimSpace(update.Org)
	merged.APIBase = strings.TrimSpace(update.APIBase)
	merged.Enterprise = strings.TrimSpace(update.Enterprise)
	merged.SCIMEnabled = update.SCIMEnabled
	if token := strings.TrimSpace(update.Token); token != "" {
		merged.Token = token
	}
	return merged
}

func MergeDatadogConfig(existing DatadogConfig, update DatadogConfig) DatadogConfig {
	merged := existing
	merged.Site = strings.TrimSpace(update.Site)
	if key := strings.TrimSpace(update.APIKey); key != "" {
		merged.APIKey = key
	}
	if key := strings.TrimSpace(update.AppKey); key != "" {
		merged.AppKey = key
	}
	return merged
}

func MergeAWSIdentityCenterConfig(existing AWSIdentityCenterConfig, update AWSIdentityCenterConfig) AWSIdentityCenterConfig {
	merged := existing
	merged.Region = strings.TrimSpace(update.Region)
	merged.Name = strings.TrimSpace(update.Name)
	merged.InstanceARN = strings.TrimSpace(update.InstanceARN)
	merged.IdentityStoreID = strings.TrimSpace(update.IdentityStoreID)
	merged.AuthType = strings.ToLower(strings.TrimSpace(update.AuthType))
	if merged.AuthType == "" {
		merged.AuthType = AWSIdentityCenterAuthTypeDefaultChain
	}

	switch merged.AuthType {
	case AWSIdentityCenterAuthTypeDefaultChain:
		merged.AccessKeyID = ""
		merged.SecretAccessKey = ""
		merged.SessionToken = ""
	case AWSIdentityCenterAuthTypeAccessKey:
		if accessKeyID := strings.TrimSpace(update.AccessKeyID); accessKeyID != "" {
			merged.AccessKeyID = accessKeyID
		}
		if secret := strings.TrimSpace(update.SecretAccessKey); secret != "" {
			merged.SecretAccessKey = secret
		}
		if token := strings.TrimSpace(update.SessionToken); token != "" {
			merged.SessionToken = token
		}
	}
	return merged
}

func MergeEntraConfig(existing EntraConfig, update EntraConfig) EntraConfig {
	merged := existing
	merged.TenantID = normalizeGUID(update.TenantID)
	merged.ClientID = normalizeGUID(update.ClientID)
	merged.DiscoveryEnabled = update.DiscoveryEnabled
	if secret := strings.TrimSpace(update.ClientSecret); secret != "" {
		merged.ClientSecret = secret
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
