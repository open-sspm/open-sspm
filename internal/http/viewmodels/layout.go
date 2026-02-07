package viewmodels

type LayoutData struct {
	Title                       string
	CSRFToken                   string
	UserEmail                   string
	UserRole                    string
	IsAdmin                     bool
	FindingsRulesets            []FindingsRulesetItem
	GitHubOrg                   string
	GitHubEnabled               bool
	GitHubConfigured            bool
	DatadogSite                 string
	DatadogEnabled              bool
	DatadogConfigured           bool
	AWSIdentityCenterName       string
	AWSIdentityCenterEnabled    bool
	AWSIdentityCenterConfigured bool
	EntraTenantID               string
	EntraEnabled                bool
	EntraConfigured             bool
	Toast                       *ToastViewData
	ActivePath                  string
	CommandUsers                []DashboardCommandUserItem
	CommandApps                 []DashboardCommandAppItem
}
