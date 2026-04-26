package viewmodels

type ResyncBanner struct {
	Class   string
	Title   string
	Message string
}

type RiskPolicyPackSummary struct {
	Domain  string
	ID      string
	Version string
}

type SettingsViewData struct {
	Layout                LayoutData
	SyncInterval          string
	SyncDiscoveryInterval string
	SyncDiscoveryEnabled  bool
	ResyncEnabled         bool
	ResyncBanner          *ResyncBanner
	RiskPolicyPacks       []RiskPolicyPackSummary
	RiskPolicyExpressions int
}
