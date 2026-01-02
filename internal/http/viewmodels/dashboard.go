package viewmodels

type DashboardViewData struct {
	Layout         LayoutData
	OktaCount      int64
	GitHubCount    int64
	DatadogCount   int64
	MatchedCount   int64
	UnmatchedCount int64
	CommandUsers   []DashboardCommandUserItem
	CommandApps    []DashboardCommandAppItem
}

type DashboardCommandUserItem struct {
	ID          int64
	Email       string
	DisplayName string
	Status      string
}

type DashboardCommandAppItem struct {
	ExternalID string
	Label      string
	Name       string
	Status     string
}
