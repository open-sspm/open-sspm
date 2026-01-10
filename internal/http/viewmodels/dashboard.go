package viewmodels

type DashboardViewData struct {
	Layout            LayoutData
	ActiveUserCount   int64
	AppCount          int64
	ConnectedAppCount int64
	OktaCount         int64
	GitHubCount       int64
	DatadogCount      int64
	MatchedCount      int64
	UnmatchedCount    int64
	FrameworkPosture  []DashboardFrameworkPostureItem
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

type DashboardFrameworkPostureItem struct {
	Key         string
	Name        string
	PassedCount int64
	TotalCount  int64
	PassPercent int
	BadgeLabel  string
	Href        string
}
