package viewmodels

type DashboardViewData struct {
	Layout            LayoutData
	ActiveUserCount   int64
	AppCount          int64
	ConnectedAppCount int64
	FrameworkPosture  []DashboardFrameworkPostureItem
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
