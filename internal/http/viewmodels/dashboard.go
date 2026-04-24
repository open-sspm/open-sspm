package viewmodels

type DashboardViewData struct {
	Layout            LayoutData
	IdentityCount     int64
	DiscoveryAppCount int64
	AppAssetCount     int64
	RelationshipGraph OverviewMapGraph
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
