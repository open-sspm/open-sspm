package viewmodels

type DashboardViewData struct {
	Layout                     LayoutData
	IdentityCount              int64
	DiscoveryAppCount          int64
	AppAssetCount              int64
	CredentialsAttention       DashboardCredentialsAttention
	UnreviewedDiscoveryApps    int64
	SuspendedHumanIdentities   int64
	ConnectorsNeedingAttention int64
	FindingSeverity            DashboardFindingSeverity
	FrameworkPosture           []DashboardFrameworkPostureItem
}

type DashboardCredentialsAttention struct {
	Total    int64
	Critical int64
	High     int64
}

type DashboardFindingSeverity struct {
	Evaluated     int64
	Open          int64
	Critical      int64
	High          int64
	Medium        int64
	Low           int64
	Informational int64
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
