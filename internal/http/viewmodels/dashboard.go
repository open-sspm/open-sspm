package viewmodels

type DashboardViewData struct {
	Layout            LayoutData
	IdentityCount     int64
	DiscoveryAppCount int64
	AppAssetCount     int64
	RelationshipGraph DashboardRelationshipGraph
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

type DashboardRelationshipGraph struct {
	CenterX       int
	CenterY       int
	IdentityCount int64
	AccountCount  int64
	Sources       []DashboardGraphSourceNode
}

type DashboardGraphSourceNode struct {
	Kind                  string
	SourceName            string
	Label                 string
	Href                  string
	X                     int
	Y                     int
	IdentityCount         int64
	AccountCount          int64
	ManagedAccountCount   int64
	UnmanagedAccountCount int64
	CoveragePercent       int
	Tone                  string
	Buckets               []DashboardGraphBucket
}

type DashboardGraphBucket struct {
	Label            string
	Severity         string
	AffectedCount    int64
	EntitlementCount int64
}
