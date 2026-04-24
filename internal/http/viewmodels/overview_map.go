package viewmodels

const (
	OverviewMapToneDefault = "default"
	OverviewMapToneOkta    = "okta"
	OverviewMapToneEntra   = "entra"
	OverviewMapToneGoogle  = "google"
	OverviewMapToneGitHub  = "github"
	OverviewMapToneDatadog = "datadog"
	OverviewMapToneAWS     = "aws"
	OverviewMapToneVault   = "vault"
)

const (
	OverviewMapSeverityCritical = "critical"
	OverviewMapSeverityHigh     = "high"
	OverviewMapSeverityMedium   = "medium"
	OverviewMapSeverityLow      = "low"
	OverviewMapSeverityInfo     = "info"
)

type OverviewMapGraph struct {
	CenterX       int
	CenterY       int
	IdentityCount int64
	AccountCount  int64
	Sources       []OverviewMapSourceNode
}

type OverviewMapSourceNode struct {
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
	Buckets               []OverviewMapBucket
}

type OverviewMapBucket struct {
	Label            string
	Severity         string
	AffectedCount    int64
	EntitlementCount int64
}

func ClampOverviewMapPercent(value int) int {
	if value < 0 {
		return 0
	}
	if value > 100 {
		return 100
	}
	return value
}
