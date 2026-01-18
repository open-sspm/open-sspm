package viewmodels

type FindingsRulesetViewData struct {
	Layout LayoutData

	Ruleset FindingsRulesetItem

	SourceName        string
	ConnectorHintHref string

	Tags              []string
	References        []FindingsReferenceItem
	FrameworkMappings []FindingsFrameworkMappingItem
	HasMetadata       bool

	OverrideExists  bool
	OverrideEnabled bool

	StatusFilter     string
	SeverityFilter   string
	MonitoringFilter string

	Rules    []FindingsRuleItem
	HasRules bool

	Alert *FindingsAlert
}
