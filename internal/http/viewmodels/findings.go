package viewmodels

type FindingsRulesetsViewData struct {
	Layout      LayoutData
	Rulesets    []FindingsRulesetItem
	HasRulesets bool
}

type FindingsRulesetItem struct {
	Key           string
	Name          string
	Description   string
	ScopeKind     string
	ConnectorKind string
	Status        string
	Source        string
	SourceVersion string
	Href          string
}

type FindingsRuleItem struct {
	Key              string
	Severity         string
	Title            string
	Summary          string
	MonitoringStatus string
	Status           string
	EvaluatedAt      string
	EvidenceSummary  string
	ErrorKind        string
	Href             string
}

type FindingsReferenceItem struct {
	Title string
	URL   string
	Type  string
}

type FindingsFrameworkMappingItem struct {
	Framework        string
	FrameworkVersion string
	Control          string
	Coverage         string
}

type FindingsAlert struct {
	Title       string
	Message     string
	Destructive bool
}
