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
	EvaluatedAt      TimeDisplay
	EvidenceSummary  string
	ErrorKind        string
	Href             string
}
