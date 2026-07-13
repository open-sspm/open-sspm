package viewmodels

import "github.com/open-sspm/open-sspm/internal/http/querystate"

type FindingsRulesetViewData struct {
	Layout LayoutData

	Ruleset FindingsRulesetItem

	SourceName        string
	ConnectorHintHref string

	OverrideExists  bool
	OverrideEnabled bool

	StatusFilter     string
	SeverityFilter   string
	MonitoringFilter string
	Query            querystate.FindingsRulesQuery

	Rules    []FindingsRuleItem
	HasRules bool

	Alert *AlertViewData
}
