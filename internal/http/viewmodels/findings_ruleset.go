package viewmodels

import "github.com/open-sspm/open-sspm/internal/http/querystate"

type FindingsRulesetViewData struct {
	Layout LayoutData

	Ruleset FindingsRulesetItem

	SourceName        string
	ConnectorHintHref string

	OverrideExists  bool
	OverrideEnabled bool

	Query querystate.FindingsRulesQuery

	Rules    []FindingsRuleItem
	HasRules bool

	Alert *AlertViewData
}
