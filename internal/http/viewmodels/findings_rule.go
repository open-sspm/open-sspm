package viewmodels

type FindingsRuleViewData struct {
	Layout LayoutData

	Ruleset FindingsRulesetItem

	SourceName string

	RuleKey      string
	RuleTitle    string
	RuleSummary  string
	RuleSeverity string

	MonitoringStatus string
	MonitoringReason string

	CurrentStatus      string
	CurrentEvaluatedAt TimeDisplay
	CurrentErrorKind   string
	EvidenceSummary    string
	Evidence           FindingsEvidenceViewData

	RulesetOverrideEnabled bool

	RuleOverride FindingsRuleOverrideViewData
	Attestation  FindingsRuleAttestationViewData

	Alert *AlertViewData
}

type FindingsEvidenceViewData struct {
	IsEnvelopeV1 bool

	CheckType string
	Dataset   string

	ParamsPretty string

	SelectionTotal    int
	SelectionSelected int

	Violations          []FindingsEvidenceViolation
	ViolationsTruncated bool

	RawPretty string
}

type FindingsEvidenceViolation struct {
	ResourceID string
	Display    string
}

type FindingsRuleOverrideViewData struct {
	Enabled bool

	HasParameters bool
	Fields        []FindingsParamField

	CurrentParamsPretty string
}

type FindingsParamField struct {
	Key  string
	Type string

	DefaultValue  string
	OverrideValue string
}

type FindingsRuleAttestationViewData struct {
	Status         string
	Notes          string
	ExpiresAtInput string
	ExpiresAt      TimeDisplay
}
