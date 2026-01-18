package viewmodels

type FindingsRuleViewData struct {
	Layout LayoutData

	Ruleset FindingsRulesetItem

	SourceName string

	RuleKey         string
	RuleTitle       string
	RuleSummary     string
	RuleSeverity    string

	MonitoringStatus string
	MonitoringReason string

	RemediationInstructions string
	RemediationRisks        string
	RemediationEffort       string

	CurrentStatus      string
	CurrentEvaluatedAt string
	CurrentErrorKind   string
	EvidenceSummary    string
	Evidence           FindingsEvidenceViewData

	RulesetOverrideEnabled bool

	RuleOverride FindingsRuleOverrideViewData
	Attestation  FindingsRuleAttestationViewData

	Alert *FindingsAlert
}

type FindingsEvidenceViewData struct {
	IsEnvelopeV1 bool

	CheckType string
	Dataset   string
	Left      FindingsJoinSideViewData
	Right     FindingsJoinSideViewData

	ParamsPretty string

	SelectionTotal    int
	SelectionSelected int

	Violations          []FindingsEvidenceViolation
	ViolationsTruncated bool

	RawPretty string
}

type FindingsJoinSideViewData struct {
	Dataset string
}

type FindingsEvidenceViolation struct {
	ResourceID string
	Display    string
}

type FindingsRuleOverrideViewData struct {
	Enabled bool

	HasSchema bool
	Fields    []FindingsParamField

	CurrentParamsPretty string
}

type FindingsParamField struct {
	Key         string
	Type        string
	Description string

	DefaultValue  string
	OverrideValue string
}

type FindingsRuleAttestationViewData struct {
	Status    string
	Notes     string
	ExpiresAt string
}
