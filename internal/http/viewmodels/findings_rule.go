package viewmodels

type FindingsRuleViewData struct {
	Layout LayoutData

	Ruleset FindingsRulesetItem

	SourceName string

	RuleKey         string
	RuleTitle       string
	RuleSummary     string
	RuleDescription string
	RuleCategory    string
	RuleSeverity    string

	MonitoringStatus string
	MonitoringReason string

	Tags              []string
	References        []FindingsReferenceItem
	FrameworkMappings []FindingsFrameworkMappingItem

	RemediationInstructions string
	RemediationRisks        string
	RemediationEffort       string

	RequiredData []string

	CurrentStatus     string
	CurrentEvaluatedAt string
	CurrentErrorKind  string
	EvidenceSummary   string
	Evidence          FindingsEvidenceViewData

	RulesetOverrideExists  bool
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

	ResultStatus   string
	ResultErrorKind string

	SelectionTotal    int
	SelectionSelected int

	JoinUnmatchedLeft   int
	JoinOnUnmatchedLeft string

	Violations           []FindingsEvidenceViolation
	ViolationsTruncated  bool

	RawPretty string
}

type FindingsJoinSideViewData struct {
	Dataset string
	KeyPath string
}

type FindingsEvidenceViolation struct {
	ResourceID string
	Display    string
}

type FindingsRuleOverrideViewData struct {
	Exists  bool
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
	Exists    bool
	Status    string
	Notes     string
	ExpiresAt string
}

