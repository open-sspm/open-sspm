package riskpolicy

const (
	APIVersion = "risk.open-sspm.io/v1"
	Kind       = "RiskPolicyPack"
)

type Domain string

const (
	DomainCredential Domain = "credential"
	DomainSaaS       Domain = "saas"
	DomainIdentity   Domain = "identity"
)

type PolicyPack struct {
	APIVersion string         `yaml:"api_version"`
	Kind       string         `yaml:"kind"`
	Metadata   PolicyMetadata `yaml:"metadata"`
	Spec       PolicySpec     `yaml:"spec"`
}

type PolicyMetadata struct {
	ID      string `yaml:"id"`
	Version string `yaml:"version"`
	Domain  Domain `yaml:"domain"`
}

type PolicySpec struct {
	Inputs      Inputs              `yaml:"inputs"`
	Constants   map[string][]string `yaml:"constants"`
	Suggestions Suggestions         `yaml:"suggestions"`
	Scoring     Scoring             `yaml:"scoring"`
	Levels      []LevelRule         `yaml:"levels"`
	Rules       []Rule              `yaml:"rules"`
	Aggregation Aggregation         `yaml:"aggregation"`
	ScopedRules []ScopedRule        `yaml:"scoped_rules"`
}

type Inputs struct {
	Schema string `yaml:"schema"`
}

type Suggestions struct {
	BusinessCriticality []SuggestionRule `yaml:"business_criticality"`
	DataClassification  []SuggestionRule `yaml:"data_classification"`
}

type SuggestionRule struct {
	ID    string `yaml:"id"`
	Level string `yaml:"level"`
	When  string `yaml:"when"`
}

type Scoring struct {
	Base  int           `yaml:"base"`
	Max   int           `yaml:"max"`
	Rules []ScoringRule `yaml:"rules"`
}

type ScoringRule struct {
	ID     string `yaml:"id"`
	Points int    `yaml:"points"`
	When   string `yaml:"when"`
	Signal Signal `yaml:"signal"`
}

type Signal struct {
	Severity string `yaml:"severity"`
	Title    string `yaml:"title"`
	Evidence string `yaml:"evidence"`
}

type LevelRule struct {
	Level string `yaml:"level"`
	When  string `yaml:"when"`
}

type Rule struct {
	ID         string `yaml:"id"`
	Severity   string `yaml:"severity"`
	ScoreDelta int    `yaml:"score_delta"`
	When       string `yaml:"when"`
	Title      string `yaml:"title"`
	Evidence   string `yaml:"evidence"`
}

type Aggregation struct {
	RiskLevel       AggregationStrategy `yaml:"risk_level"`
	RiskReasonCount AggregationStrategy `yaml:"risk_reason_count"`
}

type AggregationStrategy struct {
	Strategy string `yaml:"strategy"`
	Default  string `yaml:"default"`
}

type ScopedRule struct {
	ID          string            `yaml:"id"`
	Scope       Scope             `yaml:"scope"`
	Rules       []Rule            `yaml:"rules"`
	Suggestions ScopedSuggestions `yaml:"suggestions"`
}

type Scope struct {
	App AppScope `yaml:"app"`
}

type AppScope struct {
	CanonicalKey  string   `yaml:"canonical_key"`
	PrimaryDomain string   `yaml:"primary_domain"`
	DomainMatches []string `yaml:"domain_matches"`
	VendorName    string   `yaml:"vendor_name"`
	SourceKind    string   `yaml:"source_kind"`
	SourceName    string   `yaml:"source_name"`
	Category      string   `yaml:"category"`
}

type ScopedSuggestions struct {
	BusinessCriticality string `yaml:"business_criticality"`
	DataClassification  string `yaml:"data_classification"`
}
