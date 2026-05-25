package findings

import "time"

type Status string

const (
	StatusOpen         Status = "open"
	StatusResolved     Status = "resolved"
	StatusSuppressed   Status = "suppressed"
	StatusAcceptedRisk Status = "accepted_risk"
)

type SeveritySource string

const (
	SeveritySourcePolicy   SeveritySource = "policy"
	SeveritySourceOverride SeveritySource = "override"
	SeveritySourceManual   SeveritySource = "manual"
)

type SourceRef struct {
	Kind string
	Name string
}

type ScopeRef struct {
	Kind       string
	SourceKind string
	SourceName string
}

type EntityRef struct {
	Kind string
	ID   string
	Name string
}

type ResourceRef struct {
	Kind string
	ID   string
	Name string
}

type PolicyRef struct {
	BundleID      string
	BundleVersion string
	ID            string
	Title         string
	RuleID        string
	RulesetID     string
}

type EventRef struct {
	ReceivedAt time.Time
	ID         [16]byte
	Valid      bool
}

type FindingResult struct {
	Key               string
	Status            Status
	BaseSeverity      string
	EffectiveSeverity string
	SeveritySource    SeveritySource
	Title             string
	Summary           string
	Evidence          string
	Remediation       string

	Source   SourceRef
	Scope    ScopeRef
	Entity   EntityRef
	Resource ResourceRef
	Policy   PolicyRef

	EventRef    *EventRef
	Output      map[string]any
	EvaluatedAt time.Time
	SyncRunID   *int64
}
