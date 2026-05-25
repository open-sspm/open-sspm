package riskpolicy

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	EventPolicyPackID      = "opensspm.event_shadow.v1"
	EventPolicyPackVersion = "0.1.0-shadow"
)

type EventInput struct {
	SourceKind       string
	SourceName       string
	Channel          string
	ProviderEventID  string
	EventType        string
	Category         string
	Action           string
	Outcome          string
	Severity         int64
	ActorKind        string
	ActorID          string
	ActorEmail       string
	ActorDisplayName string
	TargetKind       string
	TargetID         string
	TargetName       string
	OccurredAt       time.Time
	EvaluatedAt      time.Time
}

type EventRule struct {
	ID       string
	Severity string
	Title    string
	Evidence string
	Match    func(EventInput) (bool, error)
}

type EventResult struct {
	PolicyPackID      string
	PolicyPackVersion string
	Signals           []RiskSignal
}

type EventEvaluator struct {
	rules []EventRule
}

func NewEventEvaluator(rules []EventRule) (*EventEvaluator, error) {
	if len(rules) == 0 {
		rules = BuiltinEventRules()
	}
	normalized := make([]EventRule, 0, len(rules))
	for _, rule := range rules {
		rule = normalizeEventRule(rule)
		if rule.ID == "" || rule.Match == nil {
			return nil, fmt.Errorf("event rule %q is missing id or matcher", rule.ID)
		}
		normalized = append(normalized, rule)
	}
	return &EventEvaluator{rules: normalized}, nil
}

func EvaluateEvent(input EventInput) (EventResult, error) {
	evaluator, err := NewEventEvaluator(nil)
	if err != nil {
		return EventResult{}, err
	}
	return evaluator.Evaluate(input)
}

func (e *EventEvaluator) Evaluate(input EventInput) (EventResult, error) {
	if e == nil {
		return EventResult{}, errors.New("event evaluator is nil")
	}
	input = normalizeEventInput(input)
	result := EventResult{
		PolicyPackID:      EventPolicyPackID,
		PolicyPackVersion: EventPolicyPackVersion,
		Signals:           make([]RiskSignal, 0, len(e.rules)),
	}
	for _, rule := range e.rules {
		matched, err := rule.Match(input)
		if err != nil {
			return EventResult{}, fmt.Errorf("%s: evaluate event rule: %w", rule.ID, err)
		}
		if !matched {
			continue
		}
		result.Signals = append(result.Signals, RiskSignal{
			ID:                rule.ID,
			Domain:            DomainSaaS,
			Severity:          rule.Severity,
			Title:             rule.Title,
			Evidence:          rule.Evidence,
			PolicyPackID:      EventPolicyPackID,
			PolicyPackVersion: EventPolicyPackVersion,
		})
	}
	return result, nil
}

func BuiltinEventRules() []EventRule {
	return []EventRule{
		{
			ID:       "event.oauth_grant",
			Severity: SeverityMedium,
			Title:    "OAuth grant observed",
			Evidence: "A provider event reported OAuth grant or consent activity.",
			Match: func(input EventInput) (bool, error) {
				return input.Category == "discovery.oauth_grant", nil
			},
		},
		{
			ID:       "event.state_refresh",
			Severity: SeverityLow,
			Title:    "State refresh event observed",
			Evidence: "A provider event indicated state may have changed and should be reconciled.",
			Match: func(input EventInput) (bool, error) {
				return strings.HasPrefix(input.Category, "state_refresh."), nil
			},
		},
		{
			ID:       "event.failure",
			Severity: SeverityLow,
			Title:    "Failed provider event observed",
			Evidence: "A provider event reported a failed action.",
			Match: func(input EventInput) (bool, error) {
				return input.Outcome == "failure", nil
			},
		},
	}
}

func normalizeEventInput(input EventInput) EventInput {
	input.SourceKind = strings.ToLower(strings.TrimSpace(input.SourceKind))
	input.SourceName = strings.TrimSpace(input.SourceName)
	input.Channel = strings.TrimSpace(input.Channel)
	input.ProviderEventID = strings.TrimSpace(input.ProviderEventID)
	input.EventType = strings.TrimSpace(input.EventType)
	input.Category = strings.TrimSpace(input.Category)
	input.Action = strings.TrimSpace(input.Action)
	input.Outcome = strings.ToLower(strings.TrimSpace(input.Outcome))
	input.ActorKind = strings.TrimSpace(input.ActorKind)
	input.ActorID = strings.TrimSpace(input.ActorID)
	input.ActorEmail = strings.ToLower(strings.TrimSpace(input.ActorEmail))
	input.ActorDisplayName = strings.TrimSpace(input.ActorDisplayName)
	input.TargetKind = strings.TrimSpace(input.TargetKind)
	input.TargetID = strings.TrimSpace(input.TargetID)
	input.TargetName = strings.TrimSpace(input.TargetName)
	if input.OccurredAt.IsZero() {
		input.OccurredAt = time.Now()
	}
	input.OccurredAt = input.OccurredAt.UTC()
	if input.EvaluatedAt.IsZero() {
		input.EvaluatedAt = time.Now()
	}
	input.EvaluatedAt = input.EvaluatedAt.UTC()
	return input
}

func normalizeEventRule(rule EventRule) EventRule {
	rule.ID = strings.TrimSpace(rule.ID)
	rule.Severity = strings.TrimSpace(rule.Severity)
	if rule.Severity == "" {
		rule.Severity = SeverityLow
	}
	rule.Title = strings.TrimSpace(rule.Title)
	rule.Evidence = strings.TrimSpace(rule.Evidence)
	return rule
}
