package riskpolicy

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/cel-go/cel"
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
	When     string
	Severity string
	Title    string
	Evidence string
}

type EventResult struct {
	PolicyPackID      string
	PolicyPackVersion string
	Signals           []RiskSignal
}

type EventEvaluator struct {
	rules []compiledEventRule
}

type compiledEventRule struct {
	rule    EventRule
	program cel.Program
}

func NewEventEvaluator(rules []EventRule) (*EventEvaluator, error) {
	if len(rules) == 0 {
		rules = BuiltinEventRules()
	}
	env, err := newEventEnv()
	if err != nil {
		return nil, err
	}
	compiled := make([]compiledEventRule, 0, len(rules))
	for _, rule := range rules {
		rule = normalizeEventRule(rule)
		if rule.ID == "" || rule.When == "" {
			return nil, fmt.Errorf("event rule %q is missing id or expression", rule.ID)
		}
		ast, issues := env.Compile(rule.When)
		if issues != nil && issues.Err() != nil {
			return nil, fmt.Errorf("%s: compile %q: %w", rule.ID, rule.When, issues.Err())
		}
		if !ast.OutputType().IsExactType(cel.BoolType) {
			return nil, fmt.Errorf("%s: expression must return bool, got %s", rule.ID, ast.OutputType())
		}
		program, err := env.Program(ast, cel.EvalOptions(cel.OptOptimize))
		if err != nil {
			return nil, fmt.Errorf("%s: create CEL program: %w", rule.ID, err)
		}
		compiled = append(compiled, compiledEventRule{rule: rule, program: program})
	}
	return &EventEvaluator{rules: compiled}, nil
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
	activation := eventActivation(input)
	result := EventResult{
		PolicyPackID:      EventPolicyPackID,
		PolicyPackVersion: EventPolicyPackVersion,
		Signals:           make([]RiskSignal, 0, len(e.rules)),
	}
	for _, compiled := range e.rules {
		value, _, err := compiled.program.Eval(activation)
		if err != nil {
			return EventResult{}, fmt.Errorf("%s: evaluate %q: %w", compiled.rule.ID, compiled.rule.When, err)
		}
		matched, ok := value.Value().(bool)
		if !ok {
			return EventResult{}, fmt.Errorf("%s: expected bool result, got %T", compiled.rule.ID, value.Value())
		}
		if !matched {
			continue
		}
		result.Signals = append(result.Signals, RiskSignal{
			ID:                compiled.rule.ID,
			Domain:            DomainSaaS,
			Severity:          compiled.rule.Severity,
			Title:             compiled.rule.Title,
			Evidence:          compiled.rule.Evidence,
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
			When:     `category == "discovery.oauth_grant"`,
			Severity: SeverityMedium,
			Title:    "OAuth grant observed",
			Evidence: "A provider event reported OAuth grant or consent activity.",
		},
		{
			ID:       "event.state_refresh",
			When:     `category.startsWith("state_refresh.")`,
			Severity: SeverityLow,
			Title:    "State refresh event observed",
			Evidence: "A provider event indicated state may have changed and should be reconciled.",
		},
		{
			ID:       "event.failure",
			When:     `outcome == "failure"`,
			Severity: SeverityLow,
			Title:    "Failed provider event observed",
			Evidence: "A provider event reported a failed action.",
		},
	}
}

func newEventEnv() (*cel.Env, error) {
	return cel.NewEnv(
		cel.Variable("source_kind", cel.StringType),
		cel.Variable("source_name", cel.StringType),
		cel.Variable("channel", cel.StringType),
		cel.Variable("provider_event_id", cel.StringType),
		cel.Variable("event_type", cel.StringType),
		cel.Variable("category", cel.StringType),
		cel.Variable("action", cel.StringType),
		cel.Variable("outcome", cel.StringType),
		cel.Variable("severity", cel.IntType),
		cel.Variable("actor_kind", cel.StringType),
		cel.Variable("actor_id", cel.StringType),
		cel.Variable("actor_email", cel.StringType),
		cel.Variable("actor_display_name", cel.StringType),
		cel.Variable("target_kind", cel.StringType),
		cel.Variable("target_id", cel.StringType),
		cel.Variable("target_name", cel.StringType),
		cel.Variable("occurred_at", cel.TimestampType),
		cel.Variable("evaluated_at", cel.TimestampType),
	)
}

func eventActivation(input EventInput) map[string]any {
	return map[string]any{
		"source_kind":        input.SourceKind,
		"source_name":        input.SourceName,
		"channel":            input.Channel,
		"provider_event_id":  input.ProviderEventID,
		"event_type":         input.EventType,
		"category":           input.Category,
		"action":             input.Action,
		"outcome":            input.Outcome,
		"severity":           input.Severity,
		"actor_kind":         input.ActorKind,
		"actor_id":           input.ActorID,
		"actor_email":        input.ActorEmail,
		"actor_display_name": input.ActorDisplayName,
		"target_kind":        input.TargetKind,
		"target_id":          input.TargetID,
		"target_name":        input.TargetName,
		"occurred_at":        input.OccurredAt,
		"evaluated_at":       input.EvaluatedAt,
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
	rule.When = strings.TrimSpace(rule.When)
	rule.Severity = strings.TrimSpace(rule.Severity)
	if rule.Severity == "" {
		rule.Severity = SeverityLow
	}
	rule.Title = strings.TrimSpace(rule.Title)
	rule.Evidence = strings.TrimSpace(rule.Evidence)
	return rule
}
