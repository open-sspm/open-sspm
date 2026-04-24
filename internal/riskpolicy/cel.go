package riskpolicy

import (
	"fmt"

	"github.com/google/cel-go/cel"
)

type CompiledExpression struct {
	RuleID     string
	Expression string
	ast        *cel.Ast
}

func compilePack(name string, pack PolicyPack) (CompiledPack, error) {
	if err := validatePolicyPack(name, pack); err != nil {
		return CompiledPack{}, err
	}
	env, err := newEnv(pack)
	if err != nil {
		return CompiledPack{}, fmt.Errorf("%s: create CEL env: %w", name, err)
	}

	compiled := CompiledPack{
		Policy:      pack,
		expressions: make([]CompiledExpression, 0, expressionCount(pack)),
	}
	compile := func(ruleID, expression string) error {
		ast, issues := env.Compile(expression)
		if issues != nil && issues.Err() != nil {
			return fmt.Errorf("%s: %s: compile %q: %w", name, ruleID, expression, issues.Err())
		}
		if !ast.OutputType().IsExactType(cel.BoolType) {
			return fmt.Errorf("%s: %s: compile %q: expression must return bool, got %s", name, ruleID, expression, ast.OutputType())
		}
		compiled.expressions = append(compiled.expressions, CompiledExpression{
			RuleID:     ruleID,
			Expression: expression,
			ast:        ast,
		})
		return nil
	}

	for _, rule := range pack.Spec.Suggestions.BusinessCriticality {
		if err := compile(rule.ID, rule.When); err != nil {
			return CompiledPack{}, err
		}
	}
	for _, rule := range pack.Spec.Suggestions.DataClassification {
		if err := compile(rule.ID, rule.When); err != nil {
			return CompiledPack{}, err
		}
	}
	for _, rule := range pack.Spec.Scoring.Rules {
		if err := compile(rule.ID, rule.When); err != nil {
			return CompiledPack{}, err
		}
	}
	for _, rule := range pack.Spec.Levels {
		if err := compile("level:"+rule.Level, rule.When); err != nil {
			return CompiledPack{}, err
		}
	}
	for _, rule := range pack.Spec.Rules {
		if err := compile(rule.ID, rule.When); err != nil {
			return CompiledPack{}, err
		}
	}
	for _, scopedRule := range pack.Spec.ScopedRules {
		for _, rule := range scopedRule.Rules {
			if err := compile(scopedRule.ID+"/"+rule.ID, rule.When); err != nil {
				return CompiledPack{}, err
			}
		}
	}
	return compiled, nil
}

func newEnv(pack PolicyPack) (*cel.Env, error) {
	opts := domainEnvOptions(pack.Metadata.Domain)
	for name := range pack.Spec.Constants {
		opts = append(opts, cel.Variable(name, cel.ListType(cel.StringType)))
	}
	return cel.NewEnv(opts...)
}

func domainEnvOptions(domain Domain) []cel.EnvOption {
	switch domain {
	case DomainCredential:
		return []cel.EnvOption{
			cel.Variable("source_kind", cel.StringType),
			cel.Variable("source_name", cel.StringType),
			cel.Variable("credential_kind", cel.StringType),
			cel.Variable("status", cel.StringType),
			cel.Variable("expires_at", cel.NullableType(cel.TimestampType)),
			cel.Variable("last_used_at", cel.NullableType(cel.TimestampType)),
			cel.Variable("created_at", cel.NullableType(cel.TimestampType)),
			cel.Variable("created_by_external_id", cel.StringType),
			cel.Variable("created_by_display_name", cel.StringType),
			cel.Variable("approved_by_external_id", cel.StringType),
			cel.Variable("approved_by_display_name", cel.StringType),
			cel.Variable("asset_ref_kind", cel.StringType),
			cel.Variable("asset_ref_external_id", cel.StringType),
			cel.Variable("scope_json", cel.DynType),
			cel.Variable("evaluated_at", cel.TimestampType),
		}
	case DomainSaaS:
		return []cel.EnvOption{
			cel.Variable("canonical_key", cel.StringType),
			cel.Variable("display_name", cel.StringType),
			cel.Variable("primary_domain", cel.StringType),
			cel.Variable("vendor_name", cel.StringType),
			cel.Variable("source_kind", cel.StringType),
			cel.Variable("source_name", cel.StringType),
			cel.Variable("actors_30d", cel.IntType),
			cel.Variable("has_privileged_scope", cel.BoolType),
			cel.Variable("has_confidential_scope", cel.BoolType),
			cel.Variable("managed_state", cel.StringType),
			cel.Variable("managed_reason", cel.StringType),
			cel.Variable("owner_identity_id", cel.IntType),
			cel.Variable("governance_state", cel.StringType),
			cel.Variable("review_disposition", cel.StringType),
			cel.Variable("follow_up_due_date", cel.NullableType(cel.TimestampType)),
			cel.Variable("effective_business_criticality", cel.StringType),
			cel.Variable("effective_data_classification", cel.StringType),
			cel.Variable("connector_binding_configured", cel.BoolType),
			cel.Variable("connector_binding_enabled", cel.BoolType),
			cel.Variable("connector_binding_stale", cel.BoolType),
			cel.Variable("connector_binding_healthy", cel.BoolType),
			cel.Variable("score", cel.IntType),
		}
	case DomainIdentity:
		return []cel.EnvOption{
			cel.Variable("identity_id", cel.IntType),
			cel.Variable("principal_ref", cel.StringType),
			cel.Variable("principal_type", cel.StringType),
			cel.Variable("source_kind", cel.StringType),
			cel.Variable("source_name", cel.StringType),
			cel.Variable("display_name", cel.StringType),
			cel.Variable("primary_email", cel.StringType),
			cel.Variable("last_seen_at", cel.NullableType(cel.TimestampType)),
			cel.Variable("owner_presence", cel.StringType),
			cel.Variable("governance_state", cel.StringType),
			cel.Variable("linked_assets_count", cel.IntType),
			cel.Variable("linked_credentials_count", cel.IntType),
			cel.Variable("credential_signals", cel.ListType(cel.StringType)),
			cel.Variable("has_critical_credential", cel.BoolType),
			cel.Variable("has_high_risk_credential", cel.BoolType),
			cel.Variable("has_expired_credential", cel.BoolType),
			cel.Variable("has_expiring_credential", cel.BoolType),
			cel.Variable("has_unused_credential", cel.BoolType),
			cel.Variable("has_stale_evidence", cel.BoolType),
		}
	default:
		return nil
	}
}

func expressionCount(pack PolicyPack) int {
	count := len(pack.Spec.Suggestions.BusinessCriticality) +
		len(pack.Spec.Suggestions.DataClassification) +
		len(pack.Spec.Scoring.Rules) +
		len(pack.Spec.Levels) +
		len(pack.Spec.Rules)
	for _, scopedRule := range pack.Spec.ScopedRules {
		count += len(scopedRule.Rules)
	}
	return count
}
