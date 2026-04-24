package riskpolicy

import (
	"errors"
	"fmt"
	"strings"
)

func validatePolicyPack(name string, pack PolicyPack) error {
	var errs []error
	if pack.APIVersion != APIVersion {
		errs = append(errs, fmt.Errorf("api_version must be %q", APIVersion))
	}
	if pack.Kind != Kind {
		errs = append(errs, fmt.Errorf("kind must be %q", Kind))
	}
	if pack.Metadata.ID == "" {
		errs = append(errs, errors.New("metadata.id is required"))
	}
	if pack.Metadata.Version == "" {
		errs = append(errs, errors.New("metadata.version is required"))
	}
	if !validDomain(pack.Metadata.Domain) {
		errs = append(errs, fmt.Errorf("metadata.domain %q is not supported", pack.Metadata.Domain))
	}
	if pack.Spec.Inputs.Schema == "" && len(pack.Spec.ScopedRules) == 0 {
		errs = append(errs, errors.New("spec.inputs.schema is required"))
	}

	ruleIDs := make(map[string]string)
	validateSuggestions(&errs, "spec.suggestions.business_criticality", pack.Spec.Suggestions.BusinessCriticality, ruleIDs, validBusinessCriticality)
	validateSuggestions(&errs, "spec.suggestions.data_classification", pack.Spec.Suggestions.DataClassification, ruleIDs, validDataClassification)
	validateScoring(&errs, pack.Spec.Scoring, ruleIDs)
	validateLevelRules(&errs, "spec.levels", pack.Spec.Levels)
	validateRules(&errs, "spec.rules", pack.Spec.Rules, ruleIDs)
	validateAggregation(&errs, pack.Spec.Aggregation)
	validateScopedRules(&errs, pack.Spec.ScopedRules, ruleIDs)

	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("%s: validate policy: %w", name, err)
	}
	return nil
}

func validDomain(domain Domain) bool {
	switch domain {
	case DomainCredential, DomainSaaS, DomainIdentity:
		return true
	default:
		return false
	}
}

func validateSuggestions(errs *[]error, path string, rules []SuggestionRule, ruleIDs map[string]string, validLevel func(string) bool) {
	for i, rule := range rules {
		currentPath := fmt.Sprintf("%s[%d]", path, i)
		validateID(errs, currentPath+".id", rule.ID, ruleIDs)
		if rule.Level == "" {
			*errs = append(*errs, fmt.Errorf("%s.level is required", currentPath))
		} else if !validLevel(rule.Level) {
			*errs = append(*errs, fmt.Errorf("%s.level %q is invalid", currentPath, rule.Level))
		}
		if rule.When == "" {
			*errs = append(*errs, fmt.Errorf("%s.when is required", currentPath))
		}
	}
}

func validateScoring(errs *[]error, scoring Scoring, ruleIDs map[string]string) {
	if scoring.Max == 0 && len(scoring.Rules) == 0 {
		return
	}
	if scoring.Max <= 0 {
		*errs = append(*errs, errors.New("spec.scoring.max must be greater than zero"))
	}
	if scoring.Base < 0 {
		*errs = append(*errs, errors.New("spec.scoring.base must be non-negative"))
	}
	if scoring.Max > 0 && scoring.Base > scoring.Max {
		*errs = append(*errs, errors.New("spec.scoring.base must not exceed spec.scoring.max"))
	}
	for i, rule := range scoring.Rules {
		path := fmt.Sprintf("spec.scoring.rules[%d]", i)
		validateID(errs, path+".id", rule.ID, ruleIDs)
		if rule.Points == 0 {
			*errs = append(*errs, fmt.Errorf("%s.points must be non-zero", path))
		}
		if rule.When == "" {
			*errs = append(*errs, fmt.Errorf("%s.when is required", path))
		}
		if rule.Signal.Severity != "" && !ValidSeverity(rule.Signal.Severity) {
			*errs = append(*errs, fmt.Errorf("%s.signal.severity %q is invalid", path, rule.Signal.Severity))
		}
		if rule.Signal.Severity != "" && rule.Signal.Title == "" {
			*errs = append(*errs, fmt.Errorf("%s.signal.title is required when signal.severity is set", path))
		}
	}
}

func validateLevelRules(errs *[]error, path string, rules []LevelRule) {
	for i, rule := range rules {
		currentPath := fmt.Sprintf("%s[%d]", path, i)
		if !ValidSeverity(rule.Level) {
			*errs = append(*errs, fmt.Errorf("%s.level %q is invalid", currentPath, rule.Level))
		}
		if rule.When == "" {
			*errs = append(*errs, fmt.Errorf("%s.when is required", currentPath))
		}
	}
	if len(rules) > 0 && strings.TrimSpace(rules[len(rules)-1].When) != "true" {
		*errs = append(*errs, fmt.Errorf("%s must end with a deterministic fallback where when is true", path))
	}
}

func validateRules(errs *[]error, path string, rules []Rule, ruleIDs map[string]string) {
	for i, rule := range rules {
		currentPath := fmt.Sprintf("%s[%d]", path, i)
		validateID(errs, currentPath+".id", rule.ID, ruleIDs)
		if !ValidSeverity(rule.Severity) {
			*errs = append(*errs, fmt.Errorf("%s.severity %q is invalid", currentPath, rule.Severity))
		}
		if rule.When == "" {
			*errs = append(*errs, fmt.Errorf("%s.when is required", currentPath))
		}
		if rule.Title == "" {
			*errs = append(*errs, fmt.Errorf("%s.title is required", currentPath))
		}
	}
}

func validateAggregation(errs *[]error, aggregation Aggregation) {
	if aggregation.RiskLevel.Strategy != "" {
		if aggregation.RiskLevel.Strategy != "max_severity" {
			*errs = append(*errs, fmt.Errorf("spec.aggregation.risk_level.strategy %q is not supported", aggregation.RiskLevel.Strategy))
		}
		if aggregation.RiskLevel.Default != "" && !ValidSeverity(aggregation.RiskLevel.Default) {
			*errs = append(*errs, fmt.Errorf("spec.aggregation.risk_level.default %q is invalid", aggregation.RiskLevel.Default))
		}
	}
	if aggregation.RiskReasonCount.Strategy != "" && aggregation.RiskReasonCount.Strategy != "count_matching_rules" {
		*errs = append(*errs, fmt.Errorf("spec.aggregation.risk_reason_count.strategy %q is not supported", aggregation.RiskReasonCount.Strategy))
	}
}

func validateScopedRules(errs *[]error, scopedRules []ScopedRule, ruleIDs map[string]string) {
	for i, scopedRule := range scopedRules {
		path := fmt.Sprintf("spec.scoped_rules[%d]", i)
		validateID(errs, path+".id", scopedRule.ID, ruleIDs)
		if !scopedRule.Scope.App.hasSelector() {
			*errs = append(*errs, fmt.Errorf("%s.scope.app must include at least one selector", path))
		}
		validateScopedSuggestions(errs, path+".suggestions", scopedRule.Suggestions)
		validateRules(errs, path+".rules", scopedRule.Rules, ruleIDs)
	}
}

func validateScopedSuggestions(errs *[]error, path string, suggestions ScopedSuggestions) {
	if suggestions.BusinessCriticality != "" && !validBusinessCriticality(suggestions.BusinessCriticality) {
		*errs = append(*errs, fmt.Errorf("%s.business_criticality %q is invalid", path, suggestions.BusinessCriticality))
	}
	if suggestions.DataClassification != "" && !validDataClassification(suggestions.DataClassification) {
		*errs = append(*errs, fmt.Errorf("%s.data_classification %q is invalid", path, suggestions.DataClassification))
	}
}

func validBusinessCriticality(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "unknown", "low", "medium", "high", "critical":
		return true
	default:
		return false
	}
}

func validDataClassification(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "unknown", "public", "internal", "confidential", "restricted":
		return true
	default:
		return false
	}
}

func (scope AppScope) hasSelector() bool {
	return scope.CanonicalKey != "" ||
		scope.PrimaryDomain != "" ||
		len(scope.DomainMatches) > 0 ||
		scope.VendorName != "" ||
		scope.SourceKind != "" ||
		scope.SourceName != "" ||
		scope.Category != ""
}

func validateID(errs *[]error, path, id string, seen map[string]string) {
	if id == "" {
		*errs = append(*errs, fmt.Errorf("%s is required", path))
		return
	}
	if previousPath := seen[id]; previousPath != "" {
		*errs = append(*errs, fmt.Errorf("%s duplicates id %q from %s", path, id, previousPath))
		return
	}
	seen[id] = path
}
