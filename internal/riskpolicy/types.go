package riskpolicy

import osspecv2 "github.com/open-sspm/open-sspm-spec/gen/go/opensspm/spec/v2"

const Kind = "opensspm.entity_policy_pack"

type Domain = osspecv2.EntityPolicyDomain

const (
	DomainCredential Domain = osspecv2.EntityPolicyDomain_CREDENTIAL
	DomainSaaS       Domain = osspecv2.EntityPolicyDomain_SAAS
	DomainIdentity   Domain = osspecv2.EntityPolicyDomain_IDENTITY
)

type PolicyPack = osspecv2.EntityPolicyPack
type PolicyMetadata = osspecv2.EntityPolicyMetadata
type PolicySpec = osspecv2.EntityPolicySpec
type Inputs = osspecv2.EntityPolicyInputs
type Suggestions = osspecv2.EntityPolicySuggestions
type SuggestionRule = osspecv2.EntityPolicySuggestionRule
type Scoring = osspecv2.EntityPolicyScoring
type ScoringRule = osspecv2.EntityPolicyScoringRule
type Signal = osspecv2.EntityPolicySignal
type LevelRule = osspecv2.EntityPolicyLevelRule
type Rule = osspecv2.EntityPolicyRule
type Aggregation = osspecv2.EntityPolicyAggregation
type AggregationStrategy = osspecv2.EntityPolicyAggregationStrategy
type ScopedRule = osspecv2.EntityPolicyScopedRule
type Scope = osspecv2.EntityPolicyScope
type AppScope = osspecv2.EntityPolicyAppScope
type ScopedSuggestions = osspecv2.EntityPolicyScopedSuggestions
