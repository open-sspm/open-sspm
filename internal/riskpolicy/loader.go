package riskpolicy

import (
	"bytes"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	osspecv2 "github.com/open-sspm/open-sspm-spec/gen/go/opensspm/spec/v2"
	"gopkg.in/yaml.v3"
)

var (
	builtinRegistryOnce sync.Once
	builtinRegistry     *Registry
	builtinRegistryErr  error
)

type Registry struct {
	packs []CompiledPack
}

type CompiledPack struct {
	Policy      PolicyPack
	expressions []CompiledExpression
}

func LoadBuiltin() (*Registry, error) {
	return LoadPolicyPacks(specPolicyPacks())
}

func BuiltinRegistry() (*Registry, error) {
	builtinRegistryOnce.Do(func() {
		builtinRegistry, builtinRegistryErr = LoadBuiltin()
	})
	return builtinRegistry, builtinRegistryErr
}

func LoadDocuments(docs map[string][]byte) (*Registry, error) {
	names := make([]string, 0, len(docs))
	for name := range docs {
		names = append(names, name)
	}
	sort.Strings(names)

	registry := &Registry{
		packs: make([]CompiledPack, 0, len(names)),
	}
	seenPackIDs := make(map[string]string, len(names))
	for _, name := range names {
		pack, err := decodePolicyPack(name, docs[name])
		if err != nil {
			return nil, err
		}
		if previous := seenPackIDs[pack.Metadata.ID]; previous != "" {
			return nil, fmt.Errorf("%s: duplicate policy pack id %q also defined in %s", name, pack.Metadata.ID, previous)
		}
		seenPackIDs[pack.Metadata.ID] = name

		compiled, err := compilePack(name, pack)
		if err != nil {
			return nil, err
		}
		registry.packs = append(registry.packs, compiled)
	}
	return registry, nil
}

func LoadPolicyPacks(packs []PolicyPack) (*Registry, error) {
	registry := &Registry{
		packs: make([]CompiledPack, 0, len(packs)),
	}
	seenPackIDs := make(map[string]string, len(packs))
	for i, pack := range packs {
		name := fmt.Sprintf("entity_policy_packs[%d]", i)
		normalizePolicyPack(&pack)
		if previous := seenPackIDs[pack.Metadata.ID]; previous != "" {
			return nil, fmt.Errorf("%s: duplicate policy pack id %q also defined in %s", name, pack.Metadata.ID, previous)
		}
		seenPackIDs[pack.Metadata.ID] = name

		compiled, err := compilePack(name, pack)
		if err != nil {
			return nil, err
		}
		registry.packs = append(registry.packs, compiled)
	}
	return registry, nil
}

func (r *Registry) Packs() []PolicyPack {
	if r == nil {
		return nil
	}
	packs := make([]PolicyPack, 0, len(r.packs))
	for _, pack := range r.packs {
		packs = append(packs, clonePolicyPack(pack.Policy))
	}
	return packs
}

func (r *Registry) PackMetadatas() []PolicyMetadata {
	if r == nil {
		return nil
	}
	metadatas := make([]PolicyMetadata, 0, len(r.packs))
	for _, pack := range r.packs {
		metadatas = append(metadatas, pack.Policy.Metadata)
	}
	return metadatas
}

func (r *Registry) PackCount() int {
	if r == nil {
		return 0
	}
	return len(r.packs)
}

func (r *Registry) CompiledExpressionCount() int {
	if r == nil {
		return 0
	}
	var count int
	for _, pack := range r.packs {
		count += len(pack.expressions)
	}
	return count
}

func decodePolicyPack(name string, data []byte) (PolicyPack, error) {
	var doc osspecv2.EntityPolicyPackDoc
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&doc); err != nil {
		return PolicyPack{}, fmt.Errorf("%s: decode policy: %w", name, err)
	}
	var trailing any
	if err := dec.Decode(&trailing); err != nil && err != io.EOF {
		return PolicyPack{}, fmt.Errorf("%s: decode policy trailing document: %w", name, err)
	} else if err == nil {
		return PolicyPack{}, fmt.Errorf("%s: multiple YAML documents are not supported", name)
	}
	if doc.Kind != Kind {
		return PolicyPack{}, fmt.Errorf("%s: kind must be %q", name, Kind)
	}
	if doc.SchemaVersion != 2 {
		return PolicyPack{}, fmt.Errorf("%s: schema_version must be 2", name)
	}
	pack := doc.EntityPolicyPack
	normalizePolicyPack(&pack)
	return pack, nil
}

func specPolicyPacks() []PolicyPack {
	out := make([]PolicyPack, 0, len(osspecv2.GeneratedDescriptor.EntityPolicyPacks))
	for _, compiled := range osspecv2.GeneratedDescriptor.EntityPolicyPacks {
		out = append(out, compiled.Object.EntityPolicyPack)
	}
	return out
}

func clonePolicyPack(pack PolicyPack) PolicyPack {
	cloned := pack
	cloned.Spec.Constants = make(map[string][]string, len(pack.Spec.Constants))
	for key, values := range pack.Spec.Constants {
		cloned.Spec.Constants[key] = cloneSlice(values)
	}
	cloned.Spec.Suggestions.BusinessCriticality = cloneSlice(pack.Spec.Suggestions.BusinessCriticality)
	cloned.Spec.Suggestions.DataClassification = cloneSlice(pack.Spec.Suggestions.DataClassification)
	cloned.Spec.Scoring.Rules = cloneSlice(pack.Spec.Scoring.Rules)
	cloned.Spec.Levels = cloneSlice(pack.Spec.Levels)
	cloned.Spec.Rules = cloneSlice(pack.Spec.Rules)
	cloned.Spec.ScopedRules = cloneSlice(pack.Spec.ScopedRules)
	for i := range cloned.Spec.ScopedRules {
		cloned.Spec.ScopedRules[i].Scope.App.DomainMatches = cloneSlice(pack.Spec.ScopedRules[i].Scope.App.DomainMatches)
		cloned.Spec.ScopedRules[i].Rules = cloneSlice(pack.Spec.ScopedRules[i].Rules)
	}
	return cloned
}

func cloneSlice[T any](values []T) []T {
	if values == nil {
		return nil
	}
	return append([]T(nil), values...)
}

func normalizePolicyPack(pack *PolicyPack) {
	pack.Metadata.ID = strings.TrimSpace(pack.Metadata.ID)
	pack.Metadata.Version = strings.TrimSpace(pack.Metadata.Version)
	pack.Metadata.Domain = Domain(strings.ToLower(strings.TrimSpace(string(pack.Metadata.Domain))))
	pack.Spec.Inputs.Schema = strings.TrimSpace(pack.Spec.Inputs.Schema)
	for i := range pack.Spec.Rules {
		normalizeRule(&pack.Spec.Rules[i])
	}
	for i := range pack.Spec.Scoring.Rules {
		pack.Spec.Scoring.Rules[i].ID = strings.TrimSpace(pack.Spec.Scoring.Rules[i].ID)
		pack.Spec.Scoring.Rules[i].When = strings.TrimSpace(pack.Spec.Scoring.Rules[i].When)
		pack.Spec.Scoring.Rules[i].Signal.Severity = NormalizeSeverity(pack.Spec.Scoring.Rules[i].Signal.Severity)
		pack.Spec.Scoring.Rules[i].Signal.Title = strings.TrimSpace(pack.Spec.Scoring.Rules[i].Signal.Title)
		pack.Spec.Scoring.Rules[i].Signal.Evidence = strings.TrimSpace(pack.Spec.Scoring.Rules[i].Signal.Evidence)
	}
	for i := range pack.Spec.Levels {
		pack.Spec.Levels[i].Level = NormalizeSeverity(pack.Spec.Levels[i].Level)
		pack.Spec.Levels[i].When = strings.TrimSpace(pack.Spec.Levels[i].When)
	}
	normalizeSuggestions(&pack.Spec.Suggestions)
	normalizeAggregation(&pack.Spec.Aggregation)
	for i := range pack.Spec.ScopedRules {
		pack.Spec.ScopedRules[i].ID = strings.TrimSpace(pack.Spec.ScopedRules[i].ID)
		normalizeAppScope(&pack.Spec.ScopedRules[i].Scope.App)
		pack.Spec.ScopedRules[i].Suggestions.BusinessCriticality = strings.ToLower(strings.TrimSpace(pack.Spec.ScopedRules[i].Suggestions.BusinessCriticality))
		pack.Spec.ScopedRules[i].Suggestions.DataClassification = strings.ToLower(strings.TrimSpace(pack.Spec.ScopedRules[i].Suggestions.DataClassification))
		for j := range pack.Spec.ScopedRules[i].Rules {
			normalizeRule(&pack.Spec.ScopedRules[i].Rules[j])
		}
	}
}

func normalizeRule(rule *Rule) {
	rule.ID = strings.TrimSpace(rule.ID)
	rule.Severity = NormalizeSeverity(rule.Severity)
	rule.When = strings.TrimSpace(rule.When)
	rule.Title = strings.TrimSpace(rule.Title)
	rule.Evidence = strings.TrimSpace(rule.Evidence)
}

func normalizeSuggestions(suggestions *Suggestions) {
	for i := range suggestions.BusinessCriticality {
		suggestions.BusinessCriticality[i].ID = strings.TrimSpace(suggestions.BusinessCriticality[i].ID)
		suggestions.BusinessCriticality[i].Level = strings.ToLower(strings.TrimSpace(suggestions.BusinessCriticality[i].Level))
		suggestions.BusinessCriticality[i].When = strings.TrimSpace(suggestions.BusinessCriticality[i].When)
	}
	for i := range suggestions.DataClassification {
		suggestions.DataClassification[i].ID = strings.TrimSpace(suggestions.DataClassification[i].ID)
		suggestions.DataClassification[i].Level = strings.ToLower(strings.TrimSpace(suggestions.DataClassification[i].Level))
		suggestions.DataClassification[i].When = strings.TrimSpace(suggestions.DataClassification[i].When)
	}
}

func normalizeAggregation(aggregation *Aggregation) {
	aggregation.RiskLevel.Strategy = strings.ToLower(strings.TrimSpace(aggregation.RiskLevel.Strategy))
	aggregation.RiskLevel.Default = NormalizeSeverity(aggregation.RiskLevel.Default)
	aggregation.RiskReasonCount.Strategy = strings.ToLower(strings.TrimSpace(aggregation.RiskReasonCount.Strategy))
	aggregation.RiskReasonCount.Default = strings.TrimSpace(aggregation.RiskReasonCount.Default)
}

func normalizeAppScope(scope *AppScope) {
	scope.CanonicalKey = strings.ToLower(strings.TrimSpace(scope.CanonicalKey))
	scope.PrimaryDomain = strings.ToLower(strings.TrimSpace(scope.PrimaryDomain))
	scope.VendorName = strings.ToLower(strings.TrimSpace(scope.VendorName))
	scope.SourceKind = strings.ToLower(strings.TrimSpace(scope.SourceKind))
	scope.SourceName = strings.TrimSpace(scope.SourceName)
	scope.Category = strings.ToLower(strings.TrimSpace(scope.Category))
	for i := range scope.DomainMatches {
		scope.DomainMatches[i] = strings.ToLower(strings.TrimSpace(scope.DomainMatches[i]))
	}
}
