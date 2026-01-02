package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestLoadRulesFromOpenSSPMDescriptor_SaneAndDeterministic(t *testing.T) {
	t.Parallel()

	first, err := loadRulesFromOpenSSPMDescriptor()
	if err != nil {
		t.Fatalf("load rulesets: %v", err)
	}
	if len(first) == 0 {
		t.Fatalf("expected at least 1 ruleset")
	}

	firstByKey := make(map[string]LoadedRuleset, len(first))
	for _, ls := range first {
		key := strings.TrimSpace(ls.Ruleset.Key)
		if key == "" {
			t.Fatalf("missing ruleset key")
		}
		if _, ok := firstByKey[key]; ok {
			t.Fatalf("duplicate ruleset key %q", key)
		}

		if strings.TrimSpace(ls.Ruleset.DefinitionHash) == "" {
			t.Fatalf("%s: missing ruleset definition hash", key)
		}
		if !json.Valid(ls.Ruleset.DefinitionJson) {
			t.Fatalf("%s: ruleset definition_json is invalid JSON", key)
		}

		ruleKeys := make(map[string]struct{}, len(ls.Rules))
		for _, r := range ls.Rules {
			ruleKey := strings.TrimSpace(r.Key)
			if ruleKey == "" {
				t.Fatalf("%s: missing rule key", key)
			}
			if _, ok := ruleKeys[ruleKey]; ok {
				t.Fatalf("%s: duplicate rule key %q", key, ruleKey)
			}
			ruleKeys[ruleKey] = struct{}{}

			if !json.Valid(r.RequiredData) {
				t.Fatalf("%s/%s: required_data is invalid JSON", key, ruleKey)
			}
			if !json.Valid(r.ExpectedParams) {
				t.Fatalf("%s/%s: expected_params is invalid JSON", key, ruleKey)
			}
			if !json.Valid(r.DefinitionJson) {
				t.Fatalf("%s/%s: rule definition_json is invalid JSON", key, ruleKey)
			}
		}

		firstByKey[key] = ls
	}

	second, err := loadRulesFromOpenSSPMDescriptor()
	if err != nil {
		t.Fatalf("reload rulesets: %v", err)
	}

	secondByKey := make(map[string]LoadedRuleset, len(second))
	for _, ls := range second {
		key := strings.TrimSpace(ls.Ruleset.Key)
		if key == "" {
			t.Fatalf("missing ruleset key")
		}
		secondByKey[key] = ls
	}

	if len(secondByKey) != len(firstByKey) {
		t.Fatalf("ruleset count changed between loads: first=%d second=%d", len(firstByKey), len(secondByKey))
	}

	for key, a := range firstByKey {
		b, ok := secondByKey[key]
		if !ok {
			t.Fatalf("ruleset missing on second load: %s", key)
		}
		if a.Ruleset.DefinitionHash != b.Ruleset.DefinitionHash {
			t.Fatalf("%s: definition hash changed between loads: first=%s second=%s", key, a.Ruleset.DefinitionHash, b.Ruleset.DefinitionHash)
		}
		if !bytes.Equal(a.Ruleset.DefinitionJson, b.Ruleset.DefinitionJson) {
			t.Fatalf("%s: definition_json changed between loads", key)
		}

		if len(a.Rules) != len(b.Rules) {
			t.Fatalf("%s: rule count changed between loads: first=%d second=%d", key, len(a.Rules), len(b.Rules))
		}

		aRules := make(map[string]SeedRule, len(a.Rules))
		for _, r := range a.Rules {
			aRules[r.Key] = r
		}
		for _, r := range b.Rules {
			prev, ok := aRules[r.Key]
			if !ok {
				t.Fatalf("%s: rule missing on first load: %s", key, r.Key)
			}
			if prev.IsActive != r.IsActive {
				t.Fatalf("%s/%s: is_active changed between loads", key, r.Key)
			}
			if !bytes.Equal(prev.RequiredData, r.RequiredData) {
				t.Fatalf("%s/%s: required_data changed between loads", key, r.Key)
			}
			if !bytes.Equal(prev.ExpectedParams, r.ExpectedParams) {
				t.Fatalf("%s/%s: expected_params changed between loads", key, r.Key)
			}
			if !bytes.Equal(prev.DefinitionJson, r.DefinitionJson) {
				t.Fatalf("%s/%s: definition_json changed between loads", key, r.Key)
			}
		}
	}
}
