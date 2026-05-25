package riskpolicy

import (
	"bytes"
	"encoding/json"
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
	Policy    PolicyPack
	evaluator entityPolicyEvaluator
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

func decodePolicyPack(name string, data []byte) (PolicyPack, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	var raw map[string]any
	if err := dec.Decode(&raw); err != nil {
		return PolicyPack{}, fmt.Errorf("%s: decode policy: %w", name, err)
	}
	var trailing any
	if err := dec.Decode(&trailing); err != nil && err != io.EOF {
		return PolicyPack{}, fmt.Errorf("%s: decode policy trailing document: %w", name, err)
	} else if err == nil {
		return PolicyPack{}, fmt.Errorf("%s: multiple YAML documents are not supported", name)
	}
	if err := validateEntityPolicyPackDocumentKeys(raw); err != nil {
		return PolicyPack{}, fmt.Errorf("%s: decode policy: %w", name, err)
	}

	b, err := json.Marshal(raw)
	if err != nil {
		return PolicyPack{}, fmt.Errorf("%s: marshal normalized policy document: %w", name, err)
	}
	var doc osspecv2.EntityPolicyPackDoc
	if err := json.Unmarshal(b, &doc); err != nil {
		return PolicyPack{}, fmt.Errorf("%s: unmarshal policy: %w", name, err)
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

func validateEntityPolicyPackDocumentKeys(raw map[string]any) error {
	if raw == nil {
		return fmt.Errorf("document must be a mapping")
	}
	if err := validateAllowedKeys("document", raw, "schema_version", "kind", "entity_policy_pack"); err != nil {
		return err
	}
	pack, ok := rawMap(raw["entity_policy_pack"])
	if !ok {
		return fmt.Errorf("entity_policy_pack must be a mapping")
	}
	if err := validateAllowedKeys("entity_policy_pack", pack, "metadata", "inputs", "policy"); err != nil {
		return err
	}
	metadata, ok := rawMap(pack["metadata"])
	if !ok {
		return fmt.Errorf("entity_policy_pack.metadata must be a mapping")
	}
	if err := validateAllowedKeys("entity_policy_pack.metadata", metadata, "id", "version", "domain"); err != nil {
		return err
	}
	if inputs, ok := rawMap(pack["inputs"]); ok {
		if err := validateAllowedKeys("entity_policy_pack.inputs", inputs, "schema"); err != nil {
			return err
		}
	}
	policy, ok := rawMap(pack["policy"])
	if !ok {
		return fmt.Errorf("entity_policy_pack.policy must be a mapping")
	}
	return validateAllowedKeys("entity_policy_pack.policy", policy, "engine", "package", "query", "rego", "rego_path")
}

func rawMap(value any) (map[string]any, bool) {
	m, ok := value.(map[string]any)
	return m, ok
}

func validateAllowedKeys(path string, raw map[string]any, allowed ...string) error {
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, key := range allowed {
		allowedSet[key] = struct{}{}
	}
	keys := make([]string, 0, len(raw))
	for key := range raw {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if _, ok := allowedSet[key]; !ok {
			return fmt.Errorf("%s: field %s not found", path, key)
		}
	}
	return nil
}

func specPolicyPacks() []PolicyPack {
	out := make([]PolicyPack, 0, len(osspecv2.GeneratedDescriptor.EntityPolicyPacks))
	for _, compiled := range osspecv2.GeneratedDescriptor.EntityPolicyPacks {
		out = append(out, compiled.Object.EntityPolicyPack)
	}
	return out
}

func clonePolicyPack(pack PolicyPack) PolicyPack {
	return pack
}

func normalizePolicyPack(pack *PolicyPack) {
	pack.Metadata.ID = strings.TrimSpace(pack.Metadata.ID)
	pack.Metadata.Version = strings.TrimSpace(pack.Metadata.Version)
	pack.Metadata.Domain = Domain(strings.ToLower(strings.TrimSpace(string(pack.Metadata.Domain))))
	pack.Inputs.Schema = strings.TrimSpace(pack.Inputs.Schema)
	pack.Policy.Engine = osspecv2.CheckEngine(strings.ToLower(strings.TrimSpace(string(pack.Policy.Engine))))
	pack.Policy.Package = strings.TrimSpace(pack.Policy.Package)
	pack.Policy.Query = strings.TrimSpace(pack.Policy.Query)
	pack.Policy.Rego = strings.TrimSpace(pack.Policy.Rego)
	pack.Policy.RegoPath = strings.TrimSpace(pack.Policy.RegoPath)
}

func compilePack(name string, pack PolicyPack) (CompiledPack, error) {
	if err := validatePolicyPack(name, pack); err != nil {
		return CompiledPack{}, err
	}
	evaluator, err := prepareEntityPolicyEvaluator(name, pack)
	if err != nil {
		return CompiledPack{}, err
	}
	return CompiledPack{Policy: pack, evaluator: evaluator}, nil
}
