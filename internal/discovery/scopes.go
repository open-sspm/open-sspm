package discovery

import (
	"encoding/json"
	"strings"
)

func NormalizeScopes(scopes []string) []string {
	if len(scopes) == 0 {
		return nil
	}
	out := make([]string, 0, len(scopes))
	seen := make(map[string]struct{}, len(scopes))
	for _, scope := range scopes {
		normalized := strings.ToLower(strings.TrimSpace(scope))
		if normalized == "" {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	return out
}

func ScopesJSON(scopes []string) []byte {
	normalized := NormalizeScopes(scopes)
	if len(normalized) == 0 {
		return []byte("[]")
	}
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return []byte("[]")
	}
	return encoded
}
