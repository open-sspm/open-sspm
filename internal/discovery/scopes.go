package discovery

import (
	"encoding/json"
	"slices"
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

func HasPrivilegedScopes(scopes []string) bool {
	return slices.ContainsFunc(NormalizeScopes(scopes), isPrivilegedScope)
}

func HasConfidentialScopes(scopes []string) bool {
	return slices.ContainsFunc(NormalizeScopes(scopes), isConfidentialScope)
}

func isPrivilegedScope(scope string) bool {
	privilegedNeedles := []string{
		"directory.readwrite.all",
		"application.readwrite.all",
		"rolemanagement.readwrite.directory",
		"mailboxsettings.readwrite",
		"full_access_as_app",
		"files.readwrite.all",
		"sites.readwrite.all",
		"user.readwrite.all",
		"offline_access",
	}
	for _, needle := range privilegedNeedles {
		if strings.Contains(scope, needle) {
			return true
		}
	}
	return false
}

func isConfidentialScope(scope string) bool {
	confidentialNeedles := []string{
		"mail.",
		"files.",
		"calendar.",
		"readwrite",
		"sites.read",
	}
	for _, needle := range confidentialNeedles {
		if strings.Contains(scope, needle) {
			return true
		}
	}
	return false
}
