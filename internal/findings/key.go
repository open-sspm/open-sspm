package findings

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

func BuildKey(result FindingResult) string {
	seed := strings.Join([]string{
		"default",
		normalize(result.Source.Kind),
		strings.TrimSpace(result.Source.Name),
		strings.TrimSpace(result.Policy.BundleID),
		strings.TrimSpace(result.Policy.ID),
		normalize(result.Scope.Kind),
		normalize(result.Scope.SourceKind),
		strings.TrimSpace(result.Scope.SourceName),
		normalize(result.Entity.Kind),
		strings.TrimSpace(result.Entity.ID),
		normalize(result.Resource.Kind),
		strings.TrimSpace(result.Resource.ID),
	}, "\x00")
	sum := sha256.Sum256([]byte(seed))
	return "finding:" + hex.EncodeToString(sum[:16])
}

func normalize(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}
