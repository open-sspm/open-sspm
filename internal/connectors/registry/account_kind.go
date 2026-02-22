package registry

import (
	"encoding/json"
	"strings"
	"unicode"
)

const (
	AccountKindHuman   = "human"
	AccountKindService = "service"
	AccountKindBot     = "bot"
	AccountKindUnknown = "unknown"
)

const (
	EntityCategoryUser             = "user"
	EntityCategoryGroup            = "group"
	EntityCategoryServicePrincipal = "service_principal"
	EntityCategoryServiceAccount   = "service_account"
	EntityCategoryTeam             = "team"
	EntityCategoryRole             = "role"
	EntityCategoryAuthRole         = "auth_role"
	EntityCategoryEntity           = "entity"
)

func NormalizeAccountKind(kind string) string {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case AccountKindHuman:
		return AccountKindHuman
	case AccountKindService:
		return AccountKindService
	case AccountKindBot:
		return AccountKindBot
	default:
		return AccountKindUnknown
	}
}

func AggregateAccountKinds(kinds ...string) string {
	best := AccountKindUnknown
	bestWeight := accountKindWeight(best)
	for _, kind := range kinds {
		normalized := NormalizeAccountKind(kind)
		weight := accountKindWeight(normalized)
		if weight > bestWeight {
			best = normalized
			bestWeight = weight
		}
	}
	return best
}

func ClassifyKindFromSignals(values ...string) string {
	normalized := NormalizeClassifierText(strings.Join(values, " "))
	if HasIndicator(normalized, BotIndicators()) {
		return AccountKindBot
	}
	if HasIndicator(normalized, ServiceIndicators()) {
		return AccountKindService
	}
	return AccountKindUnknown
}

func BotIndicators() []string {
	return []string{
		"bot",
		"github actions",
		"dependabot",
		"renovate",
		"automation",
	}
}

func ServiceIndicators() []string {
	return []string{
		"service account",
		"service principal",
		"svc",
		"workload",
		"daemon",
		"managed identity",
		"spn",
		"terraform",
		"ansible",
		"jenkins",
	}
}

func NormalizeClassifierText(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" {
		return ""
	}
	builder := strings.Builder{}
	builder.Grow(len(raw))
	lastWasSpace := false
	for _, r := range raw {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			builder.WriteRune(r)
			lastWasSpace = false
			continue
		}
		if !lastWasSpace {
			builder.WriteByte(' ')
			lastWasSpace = true
		}
	}
	return strings.Join(strings.Fields(builder.String()), " ")
}

func HasIndicator(normalizedText string, indicators []string) bool {
	normalizedText = strings.TrimSpace(normalizedText)
	if normalizedText == "" {
		return false
	}
	padded := " " + normalizedText + " "
	for _, indicator := range indicators {
		indicator = strings.TrimSpace(indicator)
		if indicator == "" {
			continue
		}
		if strings.Contains(padded, " "+indicator+" ") {
			return true
		}
	}
	return false
}

func WithEntityCategory(raw []byte, category string) []byte {
	category = strings.ToLower(strings.TrimSpace(category))
	if category == "" {
		return NormalizeJSON(raw)
	}

	payload := make(map[string]any)
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &payload); err != nil {
			payload = make(map[string]any)
		}
	}
	payload["entity_category"] = category
	return MarshalJSON(payload)
}

func accountKindWeight(kind string) int {
	switch NormalizeAccountKind(kind) {
	case AccountKindBot:
		return 4
	case AccountKindService:
		return 3
	case AccountKindHuman:
		return 2
	default:
		return 1
	}
}
