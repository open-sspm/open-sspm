package querystate

import (
	"net/url"
	"strconv"
	"strings"

	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
)

type SourceSelection struct {
	Kind string
	Name string
}

func encodeURL(basePath string, values url.Values) string {
	if len(values) == 0 {
		return basePath
	}
	return basePath + "?" + values.Encode()
}

func parsePage(raw string) int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 1
	}
	page, err := strconv.Atoi(raw)
	if err != nil || page < 1 {
		return 1
	}
	return page
}

func normalizeSourceKind(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "aws", configstore.KindAWSIdentityCenter:
		return "aws"
	default:
		return strings.ToLower(strings.TrimSpace(raw))
	}
}

func setIfNotEmpty(values url.Values, key, value string) {
	value = strings.TrimSpace(value)
	if value != "" {
		values.Set(key, value)
	}
}

func setIfPositive(values url.Values, key string, value int) {
	if value > 0 {
		values.Set(key, strconv.Itoa(value))
	}
}

func setIfPage(values url.Values, page int) {
	if page > 1 {
		values.Set("page", strconv.Itoa(page))
	}
}

func setIfTrue(values url.Values, key string, enabled bool) {
	if enabled {
		values.Set(key, "1")
	}
}

func parseBool(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func parseNonNegativeInt(raw string, maxValue int) int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		return 0
	}
	if maxValue > 0 && value > maxValue {
		return maxValue
	}
	return value
}

func hasSourceKind(kind string, sources []SourceSelection) bool {
	kind = normalizeSourceKind(kind)
	if kind == "" {
		return false
	}
	for _, source := range sources {
		if normalizeSourceKind(source.Kind) == kind {
			return true
		}
	}
	return false
}

func firstSourceKindByName(name string, sources []SourceSelection) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	for _, source := range sources {
		if strings.EqualFold(strings.TrimSpace(source.Name), name) {
			return normalizeSourceKind(source.Kind)
		}
	}
	return ""
}

func canonicalSourceSelection(values url.Values, sources []SourceSelection) SourceSelection {
	selectedKind := normalizeSourceKind(values.Get("source_kind"))
	if selectedKind != "" && hasSourceKind(selectedKind, sources) {
		return SourceSelection{Kind: selectedKind}
	}

	selectedName := strings.TrimSpace(values.Get("source_name"))
	if selectedKind = firstSourceKindByName(selectedName, sources); selectedKind != "" {
		return SourceSelection{Kind: selectedKind}
	}

	return SourceSelection{}
}
