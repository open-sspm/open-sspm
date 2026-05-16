package identitydetail

import (
	"encoding/json"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/open-sspm/open-sspm/internal/connectordisplay"
)

// EntitlementGroupMode selects how identity detail entitlements are bucketed.
type EntitlementGroupMode string

const (
	EntitlementGroupSource   EntitlementGroupMode = "source"
	EntitlementGroupKind     EntitlementGroupMode = "kind"
	EntitlementGroupResource EntitlementGroupMode = "resource"
	EntitlementGroupNone     EntitlementGroupMode = "none"
)

// LinkedAccountSortMode selects the sort order for linked source accounts.
type LinkedAccountSortMode string

const (
	LinkedAccountSortGrants   LinkedAccountSortMode = "grants"
	LinkedAccountSortSource   LinkedAccountSortMode = "source"
	LinkedAccountSortActivity LinkedAccountSortMode = "activity"
)

type ShowQuery struct {
	Group              EntitlementGroupMode
	AccountQuery       string
	EntitlementQuery   string
	AccountSort        LinkedAccountSortMode
	EntitlementAdmin   bool
	EntitlementDormant bool
	EntitlementSource  string
}

func BuildShowHref(basePath string, q ShowQuery) string {
	values := url.Values{}
	if q.Group != "" && q.Group != EntitlementGroupResource {
		values.Set("group", string(q.Group))
	}
	if accountQuery := strings.TrimSpace(q.AccountQuery); accountQuery != "" {
		values.Set("account_q", accountQuery)
	}
	if entitlementQuery := strings.TrimSpace(q.EntitlementQuery); entitlementQuery != "" {
		values.Set("entitlement_q", entitlementQuery)
	}
	if q.AccountSort != "" && q.AccountSort != LinkedAccountSortGrants {
		values.Set("account_sort", string(q.AccountSort))
	}
	if q.EntitlementAdmin {
		values.Set("admin", "1")
	}
	if q.EntitlementDormant {
		values.Set("dormant", "1")
	}
	if sourceKind := strings.TrimSpace(q.EntitlementSource); sourceKind != "" {
		values.Set("source_kind", sourceKind)
	}
	encoded := values.Encode()
	if encoded == "" {
		return basePath
	}
	return basePath + "?" + encoded
}

type EntitlementGroupFields struct {
	AccountSourceKind string
	AccountSourceName string
	Kind              string
	ResourceKind      string
	ResourceID        string
	ResourceLabel     string
	ResourceHref      string
	Permission        string
	IsAdmin           bool
	LastActivityUnix  int64
	Dormant           bool
}

type EntitlementGroupItem interface {
	EntitlementGroupFields() EntitlementGroupFields
}

type EntitlementGroup[T EntitlementGroupItem] struct {
	Key           string
	Title         string
	TitleHref     string
	Subtitle      string
	Count         int
	SourceKind    string
	Items         []T
	PreviewItems  []T
	OverflowItems []T
	OverflowCount int
}

func BuildEntitlementGroups[T EntitlementGroupItem](items []T, mode EntitlementGroupMode) []EntitlementGroup[T] {
	if len(items) == 0 {
		return nil
	}
	const previewLimit = 10
	if mode == EntitlementGroupNone {
		sorted := append([]T(nil), items...)
		sort.SliceStable(sorted, func(i, j int) bool {
			return entitlementItemLess(sorted[i].EntitlementGroupFields(), sorted[j].EntitlementGroupFields())
		})
		preview, overflow := splitEntitlementPreview(sorted, previewLimit)
		return []EntitlementGroup[T]{{
			Key:           "all",
			Count:         len(sorted),
			Items:         sorted,
			PreviewItems:  preview,
			OverflowItems: overflow,
			OverflowCount: len(overflow),
		}}
	}

	type bucket struct {
		key        string
		title      string
		titleHref  string
		subtitle   string
		sourceKind string
		items      []T
	}
	bucketByKey := map[string]*bucket{}
	keyOrder := []string{}
	ambiguousResources := ambiguousResourceKeys(items, mode)

	for _, item := range items {
		fields := item.EntitlementGroupFields()
		showSourceScope := ambiguousResources[resourceBaseKey(fields)]
		key, title, titleHref, subtitle, sourceKind := entitlementGroupDescriptor(fields, mode, showSourceScope)

		b, ok := bucketByKey[key]
		if !ok {
			b = &bucket{
				key:        key,
				title:      title,
				titleHref:  titleHref,
				subtitle:   subtitle,
				sourceKind: sourceKind,
			}
			bucketByKey[key] = b
			keyOrder = append(keyOrder, key)
		}
		b.items = append(b.items, item)
	}

	out := make([]EntitlementGroup[T], 0, len(keyOrder))
	for _, key := range keyOrder {
		b := bucketByKey[key]
		sort.SliceStable(b.items, func(i, j int) bool {
			return entitlementItemLess(b.items[i].EntitlementGroupFields(), b.items[j].EntitlementGroupFields())
		})
		preview, overflow := splitEntitlementPreview(b.items, previewLimit)
		out = append(out, EntitlementGroup[T]{
			Key:           b.key,
			Title:         b.title,
			TitleHref:     b.titleHref,
			Subtitle:      b.subtitle,
			Count:         len(b.items),
			SourceKind:    b.sourceKind,
			Items:         b.items,
			PreviewItems:  preview,
			OverflowItems: overflow,
			OverflowCount: len(overflow),
		})
	}

	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return strings.ToLower(out[i].Title) < strings.ToLower(out[j].Title)
	})
	return out
}

func ambiguousResourceKeys[T EntitlementGroupItem](items []T, mode EntitlementGroupMode) map[string]bool {
	if mode != EntitlementGroupResource {
		return nil
	}
	sourcesByResource := map[string]map[string]struct{}{}
	for _, item := range items {
		fields := item.EntitlementGroupFields()
		key := resourceBaseKey(fields)
		if key == "" {
			continue
		}
		if sourcesByResource[key] == nil {
			sourcesByResource[key] = map[string]struct{}{}
		}
		sourceKey := strings.ToLower(strings.TrimSpace(fields.AccountSourceKind)) + "\x00" + strings.ToLower(strings.TrimSpace(fields.AccountSourceName))
		sourcesByResource[key][sourceKey] = struct{}{}
	}
	out := map[string]bool{}
	for key, sources := range sourcesByResource {
		if len(sources) > 1 {
			out[key] = true
		}
	}
	return out
}

func resourceBaseKey(fields EntitlementGroupFields) string {
	rid := strings.TrimSpace(fields.ResourceID)
	if rid == "" {
		rid = strings.TrimSpace(fields.ResourceLabel)
	}
	if rid == "" && strings.TrimSpace(fields.ResourceKind) == "" {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(fields.ResourceKind)) + "\x00" + rid
}

func entitlementGroupDescriptor(fields EntitlementGroupFields, mode EntitlementGroupMode, showSourceScope bool) (key, title, titleHref, subtitle, sourceKind string) {
	switch mode {
	case EntitlementGroupKind:
		key = strings.ToLower(strings.TrimSpace(fields.Kind))
		if key == "" {
			return "_unknown_kind", "Unknown kind", "", "", ""
		}
		return key, fields.Kind, "", "", ""
	case EntitlementGroupResource:
		rid := strings.TrimSpace(fields.ResourceID)
		if rid == "" {
			rid = strings.TrimSpace(fields.ResourceLabel)
		}
		sourceKindKey := strings.ToLower(strings.TrimSpace(fields.AccountSourceKind))
		sourceNameKey := strings.ToLower(strings.TrimSpace(fields.AccountSourceName))
		resourceKindKey := strings.ToLower(strings.TrimSpace(fields.ResourceKind))
		key = sourceKindKey + "\x00" + sourceNameKey + "\x00" + resourceKindKey + "\x00" + rid
		title = strings.TrimSpace(fields.ResourceLabel)
		if title == "" {
			title = "(unknown resource)"
		}
		return key, title, fields.ResourceHref, resourceSubtitle(fields, showSourceScope), strings.TrimSpace(fields.AccountSourceKind)
	case EntitlementGroupSource:
		fallthrough
	default:
		key = strings.ToLower(strings.TrimSpace(fields.AccountSourceKind)) + "\x00" + strings.ToLower(strings.TrimSpace(fields.AccountSourceName))
		sourceKind = strings.TrimSpace(fields.AccountSourceKind)
		title = ConnectorKindLabel(fields.AccountSourceKind)
		if scope := ConnectorScopeLabel(fields.AccountSourceKind); scope != "" && strings.TrimSpace(fields.AccountSourceName) != "" {
			subtitle = scope + ": " + strings.TrimSpace(fields.AccountSourceName)
		} else if name := strings.TrimSpace(fields.AccountSourceName); name != "" {
			subtitle = name
		}
		return key, title, "", subtitle, sourceKind
	}
}

func resourceSubtitle(fields EntitlementGroupFields, showSourceScope bool) string {
	parts := make([]string, 0, 2)
	if kind := strings.TrimSpace(fields.Kind); kind != "" {
		parts = append(parts, kind)
	}
	if !showSourceScope {
		return strings.Join(parts, " · ")
	}
	sourceName := strings.TrimSpace(fields.AccountSourceName)
	if sourceName != "" {
		if scope := ConnectorScopeLabel(fields.AccountSourceKind); scope != "" {
			parts = append(parts, scope+": "+sourceName)
		} else {
			parts = append(parts, sourceName)
		}
	}
	return strings.Join(parts, " · ")
}

func entitlementItemLess(left, right EntitlementGroupFields) bool {
	if !strings.EqualFold(left.AccountSourceKind, right.AccountSourceKind) {
		return strings.ToLower(left.AccountSourceKind) < strings.ToLower(right.AccountSourceKind)
	}
	if left.IsAdmin != right.IsAdmin {
		return left.IsAdmin
	}
	if left.Permission != right.Permission {
		return strings.ToLower(left.Permission) < strings.ToLower(right.Permission)
	}
	if left.Dormant != right.Dormant {
		return left.Dormant
	}
	if left.LastActivityUnix != right.LastActivityUnix {
		return left.LastActivityUnix > right.LastActivityUnix
	}
	return strings.ToLower(left.ResourceLabel) < strings.ToLower(right.ResourceLabel)
}

func splitEntitlementPreview[T any](items []T, limit int) ([]T, []T) {
	if limit <= 0 || len(items) <= limit {
		return items, nil
	}
	return items[:limit], items[limit:]
}

type SummaryTile struct {
	Label    string
	Value    string
	Sublabel string
	Danger   bool
	Warn     bool
}

func BuildSummaryTiles(
	linkedCount int,
	totalEntitlements int,
	adminCount int,
	dormantCount int,
	distinctSources []string,
	distinctSourceKindCount int,
	adminByKind map[string]int,
) []SummaryTile {
	tiles := make([]SummaryTile, 0, 4)

	if linkedCount > 0 {
		tiles = append(tiles, SummaryTile{
			Label:    "Source accounts",
			Value:    strconv.Itoa(linkedCount),
			Sublabel: summarizeDistinctSources(distinctSources),
		})
	}

	if totalEntitlements > 0 {
		sublabel := "across 1 source"
		if distinctSourceKindCount != 1 {
			sublabel = "across " + strconv.Itoa(distinctSourceKindCount) + " sources"
		}
		tiles = append(tiles, SummaryTile{
			Label:    "Access grants",
			Value:    strconv.Itoa(totalEntitlements),
			Sublabel: sublabel,
		})
	}

	if adminCount > 0 {
		tiles = append(tiles, SummaryTile{
			Label:    "Admin scopes",
			Value:    strconv.Itoa(adminCount),
			Sublabel: SummarizeAdminByKind(adminByKind),
			Danger:   true,
		})
	}

	if linkedCount > 0 {
		sublabel := "no accounts dormant"
		if dormantCount > 0 {
			sublabel = "60d+ inactivity"
		}
		tiles = append(tiles, SummaryTile{
			Label:    "Dormant accounts",
			Value:    strconv.Itoa(dormantCount),
			Sublabel: sublabel,
			Warn:     dormantCount > 0,
		})
	}

	return tiles
}

func summarizeDistinctSources(sources []string) string {
	if len(sources) == 0 {
		return ""
	}
	humanized := make([]string, 0, len(sources))
	for _, source := range sources {
		label := ConnectorKindLabel(source)
		if label == "" {
			continue
		}
		humanized = append(humanized, label)
	}
	if len(humanized) == 0 {
		return ""
	}
	if len(humanized) <= 3 {
		return strings.Join(humanized, " · ")
	}
	remaining := len(humanized) - 3
	return strings.Join(humanized[:3], " · ") + " · +" + strconv.Itoa(remaining) + " more"
}

func SummarizeAdminByKind(byKind map[string]int) string {
	if len(byKind) == 0 {
		return ""
	}
	type kv struct {
		kind  string
		count int
	}
	pairs := make([]kv, 0, len(byKind))
	for k, c := range byKind {
		if c == 0 {
			continue
		}
		pairs = append(pairs, kv{kind: k, count: c})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].count != pairs[j].count {
			return pairs[i].count > pairs[j].count
		}
		return strings.ToLower(pairs[i].kind) < strings.ToLower(pairs[j].kind)
	})
	max := len(pairs)
	if max > 3 {
		max = 3
	}
	parts := make([]string, 0, max)
	for i := 0; i < max; i++ {
		parts = append(parts, strconv.Itoa(pairs[i].count)+" "+shortKindLabel(pairs[i].kind, pairs[i].count))
	}
	return strings.Join(parts, " · ")
}

func shortKindLabel(kind string, count int) string {
	lower := strings.ToLower(strings.TrimSpace(kind))
	plural := func(singular, plural string) string {
		if count == 1 {
			return singular
		}
		return plural
	}
	switch {
	case lower == "":
		return plural("scope", "scopes")
	case strings.Contains(lower, "repo"):
		return plural("repo", "repos")
	case strings.Contains(lower, "org_role") || strings.Contains(lower, "organization_role"):
		return plural("org", "orgs")
	case strings.Contains(lower, "team"):
		return plural("team", "teams")
	case strings.Contains(lower, "group"):
		return plural("group", "groups")
	case strings.Contains(lower, "role"):
		return plural("role", "roles")
	case strings.Contains(lower, "policy"):
		return plural("policy", "policies")
	case strings.Contains(lower, "permission"):
		return plural("permission", "permissions")
	default:
		return lower
	}
}

func ConnectorKindLabel(kind string) string {
	return connectordisplay.KindLabel(kind)
}

func ConnectorScopeLabel(kind string) string {
	return connectordisplay.ScopeLabel(kind)
}

func IsDormantAt(now, observedAt time.Time, threshold time.Duration) bool {
	if observedAt.IsZero() {
		return false
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	return now.UTC().Sub(observedAt.UTC()) >= threshold
}

func IsPrivilegedPermission(rawPermission string) bool {
	return hasPrivilegedRoleText(rawPermission)
}

func IsPrivilegedEntitlement(permission string, rawJSON []byte) bool {
	if IsPrivilegedPermission(permission) {
		return true
	}
	raw := decodeRawJSONObject(rawJSON)
	rawLabel := strings.ToLower(firstJSONText(raw,
		"role_name",
		"roleName",
		"name",
		"displayName",
		"display_name",
		"permissionSet",
		"permission_set",
		"permissionSetName",
		"permission_set_name",
	))
	return hasPrivilegedRoleText(rawLabel)
}

func hasPrivilegedRoleText(raw string) bool {
	normalized := normalizedRoleText(raw)
	if normalized == "" {
		return false
	}
	switch normalized {
	case "admin", "administrator", "owner", "root", "superuser", "poweruser", "maintain", "manage", "write":
		return true
	case "full access", "fullaccess", "administrator access", "administratoraccess", "power user", "poweruser access", "poweruseraccess":
		return true
	}

	tokens := strings.Fields(normalized)
	if hasAnyToken(tokens, "view", "viewer", "readonly", "read", "audit", "auditor", "contact", "report", "reporting") {
		return false
	}
	if hasAnyToken(tokens, "admin", "administrator", "owner", "root", "superuser", "poweruser") {
		return true
	}
	return hasToken(tokens, "full") && hasToken(tokens, "access")
}

func normalizedRoleText(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	var b strings.Builder
	var prev rune
	for _, r := range raw {
		if unicode.IsUpper(r) && prev != 0 && (unicode.IsLower(prev) || unicode.IsDigit(prev)) {
			b.WriteRune(' ')
		}
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(unicode.ToLower(r))
		} else {
			b.WriteRune(' ')
		}
		prev = r
	}
	return strings.Join(strings.Fields(b.String()), " ")
}

func hasToken(tokens []string, want string) bool {
	for _, token := range tokens {
		if token == want {
			return true
		}
	}
	return false
}

func hasAnyToken(tokens []string, wants ...string) bool {
	for _, want := range wants {
		if hasToken(tokens, want) {
			return true
		}
	}
	return false
}

func decodeRawJSONObject(raw []byte) map[string]any {
	if len(raw) == 0 {
		return nil
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil
	}
	return out
}

func firstJSONText(raw map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := raw[key]; ok {
			if text := jsonValueText(value); text != "" {
				return text
			}
		}
	}
	return ""
}

func jsonValueText(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case bool:
		if typed {
			return "Yes"
		}
		return "No"
	case map[string]any:
		return firstJSONText(typed, "displayName", "display_name", "name", "email", "mail", "userPrincipalName", "user_principal_name")
	case []any:
		parts := make([]string, 0, len(typed))
		for _, item := range typed {
			if text := jsonValueText(item); text != "" {
				parts = append(parts, text)
			}
			if len(parts) == 2 {
				break
			}
		}
		if len(parts) == 0 {
			return ""
		}
		if len(typed) > len(parts) {
			return strings.Join(parts, " · ") + " · +" + strconv.Itoa(len(typed)-len(parts))
		}
		return strings.Join(parts, " · ")
	default:
		return ""
	}
}
