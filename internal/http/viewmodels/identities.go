package viewmodels

import (
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/querystate"
)

type IdentityListItem struct {
	ID                int64
	Initials          string
	NamePrimary       string
	NameSecondary     string
	IdentityType      string
	Managed           bool
	SourceKind        string
	SourceName        string
	IntegrationsCount int64
	PrivilegedRoles   int64
	Status            string
	ActivityState     string
	LastSeen          TimeDisplay
	FirstSeen         TimeDisplay
	RowState          string
}

// IdentitiesSummary holds pre-filter bucketed counts for the identity
// inventory. It is scoped to the user's source, search, and type filters but
// ignores segment-like filters (managed/status/activity/privileged) so the
// stat strip and segment chips can show the shape of the population a user is
// actually looking at, independent of which segment they have clicked.
type IdentitiesSummary struct {
	Total               int64
	ActionRequired      int64
	Review              int64
	Privileged          int64
	PrivilegedUnmanaged int64
	StalePrivileged     int64
	Unmanaged           int64
	Suspended           int64
	Stale               int64
}

type IdentitiesViewData struct {
	PaginatedListPageData
	Items             []IdentityListItem
	Sources           []ProgrammaticSourceOption
	SourceNameOptions []ProgrammaticSourceOption
	Query             querystate.IdentitiesQuery
	Summary           IdentitiesSummary
	HasIdentities     bool
}

type IdentityLinkedAccountView struct {
	Account          gen.Account
	EntitlementCount int
	DetailHref       string
	StatusActive     bool
	LastSignIn       TimeDisplay
	LastSignInUnix   int64
	Dormant          bool
}

type IdentityEntitlementView struct {
	AccountLabel      string
	AccountHref       string
	AccountSourceKind string
	AccountSourceName string
	Kind              string
	ResourceKind      string
	ResourceID        string
	ResourceLabel     string
	ResourceHref      string
	Permission        string
	IsAdmin           bool
	LastUsed          TimeDisplay
	LastUsedUnix      int64
	Dormant           bool
}

// IdentityEntitlementGroupMode selects how entitlements are bucketed in the
// identity detail page. It is bound to the ?group= query param.
type IdentityEntitlementGroupMode string

const (
	IdentityEntitlementGroupSource   IdentityEntitlementGroupMode = "source"
	IdentityEntitlementGroupKind     IdentityEntitlementGroupMode = "kind"
	IdentityEntitlementGroupResource IdentityEntitlementGroupMode = "resource"
	IdentityEntitlementGroupNone     IdentityEntitlementGroupMode = "none"
)

// IdentityLinkedAccountSortMode selects the sort order for the linked-account
// table on the identity detail page.
type IdentityLinkedAccountSortMode string

const (
	IdentityLinkedAccountSortGrants   IdentityLinkedAccountSortMode = "grants"
	IdentityLinkedAccountSortSource   IdentityLinkedAccountSortMode = "source"
	IdentityLinkedAccountSortActivity IdentityLinkedAccountSortMode = "activity"
)

// IdentityEntitlementGroup is one bucket inside the entitlements section.
type IdentityEntitlementGroup struct {
	Key           string
	Title         string
	TitleHref     string
	Subtitle      string
	Count         int
	SourceKind    string
	Items         []IdentityEntitlementView
	PreviewItems  []IdentityEntitlementView
	OverflowItems []IdentityEntitlementView
	OverflowCount int
}

// IdentityReviewSummary is the compact verdict shown in the identity header.
// It should answer whether the reviewer needs to act before they inspect rows.
type IdentityReviewSummary struct {
	Label  string
	Detail string
	Tone   string
}

// IdentitySummaryTile is one card in the four-up summary row.
type IdentitySummaryTile struct {
	Label    string
	Value    string
	Sublabel string
	Danger   bool
	Warn     bool
}

// IdentityProfileFact is one compact fact in the profile header.
type IdentityProfileFact struct {
	Label string
	Value string
}

// IdentityFilterChip is a visible active filter with a link that removes it.
type IdentityFilterChip struct {
	Label     string
	ClearHref string
}

type IdentityShowViewData struct {
	Layout                      LayoutData
	Identity                    gen.GetIdentitySummaryByIDRow
	NamePrimary                 string
	NameSecondary               string
	Initials                    string
	AvatarClass                 string
	StatusLabel                 string
	StatusTone                  string
	IdentityTypeLabel           string
	BreadcrumbKindLabel         string
	BreadcrumbKindHref          string
	IdentityTags                []string
	AdminScopeSummary           string
	ReviewSummary               IdentityReviewSummary
	ProfileFacts                []IdentityProfileFact
	CreatedOn                   TimeDisplay
	UpdatedOn                   TimeDisplay
	SummaryTiles                []IdentitySummaryTile
	TotalLinkedAccounts         int
	ActiveLinkedAccounts        int
	DormantLinkedAccounts       int
	TotalEntitlements           int
	VisibleLinkedAccounts       int
	VisibleEntitlements         int
	LinkedAccounts              []IdentityLinkedAccountView
	Entitlements                []IdentityEntitlementView
	AccountQuery                string
	AccountQueryClearHref       string
	EntitlementQuery            string
	EntitlementClearHref        string
	EntitlementAdminOnly        bool
	EntitlementDormantOnly      bool
	EntitlementSourceFilter     string
	EntitlementSourceOptions    []IdentitySourceFilterOption
	EntitlementFilterCount      int
	EntitlementFilterChips      []IdentityFilterChip
	EntitlementClearFiltersHref string
	EntitlementFormHiddenInputs []IdentityFormHiddenInput
	AccountSortMode             IdentityLinkedAccountSortMode
	EntitlementGroups           []IdentityEntitlementGroup
	NonHumanIdentitiesHref      string
	HasLinkedAccounts           bool
	HasEntitlements             bool
	HasLinkedAccountFilter      bool
	HasEntitlementFilter        bool
}

// IdentitySourceFilterOption represents one entry in the source-kind select
// inside the entitlements filter dropdown.
type IdentitySourceFilterOption struct {
	Value    string
	Label    string
	Selected bool
}

// IdentityFormHiddenInput is a single hidden input that must be carried across
// GET form submissions to preserve unrelated query state.
type IdentityFormHiddenInput struct {
	Name  string
	Value string
}

func ParseLinkedAccountSortMode(raw string) IdentityLinkedAccountSortMode {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case string(IdentityLinkedAccountSortSource):
		return IdentityLinkedAccountSortSource
	case string(IdentityLinkedAccountSortActivity):
		return IdentityLinkedAccountSortActivity
	default:
		return IdentityLinkedAccountSortGrants
	}
}

// IsAdminPermission classifies privileged permissions for the admin stat tile
// and the permission badge styling. It runs against the raw permission string
// from the entitlement row, before any humanization rewrites.
func IsAdminPermission(rawPermission string) bool {
	switch strings.ToLower(strings.TrimSpace(rawPermission)) {
	case "admin", "owner", "write", "manage", "maintain", "root", "super", "superuser":
		return true
	default:
		return false
	}
}

// IdentityShowQuery carries every query-string knob the identity detail page
// understands. Builders take it by value so callers can clone-and-tweak when
// emitting links for toggles or "clear" actions.
type IdentityShowQuery struct {
	Group              IdentityEntitlementGroupMode
	AccountQuery       string
	EntitlementQuery   string
	AccountSort        IdentityLinkedAccountSortMode
	EntitlementAdmin   bool
	EntitlementDormant bool
	EntitlementSource  string
}

func BuildIdentityShowHref(basePath string, q IdentityShowQuery) string {
	values := url.Values{}
	if q.Group != "" && q.Group != IdentityEntitlementGroupResource {
		values.Set("group", string(q.Group))
	}
	if accountQuery := strings.TrimSpace(q.AccountQuery); accountQuery != "" {
		values.Set("account_q", accountQuery)
	}
	if entitlementQuery := strings.TrimSpace(q.EntitlementQuery); entitlementQuery != "" {
		values.Set("entitlement_q", entitlementQuery)
	}
	if q.AccountSort != "" && q.AccountSort != IdentityLinkedAccountSortGrants {
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

// BuildEntitlementGroups buckets the items according to the selected mode.
func BuildEntitlementGroups(items []IdentityEntitlementView, mode IdentityEntitlementGroupMode) []IdentityEntitlementGroup {
	if len(items) == 0 {
		return nil
	}
	const previewLimit = 10
	if mode == IdentityEntitlementGroupNone {
		sorted := append([]IdentityEntitlementView(nil), items...)
		sort.SliceStable(sorted, func(i, j int) bool {
			if sorted[i].AccountSourceKind != sorted[j].AccountSourceKind {
				return strings.ToLower(sorted[i].AccountSourceKind) < strings.ToLower(sorted[j].AccountSourceKind)
			}
			if sorted[i].IsAdmin != sorted[j].IsAdmin {
				return sorted[i].IsAdmin
			}
			if sorted[i].Permission != sorted[j].Permission {
				return strings.ToLower(sorted[i].Permission) < strings.ToLower(sorted[j].Permission)
			}
			if sorted[i].LastUsedUnix != sorted[j].LastUsedUnix {
				return sorted[i].LastUsedUnix > sorted[j].LastUsedUnix
			}
			return strings.ToLower(sorted[i].ResourceLabel) < strings.ToLower(sorted[j].ResourceLabel)
		})
		preview, overflow := splitEntitlementPreview(sorted, previewLimit)
		return []IdentityEntitlementGroup{{
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
		items      []IdentityEntitlementView
	}
	bucketByKey := map[string]*bucket{}
	keyOrder := []string{}

	for _, item := range items {
		var key, title, titleHref, subtitle, sourceKind string

		switch mode {
		case IdentityEntitlementGroupKind:
			key = strings.ToLower(strings.TrimSpace(item.Kind))
			if key == "" {
				key = "_unknown_kind"
				title = "Unknown kind"
			} else {
				title = item.Kind
			}
		case IdentityEntitlementGroupResource:
			rid := strings.TrimSpace(item.ResourceID)
			if rid == "" {
				rid = strings.TrimSpace(item.ResourceLabel)
			}
			key = strings.TrimSpace(item.ResourceKind) + "\x00" + rid
			title = item.ResourceLabel
			if title == "" {
				title = "(unknown resource)"
			}
			titleHref = item.ResourceHref
			subtitle = item.Kind
			sourceKind = strings.TrimSpace(item.AccountSourceKind)
		case IdentityEntitlementGroupSource:
			fallthrough
		default:
			key = strings.ToLower(strings.TrimSpace(item.AccountSourceKind)) + "\x00" + strings.ToLower(strings.TrimSpace(item.AccountSourceName))
			sourceKind = strings.TrimSpace(item.AccountSourceKind)
			title = humanizeConnectorKindForGrouping(item.AccountSourceKind)
			if scope := connectorScopeLabelForGrouping(item.AccountSourceKind); scope != "" && strings.TrimSpace(item.AccountSourceName) != "" {
				subtitle = scope + ": " + strings.TrimSpace(item.AccountSourceName)
			} else if name := strings.TrimSpace(item.AccountSourceName); name != "" {
				subtitle = name
			}
		}

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

	out := make([]IdentityEntitlementGroup, 0, len(keyOrder))
	for _, key := range keyOrder {
		b := bucketByKey[key]
		sort.SliceStable(b.items, func(i, j int) bool {
			if b.items[i].IsAdmin != b.items[j].IsAdmin {
				return b.items[i].IsAdmin
			}
			if b.items[i].Permission != b.items[j].Permission {
				return strings.ToLower(b.items[i].Permission) < strings.ToLower(b.items[j].Permission)
			}
			if b.items[i].Dormant != b.items[j].Dormant {
				return b.items[i].Dormant
			}
			if b.items[i].LastUsedUnix != b.items[j].LastUsedUnix {
				return b.items[i].LastUsedUnix > b.items[j].LastUsedUnix
			}
			return strings.ToLower(b.items[i].ResourceLabel) < strings.ToLower(b.items[j].ResourceLabel)
		})
		preview, overflow := splitEntitlementPreview(b.items, previewLimit)
		out = append(out, IdentityEntitlementGroup{
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

func splitEntitlementPreview(items []IdentityEntitlementView, limit int) ([]IdentityEntitlementView, []IdentityEntitlementView) {
	if limit <= 0 || len(items) <= limit {
		return items, nil
	}
	return items[:limit], items[limit:]
}

// BuildIdentitySummaryTiles produces the up-to-four metrics shown above the
// access workspace. Each tile is omitted when its source data is empty.
func BuildIdentitySummaryTiles(
	linkedCount int,
	totalEntitlements int,
	adminCount int,
	dormantCount int,
	distinctSources []string,
	distinctSourceKindCount int,
	adminByKind map[string]int,
) []IdentitySummaryTile {
	tiles := make([]IdentitySummaryTile, 0, 4)

	if linkedCount > 0 {
		tiles = append(tiles, IdentitySummaryTile{
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
		tiles = append(tiles, IdentitySummaryTile{
			Label:    "Access grants",
			Value:    strconv.Itoa(totalEntitlements),
			Sublabel: sublabel,
		})
	}

	if adminCount > 0 {
		tiles = append(tiles, IdentitySummaryTile{
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
		tiles = append(tiles, IdentitySummaryTile{
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
	for _, s := range sources {
		label := humanizeConnectorKindForGrouping(s)
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

// shortKindLabel produces a compact label for entitlement kinds used inside
// the admin-scopes sublabel. It is intentionally narrow: anything we don't
// recognise falls back to the raw kind so the breakdown stays informative.
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

// humanizeConnectorKindForGrouping is a small wrapper that maps source kinds
// to their display names. It mirrors views.HumanizeConnectorKind but lives
// here to keep the viewmodel layer free of view-package imports.
func humanizeConnectorKindForGrouping(kind string) string {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "okta":
		return "Okta"
	case "entra":
		return "Microsoft Entra"
	case "github":
		return "GitHub"
	case "aws":
		return "AWS"
	case "datadog":
		return "Datadog"
	case "google_workspace":
		return "Google Workspace"
	case "vault":
		return "Vault"
	case "slack":
		return "Slack"
	case "snowflake":
		return "Snowflake"
	case "pagerduty":
		return "PagerDuty"
	default:
		if t := strings.TrimSpace(kind); t != "" {
			return t
		}
		return "Source"
	}
}

func connectorScopeLabelForGrouping(kind string) string {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "okta":
		return "Domain"
	case "entra":
		return "Tenant"
	case "github":
		return "Org"
	case "aws":
		return "Account"
	case "datadog":
		return "Org"
	case "google_workspace":
		return "Domain"
	case "vault":
		return "Namespace"
	case "slack":
		return "Workspace"
	case "snowflake":
		return "Account"
	case "pagerduty":
		return "Subdomain"
	default:
		return ""
	}
}
