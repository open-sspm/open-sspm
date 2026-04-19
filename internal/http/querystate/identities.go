package querystate

import (
	"net/url"
	"strings"
)

type IdentitiesQuery struct {
	Source         SourceSelection
	Q              string
	IdentityType   string
	ManagedState   string
	PrivilegedOnly bool
	Status         string
	ActivityState  string
	SortBy         string
	SortDir        string
	Page           int
}

func ParseIdentitiesQuery(values url.Values, sources []SourceSelection) IdentitiesQuery {
	source := parseIdentitySourceSelection(values, sources)
	sortBy := normalizeIdentitySortBy(values.Get("sort_by"))
	return IdentitiesQuery{
		Source:         source,
		Q:              strings.TrimSpace(values.Get("q")),
		IdentityType:   normalizeIdentityType(values.Get("identity_type")),
		ManagedState:   normalizeIdentityManagedState(values.Get("managed_state")),
		PrivilegedOnly: parseBool(values.Get("privileged")),
		Status:         normalizeIdentityStatus(values.Get("status")),
		ActivityState:  normalizeIdentityActivityState(values.Get("activity_state")),
		SortBy:         sortBy,
		SortDir:        normalizeIdentitySortDir(values.Get("sort_dir"), sortBy),
		Page:           parsePage(values.Get("page")),
	}
}

func (q IdentitiesQuery) Values() url.Values {
	values := url.Values{}
	setIfNotEmpty(values, "source_kind", normalizeSourceKind(q.Source.Kind))
	setIfNotEmpty(values, "source_name", q.Source.Name)
	setIfNotEmpty(values, "q", q.Q)
	setIfNotEmpty(values, "identity_type", q.IdentityType)
	setIfNotEmpty(values, "managed_state", q.ManagedState)
	setIfTrue(values, "privileged", q.PrivilegedOnly)
	setIfNotEmpty(values, "status", q.Status)
	setIfNotEmpty(values, "activity_state", q.ActivityState)
	setIfNotEmpty(values, "sort_by", q.SortBy)
	if q.SortBy != "" {
		setIfNotEmpty(values, "sort_dir", q.SortDir)
	}
	setIfPage(values, q.Page)
	return values
}

func (q IdentitiesQuery) Href() string {
	return encodeURL("/identities", q.Values())
}

func (q IdentitiesQuery) WithPage(page int) IdentitiesQuery {
	q.Page = page
	if q.Page < 1 {
		q.Page = 1
	}
	return q
}

func (q IdentitiesQuery) ClearQuery() IdentitiesQuery {
	q.Q = ""
	q.Page = 1
	return q
}

func (q IdentitiesQuery) WithActivityState(state string) IdentitiesQuery {
	q.ActivityState = normalizeIdentityActivityState(state)
	q.Page = 1
	return q
}

func (q IdentitiesQuery) WithStatus(status string) IdentitiesQuery {
	q.Status = normalizeIdentityStatus(status)
	q.Page = 1
	return q
}

func (q IdentitiesQuery) WithManagedState(state string) IdentitiesQuery {
	q.ManagedState = normalizeIdentityManagedState(state)
	q.Page = 1
	return q
}

// ClearSegments resets the segment-style filters (activity, status, managed,
// privileged) while preserving source, search, type, and sort. It powers the
// "All" chip on the identities list.
func (q IdentitiesQuery) ClearSegments() IdentitiesQuery {
	q.ActivityState = ""
	q.Status = ""
	q.ManagedState = ""
	q.PrivilegedOnly = false
	q.Page = 1
	return q
}

// SegmentPrivileged returns a query that selects the privileged-only segment
// without leaking any other segment filter into the URL.
func (q IdentitiesQuery) SegmentPrivileged() IdentitiesQuery {
	q = q.ClearSegments()
	q.PrivilegedOnly = true
	return q
}

// HasSegment reports whether any segment-style filter is currently active.
// Used to highlight the "All" chip when no segment is selected.
func (q IdentitiesQuery) HasSegment() bool {
	return q.ActivityState != "" || q.Status != "" || q.ManagedState != "" || q.PrivilegedOnly
}

func (q IdentitiesQuery) TogglePrivilegedOnly() IdentitiesQuery {
	q.PrivilegedOnly = !q.PrivilegedOnly
	q.Page = 1
	return q
}

func (q IdentitiesQuery) WithPrivilegedOnly(enabled bool) IdentitiesQuery {
	q.PrivilegedOnly = enabled
	q.Page = 1
	return q
}

func (q IdentitiesQuery) ClearFilters() IdentitiesQuery {
	q.Source = SourceSelection{}
	q.IdentityType = ""
	q.ManagedState = ""
	q.PrivilegedOnly = false
	q.Status = ""
	q.ActivityState = ""
	q.SortBy = ""
	q.SortDir = ""
	q.Page = 1
	return q
}

func (q IdentitiesQuery) HasFilters() bool {
	return strings.TrimSpace(q.Q) != "" ||
		q.IdentityType != "" ||
		q.ManagedState != "" ||
		q.PrivilegedOnly ||
		q.Status != "" ||
		q.ActivityState != "" ||
		q.Source.Kind != "" ||
		q.Source.Name != ""
}

func (q IdentitiesQuery) FilterCount() int {
	count := 0
	if q.ActivityState != "" {
		count++
	}
	if q.PrivilegedOnly {
		count++
	}
	if q.Status != "" {
		count++
	}
	if q.Source.Kind != "" {
		count++
	}
	if q.Source.Name != "" {
		count++
	}
	if q.IdentityType != "" {
		count++
	}
	if q.ManagedState != "" {
		count++
	}
	if q.SortBy != "" {
		count++
	}
	return count
}

func parseIdentitySourceSelection(values url.Values, sources []SourceSelection) SourceSelection {
	selectedKind := normalizeSourceKind(values.Get("source_kind"))
	selectedName := strings.TrimSpace(values.Get("source_name"))

	if selectedKind != "" && !hasSourceKind(selectedKind, sources) {
		selectedKind = ""
	}

	if selectedKind == "" && selectedName != "" {
		matchedKinds := map[string]struct{}{}
		canonicalName := ""
		for _, source := range sources {
			if !strings.EqualFold(strings.TrimSpace(source.Name), selectedName) {
				continue
			}
			kind := normalizeSourceKind(source.Kind)
			if kind == "" {
				continue
			}
			if canonicalName == "" {
				canonicalName = strings.TrimSpace(source.Name)
			}
			matchedKinds[kind] = struct{}{}
		}
		switch len(matchedKinds) {
		case 0:
			selectedName = ""
		case 1:
			for kind := range matchedKinds {
				selectedKind = kind
			}
			if canonicalName != "" {
				selectedName = canonicalName
			}
		default:
			if canonicalName != "" {
				selectedName = canonicalName
			}
		}
	}

	if selectedKind == "" {
		if selectedName != "" && !identityHasSourceName(selectedName, sources) {
			selectedName = ""
		}
		return SourceSelection{Name: selectedName}
	}

	if selectedName != "" && !identitySourcePairExists(selectedKind, selectedName, sources) {
		selectedName = ""
	}

	return SourceSelection{Kind: selectedKind, Name: selectedName}
}

func identityHasSourceName(name string, sources []SourceSelection) bool {
	name = strings.TrimSpace(name)
	if name == "" {
		return false
	}
	for _, source := range sources {
		if strings.EqualFold(strings.TrimSpace(source.Name), name) {
			return true
		}
	}
	return false
}

func identitySourcePairExists(kind, name string, sources []SourceSelection) bool {
	kind = normalizeSourceKind(kind)
	name = strings.TrimSpace(name)
	if kind == "" || name == "" {
		return false
	}
	for _, source := range sources {
		if normalizeSourceKind(source.Kind) != kind {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(source.Name), name) {
			return true
		}
	}
	return false
}

func normalizeIdentityType(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "human":
		return "human"
	case "service":
		return "service"
	case "bot":
		return "bot"
	case "unknown":
		return "unknown"
	default:
		return ""
	}
}

func normalizeIdentityManagedState(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "managed":
		return "managed"
	case "unmanaged":
		return "unmanaged"
	default:
		return ""
	}
}

func normalizeIdentityStatus(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "active":
		return "active"
	case "suspended":
		return "suspended"
	case "deleted":
		return "deleted"
	case "orphaned":
		return "orphaned"
	case "unknown":
		return "unknown"
	default:
		return ""
	}
}

func normalizeIdentityActivityState(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "recent":
		return "recent"
	case "aging":
		return "aging"
	case "stale":
		return "stale"
	case "never_seen":
		return "never_seen"
	default:
		return ""
	}
}

func normalizeIdentitySortBy(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "identity":
		return "identity"
	case "identity_type":
		return "identity_type"
	case "managed":
		return "managed"
	case "source_type":
		return "source_type"
	case "linked_sources":
		return "linked_sources"
	case "privileged_roles":
		return "privileged_roles"
	case "status":
		return "status"
	case "last_seen":
		return "last_seen"
	default:
		return ""
	}
}

func normalizeIdentitySortDir(raw, sortBy string) string {
	if normalizeIdentitySortBy(sortBy) == "" {
		return ""
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "asc":
		return "asc"
	case "desc":
		return "desc"
	default:
		return "desc"
	}
}
