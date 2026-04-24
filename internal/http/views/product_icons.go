package views

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"sync"
)

type ProductIconView struct {
	Label string
	Slug  string
	Title string
	Hex   string
	Path  string
}

func (i ProductIconView) HasIcon() bool {
	return i.Slug != "" && i.Path != ""
}

func (i ProductIconView) Initial() string {
	return AppInitial(i.Label)
}

func (i ProductIconView) Style() string {
	return "--product-icon-color:#" + sanitizeSimpleIconHex(i.Hex) + ";"
}

func ProductIconFor(label string, hints ...string) ProductIconView {
	fallbackLabel := firstNonEmpty(append([]string{label}, hints...)...)
	if fallbackLabel == "" {
		fallbackLabel = "?"
	}

	index := loadProductIconIndex()
	for _, candidate := range productIconCandidates(label, hints...) {
		if icon, ok := index.lookup[candidate]; ok {
			icon.Label = fallbackLabel
			icon.Path = simpleIconSVGPath(icon.Slug)
			if icon.HasIcon() {
				return icon
			}
		}
	}

	return ProductIconView{Label: fallbackLabel}
}

type productIconIndex struct {
	lookup map[string]ProductIconView
}

type simpleIconEntry struct {
	Title   string            `json:"title"`
	Slug    string            `json:"slug"`
	Hex     string            `json:"hex"`
	Aliases simpleIconAliases `json:"aliases"`
}

type simpleIconAliases struct {
	Aka []string                   `json:"aka"`
	Old []string                   `json:"old"`
	Dup []simpleIconDuplicateAlias `json:"dup"`
	Loc map[string]string          `json:"loc"`
}

type simpleIconDuplicateAlias struct {
	Title string `json:"title"`
}

var (
	productIconIndexOnce sync.Once
	productIconIndexData productIconIndex
	productIconPathCache sync.Map
)

var simpleIconPathRegex = regexp.MustCompile(`(?i)<path\b[^>]*\bd="([^"]+)"[^>]*>`)
var simpleIconSafePathRegex = regexp.MustCompile(`(?i)^m[\-mzlhvcsqtae\d,. ]+$`)

func loadProductIconIndex() productIconIndex {
	productIconIndexOnce.Do(func() {
		productIconIndexData = readProductIconIndex()
	})
	return productIconIndexData
}

func readProductIconIndex() productIconIndex {
	data, ok := readFirstExistingFile(simpleIconsDataPathCandidates())
	if !ok {
		return productIconIndex{lookup: map[string]ProductIconView{}}
	}

	var entries []simpleIconEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return productIconIndex{lookup: map[string]ProductIconView{}}
	}

	lookup := make(map[string]ProductIconView, len(entries)*2)
	for _, entry := range entries {
		title := strings.TrimSpace(entry.Title)
		slug := strings.TrimSpace(entry.Slug)
		if slug == "" {
			slug = simpleIconTitleToSlug(title)
		}
		if slug == "" {
			continue
		}

		icon := ProductIconView{
			Slug:  slug,
			Title: title,
			Hex:   sanitizeSimpleIconHex(entry.Hex),
		}
		addProductIconAlias(lookup, slug, icon)
		addProductIconAlias(lookup, title, icon)
		for _, alias := range entry.Aliases.Aka {
			addProductIconAlias(lookup, alias, icon)
		}
		for _, alias := range entry.Aliases.Old {
			addProductIconAlias(lookup, alias, icon)
		}
		for _, alias := range entry.Aliases.Dup {
			addProductIconAlias(lookup, alias.Title, icon)
		}
		for _, alias := range entry.Aliases.Loc {
			addProductIconAlias(lookup, alias, icon)
		}
	}

	return productIconIndex{lookup: lookup}
}

func addProductIconAlias(lookup map[string]ProductIconView, value string, icon ProductIconView) {
	slug := simpleIconTitleToSlug(value)
	if slug == "" {
		return
	}
	if _, exists := lookup[slug]; !exists {
		lookup[slug] = icon
	}
}

func productIconCandidates(label string, hints ...string) []string {
	texts := append([]string{label}, hints...)
	candidates := make([]string, 0, len(texts)*4)

	addCandidate := func(value string) {
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		if !slices.Contains(candidates, value) {
			candidates = append(candidates, value)
		}
	}

	for _, text := range texts {
		for _, part := range productIconTextParts(text) {
			slug := simpleIconTitleToSlug(part)
			addCandidate(slug)
			addCandidate(productIconAliasSlug(slug))
			for _, domainSlug := range productIconDomainSlugs(part) {
				addCandidate(domainSlug)
				addCandidate(productIconAliasSlug(domainSlug))
			}
		}
	}

	return candidates
}

func productIconTextParts(text string) []string {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}

	parts := []string{text}
	for _, part := range strings.FieldsFunc(text, func(r rune) bool {
		switch r {
		case '|', '•', '·', ',', ';', '(', ')', '[', ']':
			return true
		default:
			return false
		}
	}) {
		part = strings.TrimSpace(part)
		if part != "" && !slices.Contains(parts, part) {
			parts = append(parts, part)
		}
	}
	return parts
}

func productIconAliasSlug(slug string) string {
	switch slug {
	case "googleworkspace", "googleoauth", "googleoauthapp", "googleoauthclient":
		return "google"
	case "hashicorpvault":
		return "vault"
	case "awsidentitycenter", "amazonwebservices":
		return "amazon"
	case "microsoftentra", "entra", "microsoftazure", "azure":
		return "microsoft"
	default:
		return productIconContainedAlias(slug)
	}
}

func productIconContainedAlias(slug string) string {
	for _, alias := range []struct {
		match string
		slug  string
	}{
		{match: "datadog", slug: "datadog"},
		{match: "github", slug: "github"},
		{match: "google", slug: "google"},
		{match: "hashicorp", slug: "hashicorp"},
		{match: "jira", slug: "jira"},
		{match: "okta", slug: "okta"},
		{match: "vault", slug: "vault"},
		{match: "zoom", slug: "zoom"},
	} {
		if strings.Contains(slug, alias.match) {
			return alias.slug
		}
	}
	return ""
}

func productIconDomainSlugs(value string) []string {
	host := strings.ToLower(strings.TrimSpace(value))
	host = strings.TrimPrefix(host, "http://")
	host = strings.TrimPrefix(host, "https://")
	if at := strings.LastIndex(host, "@"); at >= 0 {
		host = host[at+1:]
	}
	if slash := strings.IndexAny(host, "/?#"); slash >= 0 {
		host = host[:slash]
	}
	if colon := strings.Index(host, ":"); colon >= 0 {
		host = host[:colon]
	}
	host = strings.Trim(host, ". ")
	if !strings.Contains(host, ".") {
		return nil
	}

	labels := make([]string, 0, 3)
	for _, part := range strings.Split(host, ".") {
		part = strings.TrimSpace(part)
		if part != "" {
			labels = append(labels, part)
		}
	}
	if len(labels) == 0 {
		return nil
	}

	candidates := make([]string, 0, 3)
	add := func(value string) {
		value = simpleIconTitleToSlug(value)
		if value != "" && !slices.Contains(candidates, value) {
			candidates = append(candidates, value)
		}
	}

	add(host)
	for _, label := range labels {
		switch label {
		case "www", "app", "apps", "admin", "api", "auth", "login", "accounts", "console":
			continue
		default:
			add(label)
		}
	}
	if len(labels) >= 2 {
		add(labels[len(labels)-2])
	}
	return candidates
}

func simpleIconTitleToSlug(title string) string {
	var builder strings.Builder
	for _, r := range strings.ToLower(strings.TrimSpace(title)) {
		switch r {
		case '+':
			builder.WriteString("plus")
		case '.':
			builder.WriteString("dot")
		case '&':
			builder.WriteString("and")
		case 'đ':
			builder.WriteByte('d')
		case 'ħ':
			builder.WriteByte('h')
		case 'ı':
			builder.WriteByte('i')
		case 'ĸ':
			builder.WriteByte('k')
		case 'ŀ', 'ł':
			builder.WriteByte('l')
		case 'ß':
			builder.WriteString("ss")
		case 'ŧ':
			builder.WriteByte('t')
		case 'ø':
			builder.WriteByte('o')
		default:
			if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
				builder.WriteRune(r)
			}
		}
	}
	return builder.String()
}

func simpleIconSVGPath(slug string) string {
	slug = strings.TrimSpace(slug)
	if slug == "" {
		return ""
	}
	if cached, ok := productIconPathCache.Load(slug); ok {
		if pathData, ok := cached.(string); ok {
			return pathData
		}
		return ""
	}

	pathData := readSimpleIconSVGPath(slug)
	productIconPathCache.Store(slug, pathData)
	return pathData
}

func readSimpleIconSVGPath(slug string) string {
	if simpleIconTitleToSlug(slug) != slug {
		return ""
	}

	data, ok := readFirstExistingFile(simpleIconsPathCandidates("web/static/vendor/simple-icons/icons/" + slug + ".svg"))
	if !ok {
		return ""
	}

	match := simpleIconPathRegex.FindSubmatch(data)
	if len(match) != 2 {
		return ""
	}

	pathData := strings.TrimSpace(string(match[1]))
	if !simpleIconSafePathRegex.MatchString(pathData) {
		return ""
	}
	return pathData
}

func sanitizeSimpleIconHex(hex string) string {
	hex = strings.TrimPrefix(strings.TrimSpace(hex), "#")
	if len(hex) == 3 {
		hex = string([]byte{hex[0], hex[0], hex[1], hex[1], hex[2], hex[2]})
	}
	if len(hex) != 6 {
		return "71717A"
	}
	for _, r := range hex {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')) {
			return "71717A"
		}
	}
	return strings.ToUpper(hex)
}

func simpleIconsDataPathCandidates() []string {
	return simpleIconsPathCandidates("web/static/vendor/simple-icons/simple-icons.json")
}

func simpleIconsPathCandidates(rel string) []string {
	candidates := make([]string, 0, 12)
	if wd, err := os.Getwd(); err == nil {
		candidates = append(candidates, upwardPathCandidates(wd, rel)...)
	}
	if _, file, _, ok := runtime.Caller(0); ok {
		candidates = append(candidates, upwardPathCandidates(filepath.Dir(file), rel)...)
	}
	return dedupeStrings(candidates)
}

func upwardPathCandidates(start, rel string) []string {
	start = filepath.Clean(start)
	out := []string{}
	for {
		out = append(out, filepath.Join(start, rel))
		parent := filepath.Dir(start)
		if parent == start {
			return out
		}
		start = parent
	}
}

func readFirstExistingFile(paths []string) ([]byte, bool) {
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err == nil {
			return data, true
		}
	}
	return nil, false
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func dedupeStrings(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value != "" && !slices.Contains(out, value) {
			out = append(out, value)
		}
	}
	return out
}
