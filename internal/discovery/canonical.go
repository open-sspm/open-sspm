package discovery

import (
	"net"
	"net/url"
	"regexp"
	"strings"

	"golang.org/x/net/publicsuffix"
)

var (
	nonKeyNameChars      = regexp.MustCompile(`[^a-z0-9]+`)
	nonCategoryNameChars = regexp.MustCompile(`[^a-z0-9_]+`)
)

type appCategoryHint struct {
	category string
	domains  []string
	names    []string
}

var appCategoryHints = []appCategoryHint{
	{
		category: "developer_tools",
		domains:  []string{"github.com", "gitlab.com", "bitbucket.org", "atlassian.com"},
		names:    []string{"github", "gitlab", "bitbucket", "jira", "confluence", "atlassian"},
	},
	{
		category: "collaboration",
		domains:  []string{"slack.com", "zoom.us", "notion.so", "miro.com", "figma.com"},
		names:    []string{"slack", "microsoft teams", "zoom", "notion", "miro", "figma"},
	},
	{
		category: "finance",
		domains:  []string{"intuit.com", "quickbooks.intuit.com", "bill.com", "stripe.com", "brex.com", "ramp.com"},
		names:    []string{"quickbooks", "intuit", "bill.com", "stripe", "brex", "ramp"},
	},
	{
		category: "hr",
		domains:  []string{"workday.com", "bamboohr.com", "gusto.com", "greenhouse.io", "lever.co"},
		names:    []string{"workday", "bamboohr", "gusto", "greenhouse", "lever"},
	},
	{
		category: "sales",
		domains:  []string{"salesforce.com", "hubspot.com", "zendesk.com", "intercom.com"},
		names:    []string{"salesforce", "hubspot", "zendesk", "intercom"},
	},
	{
		category: "identity_security",
		domains:  []string{"okta.com", "duo.com", "onelogin.com", "1password.com", "lastpass.com"},
		names:    []string{"okta", "duo", "onelogin", "1password", "lastpass"},
	},
	{
		category: "cloud_infrastructure",
		domains:  []string{"amazonaws.com", "azure.com"},
		names:    []string{"aws", "amazon web services", "google cloud", "azure"},
	},
	{
		category: "data_analytics",
		domains:  []string{"snowflake.com", "looker.com", "tableau.com", "databricks.com"},
		names:    []string{"snowflake", "looker", "tableau", "databricks"},
	},
}

// BuildMetadata returns canonical metadata for discovery rows.
func BuildMetadata(input CanonicalInput) AppMetadata {
	domain := normalizeDomain(input.SourceDomain)
	canonical := CanonicalKey(input)
	display := strings.TrimSpace(input.SourceAppName)
	if display == "" {
		display = strings.TrimSpace(input.SourceAppID)
	}
	if display == "" {
		display = "Unknown app"
	}
	vendorName := inferVendorName(strings.TrimSpace(input.SourceVendorName), domain)
	category := normalizeAppCategory(input.SourceCategory)
	if category == "" {
		category = inferAppCategory(input, domain, vendorName)
	}
	return AppMetadata{
		CanonicalKey: canonical,
		DisplayName:  display,
		Domain:       domain,
		VendorName:   vendorName,
		Category:     category,
	}
}

// CanonicalKey returns a deterministic MVP key for SaaS app identity.
func CanonicalKey(input CanonicalInput) string {
	sourceKind := strings.ToLower(strings.TrimSpace(input.SourceKind))
	sourceName := strings.ToLower(strings.TrimSpace(input.SourceName))
	sourceAppID := strings.TrimSpace(input.SourceAppID)
	if domain := normalizeDomain(input.SourceDomain); domain != "" {
		return "domain:" + domain
	}

	if sourceKind == "entra" {
		if entraAppID := normalizeGUIDLike(input.EntraAppID); entraAppID != "" {
			return "entra_appid:" + entraAppID
		}
		if sourceAppID := normalizeGUIDLike(sourceAppID); sourceAppID != "" {
			return "entra_appid:" + sourceAppID
		}
	}

	if sourceKind == "okta" && sourceAppID != "" {
		if sourceName == "" {
			sourceName = "okta"
		}
		return "okta_app:" + sourceName + ":" + strings.ToLower(sourceAppID)
	}

	name := normalizeKeyName(input.SourceAppName)
	if name == "" {
		name = normalizeKeyName(sourceAppID)
	}
	if name == "" {
		name = "unknown"
	}
	if sourceKind == "" {
		sourceKind = "unknown"
	}
	return "name:" + name + ":" + sourceKind
}

func normalizeDomain(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}

	candidate := raw
	if !strings.Contains(candidate, "://") {
		candidate = "https://" + candidate
	}
	u, err := url.Parse(candidate)
	if err == nil && u.Host != "" {
		candidate = u.Host
	}
	candidate = strings.TrimSpace(candidate)
	candidate = strings.TrimPrefix(candidate, "*.")
	candidate = strings.Trim(candidate, ".")
	candidate = strings.TrimSuffix(candidate, ":443")
	candidate = strings.TrimSuffix(candidate, ":80")
	candidate = strings.ToLower(candidate)
	if ip := net.ParseIP(candidate); ip != nil {
		return ""
	}
	if after, ok := strings.CutPrefix(candidate, "www."); ok {
		candidate = after
	}
	eTLD, err := publicsuffix.EffectiveTLDPlusOne(candidate)
	if err != nil {
		return candidate
	}
	return strings.ToLower(strings.TrimSpace(eTLD))
}

func normalizeGUIDLike(raw string) string {
	raw = strings.TrimSpace(strings.ToLower(raw))
	raw = strings.TrimPrefix(raw, "{")
	raw = strings.TrimSuffix(raw, "}")
	if raw == "" {
		return ""
	}
	// Keep conservative: only GUID-like tokens are treated as app IDs.
	if len(raw) != 36 {
		return ""
	}
	for i, r := range raw {
		switch {
		case r >= '0' && r <= '9':
		case r >= 'a' && r <= 'f':
		case r == '-' && (i == 8 || i == 13 || i == 18 || i == 23):
		default:
			return ""
		}
	}
	return raw
}

func normalizeKeyName(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" {
		return ""
	}
	raw = nonKeyNameChars.ReplaceAllString(raw, "-")
	raw = strings.Trim(raw, "-")
	if raw == "" {
		return ""
	}
	if len(raw) > 64 {
		return raw[:64]
	}
	return raw
}

func inferVendorName(sourceVendorName, domain string) string {
	sourceVendorName = strings.TrimSpace(sourceVendorName)
	if sourceVendorName != "" {
		return sourceVendorName
	}
	return VendorLabelFromDomain(domain)
}

func inferAppCategory(input CanonicalInput, domain, vendorName string) string {
	for _, hint := range appCategoryHints {
		for _, candidate := range hint.domains {
			candidate = normalizeDomain(candidate)
			if domain != "" && candidate != "" && domain == candidate {
				return hint.category
			}
		}
	}

	text := categoryMatchText(input.SourceAppName, input.SourceVendorName, vendorName, input.SourceAppID)
	if text == "" {
		return ""
	}
	for _, hint := range appCategoryHints {
		for _, name := range hint.names {
			if categoryTextContains(text, name) {
				return hint.category
			}
		}
	}
	return ""
}

func normalizeAppCategory(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" {
		return ""
	}
	raw = strings.ReplaceAll(raw, "-", "_")
	raw = nonCategoryNameChars.ReplaceAllString(raw, "_")
	raw = strings.Trim(raw, "_")
	for strings.Contains(raw, "__") {
		raw = strings.ReplaceAll(raw, "__", "_")
	}
	return raw
}

func categoryMatchText(parts ...string) string {
	normalized := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.ToLower(strings.TrimSpace(part))
		if part == "" {
			continue
		}
		part = strings.ReplaceAll(part, "_", " ")
		part = nonKeyNameChars.ReplaceAllString(part, " ")
		part = strings.TrimSpace(part)
		if part != "" {
			normalized = append(normalized, part)
		}
	}
	return strings.Join(normalized, " ")
}

func categoryTextContains(text, name string) bool {
	name = categoryMatchText(name)
	if name == "" {
		return false
	}
	return strings.Contains(" "+text+" ", " "+name+" ")
}

// VendorLabelFromDomain derives a human-readable vendor label from a domain-like
// input (URL/host/domain). It returns an empty string when no label can be derived.
func VendorLabelFromDomain(raw string) string {
	domain := strings.ToLower(strings.TrimSpace(raw))
	if domain == "" {
		return ""
	}
	if idx := strings.Index(domain, "://"); idx >= 0 {
		domain = domain[idx+3:]
	}
	if idx := strings.Index(domain, "/"); idx >= 0 {
		domain = domain[:idx]
	}
	domain = strings.TrimPrefix(domain, "www.")
	domain = strings.Trim(domain, ".")
	if domain == "" {
		return ""
	}
	part := domain
	if idx := strings.Index(part, "."); idx > 0 {
		part = part[:idx]
	}
	part = strings.ReplaceAll(part, "-", " ")
	part = strings.TrimSpace(part)
	if part == "" {
		return ""
	}
	return strings.ToUpper(part[:1]) + part[1:]
}
