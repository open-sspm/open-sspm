package discovery

import (
	"net"
	"net/url"
	"regexp"
	"strings"

	"golang.org/x/net/publicsuffix"
)

var nonKeyNameChars = regexp.MustCompile(`[^a-z0-9]+`)

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
	return AppMetadata{
		CanonicalKey: canonical,
		DisplayName:  display,
		Domain:       domain,
		VendorName:   inferVendorName(domain, display),
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

func inferVendorName(domain, display string) string {
	if domain != "" {
		part := domain
		if idx := strings.Index(part, "."); idx > 0 {
			part = part[:idx]
		}
		part = strings.ReplaceAll(part, "-", " ")
		part = strings.TrimSpace(part)
		if part != "" {
			return strings.ToUpper(part[:1]) + part[1:]
		}
	}
	display = strings.TrimSpace(display)
	if display == "" {
		return ""
	}
	if len(display) > 80 {
		display = display[:80]
	}
	return display
}
