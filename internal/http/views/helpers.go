package views

import (
	"net/url"
	"strconv"
	"strings"
)

func FormatInt(v int) string {
	return strconv.Itoa(v)
}

func FormatInt64(v int64) string {
	return strconv.FormatInt(v, 10)
}

func QueryEscape(v string) string {
	return url.QueryEscape(v)
}

func AppAssetsListURL(sourceKind, sourceName, query, assetKind string, page int) string {
	values := url.Values{}
	if sourceKind = strings.TrimSpace(sourceKind); sourceKind != "" {
		values.Set("source_kind", sourceKind)
	}
	if sourceName = strings.TrimSpace(sourceName); sourceName != "" {
		values.Set("source_name", sourceName)
	}
	if query = strings.TrimSpace(query); query != "" {
		values.Set("q", query)
	}
	if assetKind = strings.TrimSpace(assetKind); assetKind != "" {
		values.Set("asset_kind", assetKind)
	}
	if page > 1 {
		values.Set("page", strconv.Itoa(page))
	}
	if len(values) == 0 {
		return "/app-assets"
	}
	return "/app-assets?" + values.Encode()
}

func AppDetailURL(integratedHref, externalID string) string {
	if integratedHref = strings.TrimSpace(integratedHref); integratedHref != "" {
		return integratedHref
	}
	if externalID = strings.TrimSpace(externalID); externalID != "" {
		return "/apps/" + externalID
	}
	return "/apps"
}

func CredentialsListURL(sourceKind, sourceName, query, credentialKind, status, riskLevel, expiryState string, expiresInDays int, page int) string {
	values := url.Values{}
	if sourceKind = strings.TrimSpace(sourceKind); sourceKind != "" {
		values.Set("source_kind", sourceKind)
	}
	if sourceName = strings.TrimSpace(sourceName); sourceName != "" {
		values.Set("source_name", sourceName)
	}
	if query = strings.TrimSpace(query); query != "" {
		values.Set("q", query)
	}
	if credentialKind = strings.TrimSpace(credentialKind); credentialKind != "" {
		values.Set("credential_kind", credentialKind)
	}
	if status = strings.TrimSpace(status); status != "" {
		values.Set("status", status)
	}
	if riskLevel = strings.TrimSpace(riskLevel); riskLevel != "" {
		values.Set("risk_level", riskLevel)
	}
	if expiryState = strings.TrimSpace(expiryState); expiryState != "" {
		values.Set("expiry_state", expiryState)
	}
	if expiresInDays > 0 {
		values.Set("expires_in_days", strconv.Itoa(expiresInDays))
	}
	if page > 1 {
		values.Set("page", strconv.Itoa(page))
	}
	if len(values) == 0 {
		return "/credentials"
	}
	return "/credentials?" + values.Encode()
}

func HumanizeProgrammaticKind(kind string) string {
	kind = strings.TrimSpace(kind)
	if kind == "" {
		return "—"
	}

	parts := strings.FieldsFunc(strings.ToLower(kind), func(r rune) bool {
		return r == '_' || r == ':' || r == '-'
	})
	for idx, part := range parts {
		if part == "" {
			continue
		}
		parts[idx] = strings.ToUpper(part[:1]) + part[1:]
	}
	if len(parts) == 0 {
		return kind
	}
	return strings.Join(parts, " ")
}

func ListURL(baseHref string, query string, state string, page int) string {
	query = strings.TrimSpace(query)
	state = strings.TrimSpace(state)

	values := url.Values{}
	if query != "" {
		values.Set("q", query)
	}
	if state != "" {
		values.Set("state", state)
	}
	if page > 1 {
		values.Set("page", strconv.Itoa(page))
	}
	if len(values) == 0 {
		return baseHref
	}
	return baseHref + "?" + values.Encode()
}

func StatusBadgeClass(status string) string {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "ACTIVE":
		return "badge bg-emerald-100 text-emerald-800 dark:bg-emerald-900/50 dark:text-emerald-100"
	case "SUSPENDED", "INACTIVE":
		return "badge bg-amber-100 text-amber-800 dark:bg-amber-900/50 dark:text-amber-100"
	default:
		return "badge-outline"
	}
}

func CredentialRiskBadgeClass(risk string) string {
	switch strings.ToLower(strings.TrimSpace(risk)) {
	case "critical":
		return "badge bg-rose-100 text-rose-800 dark:bg-rose-900/50 dark:text-rose-100"
	case "high":
		return "badge bg-amber-100 text-amber-800 dark:bg-amber-900/50 dark:text-amber-100"
	case "medium":
		return "badge bg-sky-100 text-sky-800 dark:bg-sky-900/50 dark:text-sky-100"
	case "low":
		return "badge bg-emerald-100 text-emerald-800 dark:bg-emerald-900/50 dark:text-emerald-100"
	default:
		return "badge-outline"
	}
}

func HumanizeCredentialRisk(risk string) string {
	switch strings.ToLower(strings.TrimSpace(risk)) {
	case "critical":
		return "Critical"
	case "high":
		return "High"
	case "medium":
		return "Medium"
	case "low":
		return "Low"
	default:
		risk = strings.TrimSpace(risk)
		if risk == "" {
			return "—"
		}
		return risk
	}
}

func RuleStatusBadgeClass(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "pass":
		return "badge bg-emerald-100 text-emerald-800 dark:bg-emerald-900/50 dark:text-emerald-100"
	case "fail":
		return "badge bg-rose-100 text-rose-800 dark:bg-rose-900/50 dark:text-rose-100"
	case "error":
		return "badge bg-amber-100 text-amber-800 dark:bg-amber-900/50 dark:text-amber-100"
	case "not_applicable":
		return "badge bg-slate-100 text-slate-800 dark:bg-slate-900/50 dark:text-slate-100"
	default:
		return "badge-outline"
	}
}

func RuleMonitoringBadgeClass(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "automated":
		return "badge bg-sky-100 text-sky-800 dark:bg-sky-900/50 dark:text-sky-100"
	case "partial":
		return "badge bg-amber-100 text-amber-800 dark:bg-amber-900/50 dark:text-amber-100"
	case "manual":
		return "badge bg-slate-100 text-slate-800 dark:bg-slate-900/50 dark:text-slate-100"
	default:
		return "badge-outline"
	}
}

func HumanizeRuleStatus(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "pass":
		return "Pass"
	case "fail":
		return "Fail"
	case "unknown":
		return "Unknown"
	case "not_applicable":
		return "Not applicable"
	case "error":
		return "Error"
	default:
		return strings.TrimSpace(status)
	}
}

func IsAlertDestructive(class string) bool {
	class = strings.ToLower(strings.TrimSpace(class))
	if class == "" {
		return false
	}
	return strings.Contains(class, "error") || strings.Contains(class, "destructive")
}

func AlertRole(destructive bool) string {
	if destructive {
		return "alert"
	}
	return "status"
}

func AlertAriaLive(destructive bool) string {
	if destructive {
		return "assertive"
	}
	return "polite"
}

func IsActivePath(activePath, target string) bool {
	activePath = strings.TrimSpace(activePath)
	target = strings.TrimSpace(target)
	if target == "/" {
		return activePath == "/"
	}
	return strings.HasPrefix(activePath, target)
}

func AriaCurrent(activePath, target string) string {
	if IsActivePath(activePath, target) {
		return "page"
	}
	return ""
}

func AriaCurrentProgrammatic(activePath string) string {
	if IsActivePath(activePath, "/app-assets") || IsActivePath(activePath, "/credentials") {
		return "page"
	}
	return ""
}

func AriaCurrentExact(activePath, target string) string {
	activePath = strings.TrimSpace(activePath)
	target = strings.TrimSpace(target)
	if activePath == target {
		return "page"
	}
	return ""
}

func HumanizeAuthUserRole(role string) string {
	switch strings.ToLower(strings.TrimSpace(role)) {
	case "admin":
		return "Admin"
	case "viewer":
		return "Viewer"
	default:
		role = strings.TrimSpace(role)
		if role == "" {
			return "—"
		}
		return role
	}
}

func AuthUserRoleBadgeClass(role string) string {
	switch strings.ToLower(strings.TrimSpace(role)) {
	case "admin":
		return "badge bg-sky-100 text-sky-800 dark:bg-sky-900/50 dark:text-sky-100"
	case "viewer":
		return "badge bg-slate-100 text-slate-800 dark:bg-slate-900/50 dark:text-slate-100"
	default:
		return "badge-outline"
	}
}

func AuthUserStatusLabel(active bool) string {
	if active {
		return "Active"
	}
	return "Disabled"
}

func AuthUserStatusBadgeClass(active bool) string {
	if active {
		return "badge bg-emerald-100 text-emerald-800 dark:bg-emerald-900/50 dark:text-emerald-100"
	}
	return "badge bg-amber-100 text-amber-800 dark:bg-amber-900/50 dark:text-amber-100"
}
