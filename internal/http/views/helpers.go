package views

import (
	"net/url"
	"strconv"
	"strings"
	"unicode"
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

func IdentitiesListURL(sourceKind, sourceName, query, identityType, managedState string, privilegedOnly bool, status, activityState, linkQuality, sortBy, sortDir string, showFirstSeen, showLinkQuality, showLinkReason bool, page int) string {
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
	if identityType = strings.TrimSpace(identityType); identityType != "" {
		values.Set("identity_type", identityType)
	}
	if managedState = strings.TrimSpace(managedState); managedState != "" {
		values.Set("managed_state", managedState)
	}
	if privilegedOnly {
		values.Set("privileged", "1")
	}
	if status = strings.TrimSpace(status); status != "" {
		values.Set("status", status)
	}
	if activityState = strings.TrimSpace(activityState); activityState != "" {
		values.Set("activity_state", activityState)
	}
	if linkQuality = strings.TrimSpace(linkQuality); linkQuality != "" {
		values.Set("link_quality", linkQuality)
	}
	if sortBy = strings.TrimSpace(sortBy); sortBy != "" {
		values.Set("sort_by", sortBy)
	}
	if sortDir = strings.TrimSpace(sortDir); sortDir != "" {
		values.Set("sort_dir", sortDir)
	}
	if showFirstSeen {
		values.Set("show_first_seen", "1")
	}
	if showLinkQuality {
		values.Set("show_link_quality", "1")
	}
	if showLinkReason {
		values.Set("show_link_reason", "1")
	}
	if page > 1 {
		values.Set("page", strconv.Itoa(page))
	}
	if len(values) == 0 {
		return "/identities"
	}
	return "/identities?" + values.Encode()
}

func DiscoveryAppsListURL(sourceKind, sourceName, query, managedState, riskLevel string, page int) string {
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
	if managedState = strings.TrimSpace(managedState); managedState != "" {
		values.Set("managed_state", managedState)
	}
	if riskLevel = strings.TrimSpace(riskLevel); riskLevel != "" {
		values.Set("risk_level", riskLevel)
	}
	if page > 1 {
		values.Set("page", strconv.Itoa(page))
	}
	if len(values) == 0 {
		return "/discovery/apps"
	}
	return "/discovery/apps?" + values.Encode()
}

func DiscoveryHotspotsURL(sourceKind, sourceName string) string {
	values := url.Values{}
	if sourceKind = strings.TrimSpace(sourceKind); sourceKind != "" {
		values.Set("source_kind", sourceKind)
	}
	if sourceName = strings.TrimSpace(sourceName); sourceName != "" {
		values.Set("source_name", sourceName)
	}
	if len(values) == 0 {
		return "/discovery/hotspots"
	}
	return "/discovery/hotspots?" + values.Encode()
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

func DiscoveryManagedBadgeClass(state string) string {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "managed":
		return "badge bg-emerald-100 text-emerald-800 dark:bg-emerald-900/50 dark:text-emerald-100"
	case "unmanaged":
		return "badge bg-rose-100 text-rose-800 dark:bg-rose-900/50 dark:text-rose-100"
	default:
		return "badge-outline"
	}
}

func HumanizeDiscoveryManagedState(state string) string {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "managed":
		return "Managed"
	case "unmanaged":
		return "Unmanaged"
	default:
		return fallbackHumanized(state)
	}
}

func HumanizeDiscoveryManagedReason(reason string) string {
	switch strings.ToLower(strings.TrimSpace(reason)) {
	case "active_binding_fresh_sync":
		return "Primary binding has fresh sync"
	case "no_binding":
		return "No primary binding"
	case "connector_disabled":
		return "Bound connector is disabled"
	case "connector_not_configured":
		return "Bound connector is not configured"
	case "stale_sync":
		return "Bound connector sync is stale"
	default:
		return fallbackHumanized(reason)
	}
}

func HumanizeDiscoverySignalKind(kind string) string {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "idp_sso":
		return "IdP SSO"
	case "oauth_grant":
		return "OAuth grant"
	default:
		return fallbackHumanized(kind)
	}
}

func HumanizeBusinessCriticality(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "unknown":
		return "Unknown"
	case "low":
		return "Low"
	case "medium":
		return "Medium"
	case "high":
		return "High"
	case "critical":
		return "Critical"
	default:
		return fallbackHumanized(value)
	}
}

func HumanizeDataClassification(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "unknown":
		return "Unknown"
	case "public":
		return "Public"
	case "internal":
		return "Internal"
	case "confidential":
		return "Confidential"
	case "restricted":
		return "Restricted"
	default:
		return fallbackHumanized(value)
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

func HumanizeIdentityType(identityType string) string {
	switch strings.ToLower(strings.TrimSpace(identityType)) {
	case "human":
		return "Human"
	case "service":
		return "Service"
	case "bot":
		return "Bot"
	case "unknown":
		return "Unknown"
	default:
		return fallbackHumanized(identityType)
	}
}

func HumanizeIdentityStatus(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "active":
		return "Active"
	case "suspended":
		return "Suspended"
	case "deleted":
		return "Deleted"
	case "orphaned":
		return "Orphaned"
	case "unknown":
		return "Unknown"
	default:
		return fallbackHumanized(status)
	}
}

func HumanizeIdentityLinkQuality(linkQuality string) string {
	switch strings.ToLower(strings.TrimSpace(linkQuality)) {
	case "high":
		return "High"
	case "medium":
		return "Medium"
	case "low":
		return "Low"
	case "unknown":
		return "Unknown"
	default:
		return fallbackHumanized(linkQuality)
	}
}

func FormatIdentityLinkConfidence(value float32) string {
	percent := float64(value) * 100
	if percent < 0 {
		percent = 0
	}
	if percent > 100 {
		percent = 100
	}
	return strconv.FormatFloat(percent, 'f', 0, 64) + "%"
}

func IdentityInventoryColSpan(showFirstSeen, showLinkQuality, showLinkReason bool) string {
	cols := 9
	if showFirstSeen {
		cols++
	}
	if showLinkQuality {
		cols++
	}
	if showLinkReason {
		cols++
	}
	return strconv.Itoa(cols)
}

func HumanizeIdentityRowState(state string) string {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "action_required":
		return "Action required"
	case "review":
		return "Review"
	case "healthy":
		return "Healthy"
	default:
		return fallbackHumanized(state)
	}
}

func IdentityManagedBadgeClass(managed bool) string {
	if managed {
		return "badge bg-emerald-100 text-emerald-800 dark:bg-emerald-900/50 dark:text-emerald-100"
	}
	return "badge bg-amber-100 text-amber-800 dark:bg-amber-900/50 dark:text-amber-100"
}

func IdentityStatusBadgeClass(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "active":
		return "badge bg-emerald-100 text-emerald-800 dark:bg-emerald-900/50 dark:text-emerald-100"
	case "suspended":
		return "badge bg-amber-100 text-amber-800 dark:bg-amber-900/50 dark:text-amber-100"
	case "deleted":
		return "badge bg-rose-100 text-rose-800 dark:bg-rose-900/50 dark:text-rose-100"
	case "orphaned":
		return "badge bg-slate-100 text-slate-800 dark:bg-slate-900/50 dark:text-slate-100"
	default:
		return "badge-outline"
	}
}

func IdentityRowStateBadgeClass(state string) string {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "action_required":
		return "badge bg-rose-100 text-rose-800 dark:bg-rose-900/50 dark:text-rose-100"
	case "review":
		return "badge bg-amber-100 text-amber-800 dark:bg-amber-900/50 dark:text-amber-100"
	case "healthy":
		return "badge bg-emerald-100 text-emerald-800 dark:bg-emerald-900/50 dark:text-emerald-100"
	default:
		return "badge-outline"
	}
}

func IdentityLinkQualityBadgeClass(linkQuality string) string {
	switch strings.ToLower(strings.TrimSpace(linkQuality)) {
	case "high":
		return "badge bg-emerald-100 text-emerald-800 dark:bg-emerald-900/50 dark:text-emerald-100"
	case "medium":
		return "badge bg-sky-100 text-sky-800 dark:bg-sky-900/50 dark:text-sky-100"
	case "low":
		return "badge bg-amber-100 text-amber-800 dark:bg-amber-900/50 dark:text-amber-100"
	default:
		return "badge-outline"
	}
}

func HumanizeCredentialKind(kind string) string {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "entra_client_secret":
		return "Entra client secret"
	case "entra_certificate":
		return "Entra certificate"
	case "github_deploy_key":
		return "GitHub deploy key"
	case "github_pat_request":
		return "GitHub PAT request"
	case "github_pat_fine_grained":
		return "GitHub fine-grained PAT"
	default:
		return fallbackHumanized(kind)
	}
}

func ShortIdentifier(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "—"
	}
	if value == "—" {
		return value
	}

	runes := []rune(value)
	if len(runes) <= 18 {
		return value
	}

	return string(runes[:8]) + "..." + string(runes[len(runes)-6:])
}

func CopyableValue(value string) bool {
	value = strings.TrimSpace(value)
	return value != "" && value != "—"
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

func fallbackHumanized(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "—"
	}
	parts := strings.FieldsFunc(strings.ToLower(value), func(r rune) bool {
		return r == '_' || r == ':' || r == '-'
	})
	for idx, part := range parts {
		if part == "" {
			continue
		}
		runes := []rune(part)
		if len(runes) == 0 {
			continue
		}
		runes[0] = unicode.ToUpper(runes[0])
		parts[idx] = string(runes)
	}
	if len(parts) == 0 {
		return value
	}
	return strings.Join(parts, " ")
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
