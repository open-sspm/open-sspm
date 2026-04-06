package views

import (
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

func FilterPillClass(active bool) string {
	if active {
		return "inline-flex items-center rounded-md bg-muted px-2.5 py-1 text-xs font-medium text-foreground no-underline"
	}
	return "inline-flex items-center rounded-md px-2.5 py-1 text-xs text-muted-foreground no-underline hover:bg-muted/60 hover:text-foreground"
}

func IdentityManagedTextClass(managed bool) string {
	if managed {
		return "text-sm font-medium text-emerald-700 dark:text-emerald-300"
	}
	return "text-sm font-medium text-amber-700 dark:text-amber-300"
}

func AppDetailURL(integratedHref, externalID string) string {
	if integratedHref = strings.TrimSpace(integratedHref); integratedHref != "" {
		return integratedHref
	}
	if externalID = strings.TrimSpace(externalID); externalID != "" {
		return "/assigned-apps/" + externalID
	}
	return "/assigned-apps"
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

func HumanizeConnectorKind(kind string) string {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "okta":
		return "Okta"
	case "google_workspace":
		return "Google Workspace"
	case "github":
		return "GitHub"
	case "datadog":
		return "Datadog"
	case "aws", "aws_identity_center":
		return "AWS Identity Center"
	case "entra":
		return "Microsoft Entra"
	case "vault":
		return "Vault"
	default:
		return fallbackHumanized(kind)
	}
}

func ConnectorScopeLabel(kind string) string {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "okta":
		return "Org URL"
	case "google_workspace":
		return "Customer"
	case "github":
		return "Org"
	case "datadog":
		return "Site"
	case "aws", "aws_identity_center":
		return "Instance"
	case "entra":
		return "Tenant"
	case "vault":
		return "Vault"
	default:
		return "Source"
	}
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

func RiskScoreClass(riskLevel string) string {
	switch strings.ToLower(strings.TrimSpace(riskLevel)) {
	case "critical":
		return "text-xs font-medium text-rose-700 dark:text-rose-400"
	case "high":
		return "text-xs font-medium text-amber-700 dark:text-amber-400"
	default:
		return "text-xs text-muted-foreground"
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

func AppAssetGovernanceStateBadgeClass(state string) string {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "action_required":
		return "badge bg-rose-100 text-rose-800 dark:bg-rose-900/50 dark:text-rose-100"
	case "in_review":
		return "badge bg-amber-100 text-amber-800 dark:bg-amber-900/50 dark:text-amber-100"
	case "approved":
		return "badge bg-emerald-100 text-emerald-800 dark:bg-emerald-900/50 dark:text-emerald-100"
	case "ticketed":
		return "badge bg-sky-100 text-sky-800 dark:bg-sky-900/50 dark:text-sky-100"
	case "unreviewed":
		return "badge bg-slate-100 text-slate-800 dark:bg-slate-900/50 dark:text-slate-100"
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

func HumanizeAppAssetGovernanceState(state string) string {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "unreviewed":
		return "Unreviewed"
	case "in_review":
		return "In Review"
	case "approved":
		return "Approved"
	case "action_required":
		return "Action Required"
	case "ticketed":
		return "Ticketed"
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

func AppAssetEvidenceFreshnessBadgeClass(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "fresh":
		return "badge bg-emerald-100 text-emerald-800 dark:bg-emerald-900/50 dark:text-emerald-100"
	case "aging":
		return "badge bg-amber-100 text-amber-800 dark:bg-amber-900/50 dark:text-amber-100"
	case "stale":
		return "badge bg-rose-100 text-rose-800 dark:bg-rose-900/50 dark:text-rose-100"
	default:
		return "badge-outline"
	}
}

func HumanizeAppAssetEvidenceFreshness(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "fresh":
		return "Fresh"
	case "aging":
		return "Aging"
	case "stale":
		return "Stale"
	case "unknown":
		return "Unknown"
	default:
		return fallbackHumanized(value)
	}
}

func AppAssetEvidenceConfidenceBadgeClass(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
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

func HumanizeAppAssetEvidenceConfidence(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "high":
		return "High confidence"
	case "medium":
		return "Medium confidence"
	case "low":
		return "Low confidence"
	default:
		return fallbackHumanized(value)
	}
}

func ConnectedAppReviewStateBadgeClass(state string) string {
	return AppAssetGovernanceStateBadgeClass(state)
}

func HumanizeConnectedAppReviewState(state string) string {
	return HumanizeAppAssetGovernanceState(state)
}

func ConnectedAppFreshnessBadgeClass(value string) string {
	return AppAssetEvidenceFreshnessBadgeClass(value)
}

func HumanizeConnectedAppFreshness(value string) string {
	return HumanizeAppAssetEvidenceFreshness(value)
}

func ConnectedAppConfidenceBadgeClass(value string) string {
	return AppAssetEvidenceConfidenceBadgeClass(value)
}

func HumanizeConnectedAppConfidence(value string) string {
	return HumanizeAppAssetEvidenceConfidence(value)
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

func HumanizeAccountStatus(status string) string {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "ACTIVE":
		return "Active"
	case "SUSPENDED":
		return "Suspended"
	case "INACTIVE":
		return "Inactive"
	case "DEPROVISIONED":
		return "Deprovisioned"
	case "DISABLED":
		return "Disabled"
	default:
		return fallbackHumanized(status)
	}
}

func AccountStatusBadgeClass(status string) string {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "ACTIVE":
		return "badge bg-emerald-100 text-emerald-800 dark:bg-emerald-900/50 dark:text-emerald-100"
	case "SUSPENDED", "INACTIVE", "DISABLED":
		return "badge bg-amber-100 text-amber-800 dark:bg-amber-900/50 dark:text-amber-100"
	case "DEPROVISIONED":
		return "badge bg-rose-100 text-rose-800 dark:bg-rose-900/50 dark:text-rose-100"
	default:
		return "badge-outline"
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

func IdentityRowClass(state string) string {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "action_required":
		return "cursor-pointer hover:bg-muted/50 border-l-2 border-l-rose-500"
	case "review":
		return "cursor-pointer hover:bg-muted/50 border-l-2 border-l-amber-400"
	default:
		return "cursor-pointer hover:bg-muted/50"
	}
}

func IdentityPrivilegedRoleClass(count int64) string {
	switch {
	case count >= 20:
		return "font-semibold text-rose-700 dark:text-rose-400"
	case count >= 5:
		return "font-medium text-amber-700 dark:text-amber-400"
	default:
		return ""
	}
}

func IdentityRowStateBadgeClass(state string) string {
	switch strings.ToLower(strings.TrimSpace(state)) {
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

func AriaCurrentBool(active bool) string {
	if active {
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
