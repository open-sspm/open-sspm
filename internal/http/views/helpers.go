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
