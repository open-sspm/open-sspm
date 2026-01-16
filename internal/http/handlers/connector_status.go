package handlers

import (
	"fmt"
	"strings"
	"time"
)

type connectorHealthStatus string

const (
	connectorHealthUnsupported   connectorHealthStatus = "unsupported"
	connectorHealthNotConfigured connectorHealthStatus = "not_configured"
	connectorHealthDisabled      connectorHealthStatus = "disabled"
	connectorHealthNeverSynced   connectorHealthStatus = "never_synced"
	connectorHealthHealthy       connectorHealthStatus = "healthy"
	connectorHealthDegraded      connectorHealthStatus = "degraded"
	connectorHealthStale         connectorHealthStatus = "stale"
	minStaleAfter                                      = 2 * time.Hour
	maxStaleAfter                                      = 72 * time.Hour
	connectorSuccessLookback                           = 7 * 24 * time.Hour
)

type syncRunRollup struct {
	lastRunStatus          string
	lastRunErrorKind       string
	lastRunFinishedAt      *time.Time
	lastSuccessAt          *time.Time
	finishedCount7d        int64
	successCount7d         int64
	avgSuccessDuration7d   *time.Duration
	lookbackWindowDuration time.Duration
}

type connectorHealthInput struct {
	syncable         bool
	configured       bool
	enabled          bool
	expectedInterval time.Duration
	now              time.Time
	rollup           syncRunRollup
}

type connectorHealthResult struct {
	status            connectorHealthStatus
	statusLabel       string
	statusClass       string
	lastSuccessLabel  string
	lastRunLabel      string
	successRate7d     string
	avgDuration7d     string
	needsAttention    bool
	countsAsEnabled   bool
	lookbackWindowTag string
}

func connectorHealth(input connectorHealthInput) connectorHealthResult {
	rollup := input.rollup
	if rollup.lookbackWindowDuration == 0 {
		rollup.lookbackWindowDuration = connectorSuccessLookback
	}

	result := connectorHealthResult{
		lookbackWindowTag: formatLookbackWindowTag(rollup.lookbackWindowDuration),
		lastSuccessLabel:  "—",
		lastRunLabel:      "—",
		successRate7d:     formatSuccessRate(rollup.successCount7d, rollup.finishedCount7d),
		avgDuration7d:     formatDurationOptional(rollup.avgSuccessDuration7d),
	}

	if !input.syncable {
		result.status = connectorHealthUnsupported
		result.statusLabel = "Coming soon"
		result.statusClass = badgeClassNeutral()
		return result
	}

	if !input.configured {
		result.status = connectorHealthNotConfigured
		result.statusLabel = "Not configured"
		result.statusClass = badgeClassNeutral()
		return result
	}

	if rollup.lastSuccessAt != nil && !rollup.lastSuccessAt.IsZero() {
		result.lastSuccessLabel = formatAge(input.now, *rollup.lastSuccessAt)
	}

	if rollup.lastRunFinishedAt != nil && !rollup.lastRunFinishedAt.IsZero() {
		result.lastRunLabel = formatRunLabel(rollup.lastRunStatus, rollup.lastRunErrorKind, formatAge(input.now, *rollup.lastRunFinishedAt))
	}

	if !input.enabled {
		result.status = connectorHealthDisabled
		result.statusLabel = "Disabled"
		result.statusClass = badgeClassNeutral()
		return result
	}

	result.countsAsEnabled = true

	status := connectorHealthFromRollup(input.now, input.expectedInterval, rollup)
	result.status = status
	switch status {
	case connectorHealthHealthy:
		result.statusLabel = "Healthy"
		result.statusClass = badgeClassSuccess()
	case connectorHealthDegraded:
		result.statusLabel = "Degraded"
		result.statusClass = badgeClassWarning()
		result.needsAttention = true
	case connectorHealthStale:
		result.statusLabel = "Stale"
		result.statusClass = badgeClassDanger()
		result.needsAttention = true
	case connectorHealthNeverSynced:
		result.statusLabel = "Never synced"
		result.statusClass = badgeClassWarning()
		result.needsAttention = true
	default:
		result.statusLabel = "Unknown"
		result.statusClass = badgeClassNeutral()
	}

	return result
}

func connectorHealthFromRollup(now time.Time, expectedInterval time.Duration, rollup syncRunRollup) connectorHealthStatus {
	if rollup.lastSuccessAt == nil || rollup.lastSuccessAt.IsZero() {
		return connectorHealthNeverSynced
	}

	staleAfter := staleAfterInterval(expectedInterval)
	if !now.IsZero() && now.Sub(*rollup.lastSuccessAt) > staleAfter {
		return connectorHealthStale
	}

	if strings.EqualFold(strings.TrimSpace(rollup.lastRunStatus), "success") {
		return connectorHealthHealthy
	}
	return connectorHealthDegraded
}

func staleAfterInterval(expectedInterval time.Duration) time.Duration {
	derived := expectedInterval * 4
	if derived < minStaleAfter {
		return minStaleAfter
	}
	if derived > maxStaleAfter {
		return maxStaleAfter
	}
	return derived
}

func formatLookbackWindowTag(d time.Duration) string {
	if d <= 0 {
		return ""
	}
	days := int(d.Hours() / 24)
	if days <= 0 {
		return "24h"
	}
	return fmt.Sprintf("%dd", days)
}

func formatAge(now time.Time, t time.Time) string {
	if t.IsZero() {
		return "—"
	}
	if now.IsZero() {
		now = time.Now()
	}
	delta := now.Sub(t)
	if delta < 0 {
		delta = 0
	}
	switch {
	case delta < time.Minute:
		return "just now"
	case delta < time.Hour:
		return fmt.Sprintf("%dm ago", int(delta.Minutes()))
	case delta < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(delta.Hours()))
	default:
		return fmt.Sprintf("%dd ago", int(delta.Hours()/24))
	}
}

func formatRunLabel(status, errorKind, age string) string {
	status = strings.ToLower(strings.TrimSpace(status))
	errorKind = strings.ToLower(strings.TrimSpace(errorKind))
	age = strings.TrimSpace(age)

	label := strings.TrimSpace(status)
	switch status {
	case "success":
		label = "Success"
	case "error":
		if errorKind != "" {
			label = fmt.Sprintf("Error (%s)", errorKind)
		} else {
			label = "Error"
		}
	case "canceled":
		label = "Canceled"
	case "":
		return "—"
	default:
		label = strings.Title(label) //nolint:staticcheck // acceptable for short UI labels
	}

	if age != "" && age != "—" {
		return label + " · " + age
	}
	return label
}

func formatSuccessRate(successes int64, finished int64) string {
	if finished <= 0 {
		return "—"
	}
	if successes < 0 {
		successes = 0
	}
	if successes > finished {
		successes = finished
	}
	percent := int((successes * 100) / finished)
	if percent < 0 {
		percent = 0
	}
	if percent > 100 {
		percent = 100
	}
	return fmt.Sprintf("%d%%", percent)
}

func formatDurationOptional(d *time.Duration) string {
	if d == nil || *d <= 0 {
		return "—"
	}
	return formatDuration(*d)
}

func formatDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	d = d.Round(time.Second)
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		minutes := int(d / time.Minute)
		seconds := int((d % time.Minute) / time.Second)
		if seconds == 0 {
			return fmt.Sprintf("%dm", minutes)
		}
		return fmt.Sprintf("%dm %ds", minutes, seconds)
	}

	hours := int(d / time.Hour)
	minutes := int((d % time.Hour) / time.Minute)
	if minutes == 0 {
		return fmt.Sprintf("%dh", hours)
	}
	return fmt.Sprintf("%dh %dm", hours, minutes)
}

func badgeClassSuccess() string {
	return "badge bg-emerald-100 text-emerald-800 dark:bg-emerald-900/50 dark:text-emerald-100"
}

func badgeClassWarning() string {
	return "badge bg-amber-100 text-amber-800 dark:bg-amber-900/50 dark:text-amber-100"
}

func badgeClassDanger() string {
	return "badge bg-rose-100 text-rose-800 dark:bg-rose-900/50 dark:text-rose-100"
}

func badgeClassNeutral() string {
	return "badge bg-slate-100 text-slate-800 dark:bg-slate-900/50 dark:text-slate-100"
}
