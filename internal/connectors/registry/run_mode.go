package registry

import "strings"

type RunMode string

const (
	RunModeFull      RunMode = "full"
	RunModeDiscovery RunMode = "discovery"
	RunModeTail      RunMode = "tail"
)

func (m RunMode) Normalize() RunMode {
	switch m {
	case RunModeDiscovery:
		return RunModeDiscovery
	case RunModeTail:
		return RunModeTail
	default:
		return RunModeFull
	}
}

func SyncRunSourceKind(kind string, mode RunMode) string {
	kind = strings.ToLower(strings.TrimSpace(kind))
	switch mode.Normalize() {
	case RunModeDiscovery:
		switch kind {
		case "okta":
			return "okta_discovery"
		case "entra":
			return "entra_discovery"
		case "google_workspace":
			return "google_workspace_discovery"
		}
	case RunModeTail:
		if kind != "" {
			return kind + "_tail"
		}
	}
	return kind
}

func SyncRunScopeKinds(sourceKind string) []string {
	switch strings.ToLower(strings.TrimSpace(sourceKind)) {
	case "okta", "okta_discovery", "okta_tail":
		return []string{"okta", "okta_discovery", "okta_tail"}
	case "entra", "entra_discovery", "entra_tail":
		return []string{"entra", "entra_discovery", "entra_tail"}
	case "google_workspace", "google_workspace_discovery", "google_workspace_tail":
		return []string{"google_workspace", "google_workspace_discovery", "google_workspace_tail"}
	case "datadog", "datadog_tail":
		return []string{"datadog", "datadog_tail"}
	case "aws", "aws_tail":
		return []string{"aws", "aws_tail"}
	case "":
		return nil
	default:
		return []string{strings.ToLower(strings.TrimSpace(sourceKind))}
	}
}
