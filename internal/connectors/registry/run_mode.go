package registry

import "strings"

type RunMode string

const (
	RunModeFull      RunMode = "full"
	RunModeDiscovery RunMode = "discovery"
)

func ParseRunMode(v string) RunMode {
	mode := RunMode(strings.ToLower(strings.TrimSpace(v)))
	return mode.Normalize()
}

func (m RunMode) Normalize() RunMode {
	switch m {
	case RunModeDiscovery:
		return RunModeDiscovery
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
	}
	return kind
}
