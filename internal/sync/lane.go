package sync

import "github.com/open-sspm/open-sspm/internal/connectors/registry"

const (
	legacyRunOnceScopeKind = "sync"
	legacyRunOnceScopeName = "runonce"

	RunOnceScopeNameFull      = "runonce_full"
	RunOnceScopeNameDiscovery = "runonce_discovery"

	ResyncNotifyChannelFull      = "open_sspm_resync_requested"
	ResyncNotifyChannelDiscovery = "open_sspm_resync_discovery_requested"
)

func RunOnceScopeNameForMode(mode registry.RunMode) string {
	switch mode.Normalize() {
	case registry.RunModeDiscovery:
		return RunOnceScopeNameDiscovery
	default:
		return RunOnceScopeNameFull
	}
}

func ResyncNotifyChannelForMode(mode registry.RunMode) string {
	switch mode.Normalize() {
	case registry.RunModeDiscovery:
		return ResyncNotifyChannelDiscovery
	default:
		return ResyncNotifyChannelFull
	}
}
