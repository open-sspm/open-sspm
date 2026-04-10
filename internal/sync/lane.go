package sync

import "github.com/open-sspm/open-sspm/internal/connectors/registry"

const (
	legacyRunOnceScopeKind = "sync"
	legacyRunOnceScopeName = "runonce"

	RunOnceScopeNameFull      = "runonce_full"
	RunOnceScopeNameDiscovery = "runonce_discovery"

	syncJobLaneFull      = "full"
	syncJobLaneDiscovery = "discovery"

	syncJobNotifyChannelFull      = "open_sspm_sync_jobs_full"
	syncJobNotifyChannelDiscovery = "open_sspm_sync_jobs_discovery"

	syncJobTriggerKindManual    = "manual"
	syncJobTriggerKindScheduled = "scheduled"

	syncJobStatusPending   = "pending"
	syncJobStatusClaimed   = "claimed"
	syncJobStatusRunning   = "running"
	syncJobStatusSucceeded = "succeeded"
	syncJobStatusFailed    = "failed"

	syncJobConsumerScopeKind          = "sync_jobs"
	syncJobConsumerScopeNameFull      = "consume_full"
	syncJobConsumerScopeNameDiscovery = "consume_discovery"
)

func SyncJobLaneForMode(mode registry.RunMode) string {
	switch mode.Normalize() {
	case registry.RunModeDiscovery:
		return syncJobLaneDiscovery
	default:
		return syncJobLaneFull
	}
}

func SyncJobConsumerScopeNameForMode(mode registry.RunMode) string {
	switch mode.Normalize() {
	case registry.RunModeDiscovery:
		return syncJobConsumerScopeNameDiscovery
	default:
		return syncJobConsumerScopeNameFull
	}
}

func SyncJobNotifyChannelForMode(mode registry.RunMode) string {
	switch mode.Normalize() {
	case registry.RunModeDiscovery:
		return syncJobNotifyChannelDiscovery
	default:
		return syncJobNotifyChannelFull
	}
}
