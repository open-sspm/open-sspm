package sync

import "github.com/open-sspm/open-sspm/internal/connectors/registry"

const (
	defaultRunOnceScopeKind = "sync"
	defaultRunOnceScopeName = "runonce"

	RunOnceScopeNameFull      = "runonce_full"
	RunOnceScopeNameDiscovery = "runonce_discovery"
	RunOnceScopeNameTail      = "runonce_tail"

	syncJobLaneFull      = "full"
	syncJobLaneDiscovery = "discovery"
	syncJobLaneTail      = "tail"

	syncJobNotifyChannelFull      = "open_sspm_sync_jobs_full"
	syncJobNotifyChannelDiscovery = "open_sspm_sync_jobs_discovery"
	syncJobNotifyChannelTail      = "open_sspm_sync_jobs_tail"

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
	syncJobConsumerScopeNameTail      = "consume_tail"
)

func SyncJobLaneForMode(mode registry.RunMode) string {
	switch mode.Normalize() {
	case registry.RunModeDiscovery:
		return syncJobLaneDiscovery
	case registry.RunModeTail:
		return syncJobLaneTail
	default:
		return syncJobLaneFull
	}
}

func SyncJobConsumerScopeNameForMode(mode registry.RunMode) string {
	switch mode.Normalize() {
	case registry.RunModeDiscovery:
		return syncJobConsumerScopeNameDiscovery
	case registry.RunModeTail:
		return syncJobConsumerScopeNameTail
	default:
		return syncJobConsumerScopeNameFull
	}
}

func SyncJobNotifyChannelForMode(mode registry.RunMode) string {
	switch mode.Normalize() {
	case registry.RunModeDiscovery:
		return syncJobNotifyChannelDiscovery
	case registry.RunModeTail:
		return syncJobNotifyChannelTail
	default:
		return syncJobNotifyChannelFull
	}
}
