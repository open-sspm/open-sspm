package registry

type RunMode string

const (
	RunModeFull       RunMode = "full"
	RunModeDiscovery  RunMode = "discovery"
	RunModeTail       RunMode = "tail"
	RunModeEventInbox RunMode = "event_inbox"
)

func (m RunMode) Normalize() RunMode {
	switch m {
	case RunModeDiscovery:
		return RunModeDiscovery
	case RunModeTail:
		return RunModeTail
	case RunModeEventInbox:
		return RunModeEventInbox
	default:
		return RunModeFull
	}
}
