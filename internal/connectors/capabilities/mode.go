package capabilities

import "github.com/open-sspm/open-sspm/internal/connectors/registry"

type Provider interface {
	Capabilities() Capabilities
}

func SupportsRunMode(integration any, mode registry.RunMode) (bool, bool) {
	provider, ok := integration.(Provider)
	if !ok {
		return false, false
	}

	caps := provider.Capabilities()
	switch mode.Normalize() {
	case registry.RunModeFull:
		return caps.Full != nil, true
	case registry.RunModeDiscovery:
		return caps.Discovery != nil, true
	case registry.RunModeTail:
		return caps.Tail != nil, true
	default:
		return false, false
	}
}
