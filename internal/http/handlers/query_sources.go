package handlers

import (
	"strings"

	"github.com/open-sspm/open-sspm/internal/http/querystate"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
)

func programmaticQuerySources(options []viewmodels.ProgrammaticSourceOption) []querystate.SourceSelection {
	return querySources(options, func(option viewmodels.ProgrammaticSourceOption) (string, string) {
		return option.SourceKind, option.SourceName
	})
}

func discoveryQuerySources(options []viewmodels.DiscoverySourceOption) []querystate.SourceSelection {
	return querySources(options, func(option viewmodels.DiscoverySourceOption) (string, string) {
		return option.SourceKind, option.SourceName
	})
}

func querySources[T any](options []T, fields func(T) (string, string)) []querystate.SourceSelection {
	out := make([]querystate.SourceSelection, 0, len(options))
	for _, option := range options {
		kind, name := fields(option)
		kind = strings.TrimSpace(kind)
		name = strings.TrimSpace(name)
		if kind == "" && name == "" {
			continue
		}
		out = append(out, querystate.SourceSelection{
			Kind: kind,
			Name: name,
		})
	}
	return out
}

func normalizeActiveInactiveState(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "active":
		return "active"
	case "inactive":
		return "inactive"
	default:
		return ""
	}
}
