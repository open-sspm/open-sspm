package datadog

import (
	"strings"

	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

func datadogUserAccountKind(user User) string {
	signal := registry.ClassifyKindFromSignals(user.UserName)
	switch signal {
	case registry.AccountKindBot, registry.AccountKindService:
		return signal
	default:
		return registry.AccountKindHuman
	}
}

func datadogServiceAccountExternalID(id string) string {
	id = strings.TrimSpace(id)
	if id == "" {
		return ""
	}
	return "service_account:" + id
}

func datadogRoleExternalID(id string) string {
	id = strings.TrimSpace(id)
	if id == "" {
		return ""
	}
	return "role:" + id
}
