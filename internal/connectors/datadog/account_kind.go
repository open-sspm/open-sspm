package datadog

import (
	"strings"

	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

func datadogUserAccountKind(userName string) string {
	signal := registry.ClassifyKindFromSignals(strings.TrimSpace(userName))
	switch signal {
	case registry.AccountKindBot, registry.AccountKindService:
		return signal
	default:
		return registry.AccountKindHuman
	}
}

func datadogAccountExternalID(id string, serviceAccount bool) string {
	if serviceAccount {
		return datadogServiceAccountExternalID(id)
	}
	return strings.TrimSpace(id)
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
