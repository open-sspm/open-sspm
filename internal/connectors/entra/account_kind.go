package entra

import (
	"strings"

	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

func entraUserAccountKind(user User) string {
	signal := registry.ClassifyKindFromSignals(user.DisplayName, user.Mail, user.UserPrincipalName, user.UserType)
	switch signal {
	case registry.AccountKindBot, registry.AccountKindService:
		return signal
	default:
		return registry.AccountKindHuman
	}
}

func entraServicePrincipalAccountKind(ServicePrincipal) string {
	return registry.AccountKindService
}

func entraGroupAccountKind(Group) string {
	return registry.AccountKindService
}

func entraServicePrincipalExternalID(id string) string {
	id = strings.TrimSpace(id)
	if id == "" {
		return ""
	}
	return "sp:" + id
}

func entraGroupExternalID(id string) string {
	id = strings.TrimSpace(id)
	if id == "" {
		return ""
	}
	return "group:" + id
}
