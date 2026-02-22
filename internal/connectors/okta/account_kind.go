package okta

import (
	"strings"

	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

func oktaUserAccountKind(user User) string {
	signal := registry.ClassifyKindFromSignals(user.DisplayName, user.Email)
	switch signal {
	case registry.AccountKindBot, registry.AccountKindService:
		return signal
	default:
		return registry.AccountKindHuman
	}
}

func oktaGroupExternalID(groupID string) string {
	groupID = strings.TrimSpace(groupID)
	if groupID == "" {
		return ""
	}
	return "group:" + groupID
}
