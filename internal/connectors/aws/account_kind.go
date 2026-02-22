package aws

import (
	"strings"

	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

func awsUserAccountKind(user User) string {
	signal := registry.ClassifyKindFromSignals(user.DisplayName, user.Email, user.ID)
	switch signal {
	case registry.AccountKindBot, registry.AccountKindService:
		return signal
	default:
		return registry.AccountKindHuman
	}
}

func awsGroupExternalID(groupID string) string {
	groupID = strings.TrimSpace(groupID)
	if groupID == "" {
		return ""
	}
	return "group:" + groupID
}
