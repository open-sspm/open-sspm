package vault

import (
	"strings"

	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

func vaultEntityAccountKind(entity Entity) string {
	parts := []string{entity.Name}
	if email := bestEntityEmail(entity); email != "" {
		parts = append(parts, email)
	}
	for _, alias := range entity.Aliases {
		parts = append(parts, alias.Name, alias.MountType)
	}
	for key, value := range entity.Metadata {
		parts = append(parts, key, value)
	}

	signal := registry.ClassifyKindFromSignals(parts...)
	switch signal {
	case registry.AccountKindBot, registry.AccountKindService:
		return signal
	}

	if vaultHasStrongHumanSignal(entity) {
		return registry.AccountKindHuman
	}

	return registry.AccountKindUnknown
}

func vaultHasStrongHumanSignal(entity Entity) bool {
	email := normalizeEmail(bestEntityEmail(entity))
	if email != "" {
		emailSignal := registry.ClassifyKindFromSignals(email)
		if emailSignal == registry.AccountKindUnknown {
			return true
		}
	}

	for _, alias := range entity.Aliases {
		switch strings.ToLower(strings.TrimSpace(alias.MountType)) {
		case "oidc", "okta", "saml", "ldap", "userpass", "github":
			return true
		}
	}

	return false
}

func vaultGroupExternalID(groupID string) string {
	groupID = strings.TrimSpace(groupID)
	if groupID == "" {
		return ""
	}
	return "group:" + groupID
}
