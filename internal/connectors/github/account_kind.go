package github

import (
	"strings"

	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

func githubMemberAccountKind(member Member) string {
	switch strings.ToLower(strings.TrimSpace(member.AccountType)) {
	case "bot":
		return registry.AccountKindBot
	case "organization":
		return registry.AccountKindService
	case "user", "mannequin":
		return registry.AccountKindHuman
	}

	signal := registry.ClassifyKindFromSignals(member.Login, member.DisplayName, member.Email)
	switch signal {
	case registry.AccountKindBot:
		return registry.AccountKindBot
	case registry.AccountKindService:
		return registry.AccountKindService
	default:
		return registry.AccountKindHuman
	}
}

func githubTeamExternalID(teamSlug string) string {
	teamSlug = strings.TrimSpace(teamSlug)
	if teamSlug == "" {
		return ""
	}
	return "team:" + teamSlug
}
