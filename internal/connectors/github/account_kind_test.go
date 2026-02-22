package github

import (
	"testing"

	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

func TestGitHubMemberAccountKind(t *testing.T) {
	t.Parallel()

	if got := githubMemberAccountKind(Member{AccountType: "User", Login: "alice"}); got != registry.AccountKindHuman {
		t.Fatalf("githubMemberAccountKind(human)=%q want %q", got, registry.AccountKindHuman)
	}
	if got := githubMemberAccountKind(Member{AccountType: "Bot", Login: "dependabot"}); got != registry.AccountKindBot {
		t.Fatalf("githubMemberAccountKind(bot)=%q want %q", got, registry.AccountKindBot)
	}
	if got := githubMemberAccountKind(Member{AccountType: "Organization", Login: "acme"}); got != registry.AccountKindService {
		t.Fatalf("githubMemberAccountKind(service)=%q want %q", got, registry.AccountKindService)
	}
	if got := githubMemberAccountKind(Member{Login: "release-bot"}); got != registry.AccountKindBot {
		t.Fatalf("githubMemberAccountKind(fallback bot)=%q want %q", got, registry.AccountKindBot)
	}
}

func TestGitHubTeamExternalID(t *testing.T) {
	t.Parallel()

	if got := githubTeamExternalID("platform"); got != "team:platform" {
		t.Fatalf("githubTeamExternalID()=%q want %q", got, "team:platform")
	}
}
