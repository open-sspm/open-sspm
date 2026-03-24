package okta

import (
	"testing"

	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

func TestOktaAccountKind(t *testing.T) {
	t.Parallel()

	if got := oktaAccountKind(User{DisplayName: "Alice", Email: "alice@example.com"}); got != registry.AccountKindHuman {
		t.Fatalf("oktaAccountKind(human)=%q want %q", got, registry.AccountKindHuman)
	}
	if got := oktaAccountKind(User{DisplayName: "Build Bot"}); got != registry.AccountKindBot {
		t.Fatalf("oktaAccountKind(bot)=%q want %q", got, registry.AccountKindBot)
	}
}

func TestOktaGroupExternalID(t *testing.T) {
	t.Parallel()

	if got := oktaGroupExternalID("00gabc"); got != "group:00gabc" {
		t.Fatalf("oktaGroupExternalID()=%q want %q", got, "group:00gabc")
	}
}
