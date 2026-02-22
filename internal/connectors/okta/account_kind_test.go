package okta

import (
	"testing"

	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

func TestOktaUserAccountKind(t *testing.T) {
	t.Parallel()

	if got := oktaUserAccountKind(User{DisplayName: "Alice", Email: "alice@example.com"}); got != registry.AccountKindHuman {
		t.Fatalf("oktaUserAccountKind(human)=%q want %q", got, registry.AccountKindHuman)
	}
	if got := oktaUserAccountKind(User{DisplayName: "Build Bot"}); got != registry.AccountKindBot {
		t.Fatalf("oktaUserAccountKind(bot)=%q want %q", got, registry.AccountKindBot)
	}
}

func TestOktaGroupExternalID(t *testing.T) {
	t.Parallel()

	if got := oktaGroupExternalID("00gabc"); got != "group:00gabc" {
		t.Fatalf("oktaGroupExternalID()=%q want %q", got, "group:00gabc")
	}
}
