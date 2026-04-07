package entra

import (
	"testing"

	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

func TestEntraUserAccountKind(t *testing.T) {
	t.Parallel()

	if got := entraUserAccountKind(mustParseUser(t, `{"id":"user-1","displayName":"Alice","mail":"alice@example.com","userType":"Member"}`)); got != registry.AccountKindHuman {
		t.Fatalf("entraUserAccountKind(human)=%q want %q", got, registry.AccountKindHuman)
	}
	if got := entraUserAccountKind(mustParseUser(t, `{"id":"user-2","displayName":"Deploy Bot"}`)); got != registry.AccountKindBot {
		t.Fatalf("entraUserAccountKind(bot)=%q want %q", got, registry.AccountKindBot)
	}
	if got := entraUserAccountKind(mustParseUser(t, `{"id":"user-3","displayName":"CI Service Account"}`)); got != registry.AccountKindService {
		t.Fatalf("entraUserAccountKind(service)=%q want %q", got, registry.AccountKindService)
	}
}

func TestEntraExternalIDs(t *testing.T) {
	t.Parallel()

	if got := entraServicePrincipalExternalID("sp-1"); got != "sp:sp-1" {
		t.Fatalf("entraServicePrincipalExternalID()=%q want %q", got, "sp:sp-1")
	}
	if got := entraGroupExternalID("g-1"); got != "group:g-1" {
		t.Fatalf("entraGroupExternalID()=%q want %q", got, "group:g-1")
	}
}
