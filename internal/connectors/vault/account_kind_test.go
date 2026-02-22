package vault

import (
	"testing"

	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

func TestVaultEntityAccountKind(t *testing.T) {
	t.Parallel()

	human := Entity{Name: "Alice", Metadata: map[string]string{"email": "alice@example.com"}}
	if got := vaultEntityAccountKind(human); got != registry.AccountKindHuman {
		t.Fatalf("vaultEntityAccountKind(human)=%q want %q", got, registry.AccountKindHuman)
	}

	service := Entity{Name: "ci-service-account"}
	if got := vaultEntityAccountKind(service); got != registry.AccountKindService {
		t.Fatalf("vaultEntityAccountKind(service)=%q want %q", got, registry.AccountKindService)
	}

	bot := Entity{Name: "release-bot"}
	if got := vaultEntityAccountKind(bot); got != registry.AccountKindBot {
		t.Fatalf("vaultEntityAccountKind(bot)=%q want %q", got, registry.AccountKindBot)
	}
}

func TestVaultGroupExternalID(t *testing.T) {
	t.Parallel()

	if got := vaultGroupExternalID("g-1"); got != "group:g-1" {
		t.Fatalf("vaultGroupExternalID()=%q want %q", got, "group:g-1")
	}
}
