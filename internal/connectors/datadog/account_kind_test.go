package datadog

import (
	"testing"

	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

func TestDatadogUserAccountKind(t *testing.T) {
	t.Parallel()

	if got := datadogUserAccountKind("alice@example.com"); got != registry.AccountKindHuman {
		t.Fatalf("datadogUserAccountKind(human)=%q want %q", got, registry.AccountKindHuman)
	}
	if got := datadogUserAccountKind("ci-bot@example.com"); got != registry.AccountKindBot {
		t.Fatalf("datadogUserAccountKind(bot)=%q want %q", got, registry.AccountKindBot)
	}
}

func TestDatadogExternalIDs(t *testing.T) {
	t.Parallel()

	if got := datadogServiceAccountExternalID("sa-1"); got != "service_account:sa-1" {
		t.Fatalf("datadogServiceAccountExternalID()=%q want %q", got, "service_account:sa-1")
	}
	if got := datadogRoleExternalID("r-1"); got != "role:r-1" {
		t.Fatalf("datadogRoleExternalID()=%q want %q", got, "role:r-1")
	}
	if got := datadogAccountExternalID("u-1", false); got != "u-1" {
		t.Fatalf("datadogAccountExternalID(user)=%q want %q", got, "u-1")
	}
	if got := datadogAccountExternalID("sa-1", true); got != "service_account:sa-1" {
		t.Fatalf("datadogAccountExternalID(service)=%q want %q", got, "service_account:sa-1")
	}
}
