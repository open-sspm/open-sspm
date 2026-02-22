package aws

import (
	"testing"

	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

func TestAWSUserAccountKind(t *testing.T) {
	t.Parallel()

	if got := awsUserAccountKind(User{DisplayName: "Alice Example", Email: "alice@example.com"}); got != registry.AccountKindHuman {
		t.Fatalf("awsUserAccountKind(human)=%q want %q", got, registry.AccountKindHuman)
	}
	if got := awsUserAccountKind(User{DisplayName: "Deploy Bot"}); got != registry.AccountKindBot {
		t.Fatalf("awsUserAccountKind(bot)=%q want %q", got, registry.AccountKindBot)
	}
	if got := awsUserAccountKind(User{DisplayName: "CI Service Account"}); got != registry.AccountKindService {
		t.Fatalf("awsUserAccountKind(service)=%q want %q", got, registry.AccountKindService)
	}
}

func TestAWSGroupExternalID(t *testing.T) {
	t.Parallel()

	if got := awsGroupExternalID("g-1"); got != "group:g-1" {
		t.Fatalf("awsGroupExternalID()=%q want %q", got, "group:g-1")
	}
	if got := awsGroupExternalID("  "); got != "" {
		t.Fatalf("awsGroupExternalID(empty)=%q want empty", got)
	}
}
