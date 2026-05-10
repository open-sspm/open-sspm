package registry

import (
	"encoding/json"
	"testing"
)

func TestNormalizeAccountKind(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"human":   AccountKindHuman,
		"SERVICE": AccountKindService,
		" bot ":   AccountKindBot,
		"":        AccountKindUnknown,
		"robot":   AccountKindUnknown,
	}

	for input, want := range cases {
		if got := NormalizeAccountKind(input); got != want {
			t.Fatalf("NormalizeAccountKind(%q)=%q want %q", input, got, want)
		}
	}
}

func TestAggregateAccountKinds(t *testing.T) {
	t.Parallel()

	if got := AggregateAccountKinds(AccountKindUnknown, AccountKindHuman); got != AccountKindHuman {
		t.Fatalf("AggregateAccountKinds()=%q want %q", got, AccountKindHuman)
	}
	if got := AggregateAccountKinds(AccountKindHuman, AccountKindService); got != AccountKindService {
		t.Fatalf("AggregateAccountKinds()=%q want %q", got, AccountKindService)
	}
	if got := AggregateAccountKinds(AccountKindService, AccountKindBot, AccountKindHuman); got != AccountKindBot {
		t.Fatalf("AggregateAccountKinds()=%q want %q", got, AccountKindBot)
	}
}

func TestClassifyKindFromSignals(t *testing.T) {
	t.Parallel()

	if got := ClassifyKindFromSignals("Dependabot"); got != AccountKindBot {
		t.Fatalf("ClassifyKindFromSignals bot=%q want %q", got, AccountKindBot)
	}
	if got := ClassifyKindFromSignals("Build Service Account"); got != AccountKindService {
		t.Fatalf("ClassifyKindFromSignals service=%q want %q", got, AccountKindService)
	}
	if got := ClassifyKindFromSignals("Alice Example"); got != AccountKindUnknown {
		t.Fatalf("ClassifyKindFromSignals unknown=%q want %q", got, AccountKindUnknown)
	}
}

func TestWithEntityCategory(t *testing.T) {
	t.Parallel()

	raw := WithEntityCategory([]byte(`{"id":"123"}`), EntityCategoryTeam)
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("json.Unmarshal() error=%v", err)
	}
	if got := payload["entity_category"]; got != EntityCategoryTeam {
		t.Fatalf("entity_category=%v want %q", got, EntityCategoryTeam)
	}
	if got := payload["id"]; got != "123" {
		t.Fatalf("id=%v want %q", got, "123")
	}
}
