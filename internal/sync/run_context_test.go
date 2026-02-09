package sync

import (
	"context"
	"testing"
)

func TestTriggerRequestNormalized(t *testing.T) {
	t.Parallel()

	got := (TriggerRequest{
		ConnectorKind: "  OKTA ",
		SourceName:    " example.okta.com ",
	}).Normalized()
	if got.ConnectorKind != "okta" || got.SourceName != "example.okta.com" {
		t.Fatalf("normalized request = %+v", got)
	}

	empty := (TriggerRequest{ConnectorKind: "okta", SourceName: ""}).Normalized()
	if empty.HasConnectorScope() {
		t.Fatalf("expected empty scope for missing source name")
	}
}

func TestWithConnectorScopeRoundTrip(t *testing.T) {
	t.Parallel()

	ctx := WithConnectorScope(context.Background(), " GitHub ", " Acme ")
	kind, name, ok := ConnectorScopeFromContext(ctx)
	if !ok {
		t.Fatalf("expected scope in context")
	}
	if kind != "github" || name != "Acme" {
		t.Fatalf("scope = (%q, %q), want (%q, %q)", kind, name, "github", "Acme")
	}
}

func TestWithConnectorScopeIgnoresInvalidScope(t *testing.T) {
	t.Parallel()

	ctx := WithConnectorScope(context.Background(), "okta", " ")
	if _, _, ok := ConnectorScopeFromContext(ctx); ok {
		t.Fatalf("expected no scope when source name is empty")
	}
}

func TestWithForcedSyncRoundTrip(t *testing.T) {
	t.Parallel()

	if IsForcedSync(context.Background()) {
		t.Fatalf("expected background context to be non-forced")
	}
	if !IsForcedSync(WithForcedSync(nil)) {
		t.Fatalf("expected forced context")
	}
}
