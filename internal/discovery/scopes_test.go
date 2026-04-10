package discovery

import "testing"

func TestScopeClassification(t *testing.T) {
	t.Parallel()

	normalized := NormalizeScopes([]string{" mail.read ", "MAIL.READ", "", "files.read"})
	if len(normalized) != 2 {
		t.Fatalf("NormalizeScopes len = %d, want 2", len(normalized))
	}
}
