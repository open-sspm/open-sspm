package discovery

import "testing"

func TestScopeClassification(t *testing.T) {
	t.Parallel()

	scopes := []string{"Mail.Read", "Files.ReadWrite.All", "offline_access"}
	if !HasPrivilegedScopes(scopes) {
		t.Fatalf("expected privileged scope classification")
	}
	if !HasConfidentialScopes(scopes) {
		t.Fatalf("expected confidential scope classification")
	}

	normalized := NormalizeScopes([]string{" mail.read ", "MAIL.READ", "", "files.read"})
	if len(normalized) != 2 {
		t.Fatalf("NormalizeScopes len = %d, want 2", len(normalized))
	}
}
