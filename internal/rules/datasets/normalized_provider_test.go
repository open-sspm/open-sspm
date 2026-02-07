package datasets

import (
	"context"
	"encoding/json"
	"testing"

	runtimev2 "github.com/open-sspm/open-sspm-spec/gen/go/opensspm/runtime/v2"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

type normalizedQueryStub struct {
	identitiesV1             []gen.ListNormalizedIdentitiesV1Row
	identitiesV2             []gen.ListNormalizedIdentitiesV2Row
	entitlementAssignmentsV1 []gen.ListNormalizedEntitlementAssignmentsV1Row
	entitlementAssignmentsV2 []gen.ListNormalizedEntitlementAssignmentsV2Row
}

func (s normalizedQueryStub) ListNormalizedIdentitiesV1(context.Context) ([]gen.ListNormalizedIdentitiesV1Row, error) {
	return s.identitiesV1, nil
}

func (s normalizedQueryStub) ListNormalizedIdentitiesV2(context.Context) ([]gen.ListNormalizedIdentitiesV2Row, error) {
	return s.identitiesV2, nil
}

func (s normalizedQueryStub) ListNormalizedEntitlementAssignmentsV1(context.Context) ([]gen.ListNormalizedEntitlementAssignmentsV1Row, error) {
	return s.entitlementAssignmentsV1, nil
}

func (s normalizedQueryStub) ListNormalizedEntitlementAssignmentsV2(context.Context) ([]gen.ListNormalizedEntitlementAssignmentsV2Row, error) {
	return s.entitlementAssignmentsV2, nil
}

func TestNormalizedProviderIdentitiesV1RemainsAvailable(t *testing.T) {
	t.Parallel()

	provider := &NormalizedProvider{
		Q: normalizedQueryStub{
			identitiesV1: []gen.ListNormalizedIdentitiesV1Row{
				{
					IdpUserID:          42,
					IdpUserExternalID:  "00u42",
					IdpUserEmail:       "alice@example.com",
					IdpUserDisplayName: "Alice",
					IdpUserStatus:      "ACTIVE",
				},
			},
		},
	}

	res := provider.GetDataset(context.Background(), runtimev2.EvalContext{}, runtimev2.DatasetRef{
		Dataset: "normalized:identities",
		Version: 1,
	})
	if res.Error != nil {
		t.Fatalf("GetDataset() error = %v", res.Error)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(res.Rows))
	}

	row := decodeRow(t, res.Rows[0])
	if got := row["id"]; got != "42" {
		t.Fatalf("row.id = %#v, want %q", got, "42")
	}
	if got := row["external_id"]; got != "00u42" {
		t.Fatalf("row.external_id = %#v, want %q", got, "00u42")
	}
	if got := row["status"]; got != "active" {
		t.Fatalf("row.status = %#v, want %q", got, "active")
	}
}

func TestNormalizedProviderIdentitiesV2IncludesManagedAndUnmanaged(t *testing.T) {
	t.Parallel()

	provider := &NormalizedProvider{
		Q: normalizedQueryStub{
			identitiesV2: []gen.ListNormalizedIdentitiesV2Row{
				{
					IdentityID:              1,
					IdentityKind:            "human",
					IdentityEmail:           "managed@example.com",
					IdentityDisplayName:     "Managed Person",
					IdentityManaged:         true,
					AuthoritativeSourceKind: "okta",
					AuthoritativeSourceName: "example.okta.com",
					AuthoritativeExternalID: "00u123",
				},
				{
					IdentityID:          2,
					IdentityKind:        "unknown",
					IdentityEmail:       "shadow@example.com",
					IdentityDisplayName: "Shadow User",
					IdentityManaged:     false,
				},
			},
		},
	}

	res := provider.GetDataset(context.Background(), runtimev2.EvalContext{}, runtimev2.DatasetRef{
		Dataset: "normalized:identities",
		Version: 2,
	})
	if res.Error != nil {
		t.Fatalf("GetDataset() error = %v", res.Error)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(res.Rows))
	}

	managed := decodeRow(t, res.Rows[0])
	unmanaged := decodeRow(t, res.Rows[1])

	if got := managed["managed"]; got != true {
		t.Fatalf("managed row managed = %#v, want true", got)
	}
	if got := unmanaged["managed"]; got != false {
		t.Fatalf("unmanaged row managed = %#v, want false", got)
	}

	authAccount, ok := managed["authoritative_account"].(map[string]any)
	if !ok {
		t.Fatalf("managed.authoritative_account = %#v, want map", managed["authoritative_account"])
	}
	if got := authAccount["source_kind"]; got != "okta" {
		t.Fatalf("managed.authoritative_account.source_kind = %#v, want %q", got, "okta")
	}
}

func TestNormalizedProviderEntitlementAssignmentsV1AndV2(t *testing.T) {
	t.Parallel()

	provider := &NormalizedProvider{
		Q: normalizedQueryStub{
			entitlementAssignmentsV1: []gen.ListNormalizedEntitlementAssignmentsV1Row{
				{
					EntitlementID:         100,
					IdpUserID:             7,
					IdpUserEmail:          "legacy@example.com",
					IdpUserDisplayName:    "Legacy User",
					IdpUserStatus:         "DEPROVISIONED",
					SourceKind:            "github",
					SourceName:            "acme",
					AppUserExternalID:     "legacy-gh",
					EntitlementKind:       "repo_role",
					EntitlementResource:   "repo:acme/private",
					EntitlementPermission: "maintain",
				},
			},
			entitlementAssignmentsV2: []gen.ListNormalizedEntitlementAssignmentsV2Row{
				{
					EntitlementID:         101,
					IdentityID:            8,
					IdentityKind:          "unknown",
					IdentityEmail:         "shadow@example.com",
					IdentityDisplayName:   "Shadow User",
					IdentityManaged:       false,
					SourceKind:            "github",
					SourceName:            "acme",
					AppUserExternalID:     "shadow-gh",
					EntitlementKind:       "repo_role",
					EntitlementResource:   "repo:acme/prod",
					EntitlementPermission: "owner",
				},
			},
		},
	}

	resV1 := provider.GetDataset(context.Background(), runtimev2.EvalContext{}, runtimev2.DatasetRef{
		Dataset: "normalized:entitlement_assignments",
		Version: 1,
	})
	if resV1.Error != nil {
		t.Fatalf("GetDataset(v1) error = %v", resV1.Error)
	}
	if len(resV1.Rows) != 1 {
		t.Fatalf("v1 rows = %d, want 1", len(resV1.Rows))
	}
	v1Row := decodeRow(t, resV1.Rows[0])
	identityV1, ok := v1Row["identity"].(map[string]any)
	if !ok {
		t.Fatalf("v1 identity = %#v, want map", v1Row["identity"])
	}
	if got := identityV1["status"]; got != "deprovisioned" {
		t.Fatalf("v1 identity.status = %#v, want %q", got, "deprovisioned")
	}

	resV2 := provider.GetDataset(context.Background(), runtimev2.EvalContext{}, runtimev2.DatasetRef{
		Dataset: "normalized:entitlement_assignments",
		Version: 2,
	})
	if resV2.Error != nil {
		t.Fatalf("GetDataset(v2) error = %v", resV2.Error)
	}
	if len(resV2.Rows) != 1 {
		t.Fatalf("v2 rows = %d, want 1", len(resV2.Rows))
	}
	v2Row := decodeRow(t, resV2.Rows[0])
	identityV2, ok := v2Row["identity"].(map[string]any)
	if !ok {
		t.Fatalf("v2 identity = %#v, want map", v2Row["identity"])
	}
	if got := identityV2["managed"]; got != false {
		t.Fatalf("v2 identity.managed = %#v, want false", got)
	}
}

func decodeRow(t *testing.T, raw json.RawMessage) map[string]any {
	t.Helper()
	var row map[string]any
	if err := json.Unmarshal(raw, &row); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	return row
}
