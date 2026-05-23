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
	identitiesV3             []gen.ListNormalizedIdentitiesV3Row
	entitlementAssignmentsV1 []gen.ListNormalizedEntitlementAssignmentsV1Row
	entitlementAssignmentsV2 []gen.ListNormalizedEntitlementAssignmentsV2Row
	entitlementAssignmentsV3 []gen.ListNormalizedEntitlementAssignmentsV3Row
}

func (s normalizedQueryStub) ListNormalizedIdentitiesV1(context.Context) ([]gen.ListNormalizedIdentitiesV1Row, error) {
	return s.identitiesV1, nil
}

func (s normalizedQueryStub) ListNormalizedIdentitiesV2(context.Context) ([]gen.ListNormalizedIdentitiesV2Row, error) {
	return s.identitiesV2, nil
}

func (s normalizedQueryStub) ListNormalizedIdentitiesV3(context.Context) ([]gen.ListNormalizedIdentitiesV3Row, error) {
	return s.identitiesV3, nil
}

func (s normalizedQueryStub) ListNormalizedEntitlementAssignmentsV1(context.Context) ([]gen.ListNormalizedEntitlementAssignmentsV1Row, error) {
	return s.entitlementAssignmentsV1, nil
}

func (s normalizedQueryStub) ListNormalizedEntitlementAssignmentsV2(context.Context) ([]gen.ListNormalizedEntitlementAssignmentsV2Row, error) {
	return s.entitlementAssignmentsV2, nil
}

func (s normalizedQueryStub) ListNormalizedEntitlementAssignmentsV3(context.Context) ([]gen.ListNormalizedEntitlementAssignmentsV3Row, error) {
	return s.entitlementAssignmentsV3, nil
}

func TestNormalizedProviderIdentitiesV1KeepsLegacyFields(t *testing.T) {
	t.Parallel()

	provider := &NormalizedProvider{
		Q: normalizedQueryStub{
			identitiesV1: []gen.ListNormalizedIdentitiesV1Row{
				{
					IdentityID:          1,
					IdentityExternalID:  "00u123",
					IdentityEmail:       "managed@example.com",
					IdentityDisplayName: "Managed Person",
					IdentityStatus:      "ACTIVE",
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

	row := decodeRow(t, res.Rows[0])
	if got := row["external_id"]; got != "00u123" {
		t.Fatalf("external_id = %#v, want %q", got, "00u123")
	}
	if got := row["status"]; got != "active" {
		t.Fatalf("status = %#v, want %q", got, "active")
	}
}

func TestNormalizedProviderIdentitiesV2KeepsAuthoritativeAccount(t *testing.T) {
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

	row := decodeRow(t, res.Rows[0])
	account, ok := row["authoritative_account"].(map[string]any)
	if !ok {
		t.Fatalf("authoritative_account = %#v, want map", row["authoritative_account"])
	}
	if got := account["source_kind"]; got != "okta" {
		t.Fatalf("authoritative_account.source_kind = %#v, want %q", got, "okta")
	}
}

func TestNormalizedProviderIdentitiesV3ExposePostureAndAnchor(t *testing.T) {
	t.Parallel()

	provider := &NormalizedProvider{
		Q: normalizedQueryStub{
			identitiesV3: []gen.ListNormalizedIdentitiesV3Row{
				{
					IdentityID:              1,
					IdentityKind:            "human",
					IdentityEmail:           "managed@example.com",
					IdentityDisplayName:     "Managed Person",
					IdentityManaged:         true,
					IdentityPosture:         "managed",
					IdentityAnchorState:     "anchored",
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
					IdentityPosture:     "unmanaged",
					IdentityAnchorState: "missing_anchor",
				},
			},
		},
	}

	res := provider.GetDataset(context.Background(), runtimev2.EvalContext{}, runtimev2.DatasetRef{
		Dataset: "normalized:identities",
		Version: 3,
	})
	if res.Error != nil {
		t.Fatalf("GetDataset() error = %v", res.Error)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(res.Rows))
	}

	managed := decodeRow(t, res.Rows[0])
	unmanaged := decodeRow(t, res.Rows[1])

	if got := managed["posture"]; got != "managed" {
		t.Fatalf("managed row posture = %#v, want %q", got, "managed")
	}
	managedAnchor, ok := managed["anchor"].(map[string]any)
	if !ok {
		t.Fatalf("managed.anchor = %#v, want map", managed["anchor"])
	}
	if got := managedAnchor["state"]; got != "anchored" {
		t.Fatalf("managed.anchor.state = %#v, want %q", got, "anchored")
	}
	if got := managedAnchor["source_kind"]; got != "okta" {
		t.Fatalf("managed.anchor.source_kind = %#v, want %q", got, "okta")
	}

	if got := unmanaged["managed"]; got != false {
		t.Fatalf("unmanaged row managed = %#v, want false", got)
	}
	if got := unmanaged["posture"]; got != "unmanaged" {
		t.Fatalf("unmanaged row posture = %#v, want %q", got, "unmanaged")
	}
	unmanagedAnchor, ok := unmanaged["anchor"].(map[string]any)
	if !ok {
		t.Fatalf("unmanaged.anchor = %#v, want map", unmanaged["anchor"])
	}
	if got := unmanagedAnchor["state"]; got != "missing_anchor" {
		t.Fatalf("unmanaged.anchor.state = %#v, want %q", got, "missing_anchor")
	}
}

func TestNormalizedProviderEntitlementAssignmentsV3ExposeIdentityPosture(t *testing.T) {
	t.Parallel()

	provider := &NormalizedProvider{
		Q: normalizedQueryStub{
			entitlementAssignmentsV3: []gen.ListNormalizedEntitlementAssignmentsV3Row{
				{
					EntitlementID:         101,
					IdentityID:            8,
					IdentityKind:          "unknown",
					IdentityEmail:         "shadow@example.com",
					IdentityDisplayName:   "Shadow User",
					IdentityManaged:       false,
					IdentityPosture:       "unmanaged",
					IdentityAnchorState:   "missing_anchor",
					AccountSourceKind:     "github",
					AccountSourceName:     "acme",
					AccountExternalID:     "shadow-gh",
					EntitlementKind:       "repo_role",
					EntitlementResource:   "repo:acme/prod",
					EntitlementPermission: "owner",
				},
			},
		},
	}

	res := provider.GetDataset(context.Background(), runtimev2.EvalContext{}, runtimev2.DatasetRef{
		Dataset: "normalized:entitlement_assignments",
		Version: 3,
	})
	if res.Error != nil {
		t.Fatalf("GetDataset() error = %v", res.Error)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(res.Rows))
	}

	row := decodeRow(t, res.Rows[0])
	identity, ok := row["identity"].(map[string]any)
	if !ok {
		t.Fatalf("identity = %#v, want map", row["identity"])
	}
	if got := identity["posture"]; got != "unmanaged" {
		t.Fatalf("identity.posture = %#v, want %q", got, "unmanaged")
	}
	anchor, ok := identity["anchor"].(map[string]any)
	if !ok {
		t.Fatalf("identity.anchor = %#v, want map", identity["anchor"])
	}
	if got := anchor["state"]; got != "missing_anchor" {
		t.Fatalf("identity.anchor.state = %#v, want %q", got, "missing_anchor")
	}

	account, ok := row["account"].(map[string]any)
	if !ok {
		t.Fatalf("account = %#v, want map", row["account"])
	}
	if got := account["external_id"]; got != "shadow-gh" {
		t.Fatalf("account.external_id = %#v, want %q", got, "shadow-gh")
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
