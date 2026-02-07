package datasets

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	runtimev2 "github.com/open-sspm/open-sspm-spec/gen/go/opensspm/runtime/v2"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/rules/engine"
)

type NormalizedProvider struct {
	Q normalizedQueryRunner
}

type normalizedQueryRunner interface {
	ListNormalizedIdentitiesV1(context.Context) ([]gen.ListNormalizedIdentitiesV1Row, error)
	ListNormalizedIdentitiesV2(context.Context) ([]gen.ListNormalizedIdentitiesV2Row, error)
	ListNormalizedEntitlementAssignmentsV1(context.Context) ([]gen.ListNormalizedEntitlementAssignmentsV1Row, error)
	ListNormalizedEntitlementAssignmentsV2(context.Context) ([]gen.ListNormalizedEntitlementAssignmentsV2Row, error)
}

func (p *NormalizedProvider) Capabilities(ctx context.Context) []runtimev2.DatasetRef {
	_ = ctx
	if p == nil {
		return nil
	}
	out := make([]runtimev2.DatasetRef, 0, len(normalizedCapabilitiesV2)*2)
	for _, ds := range normalizedCapabilitiesV2 {
		out = append(out, runtimev2.DatasetRef{Dataset: ds, Version: 1})
		out = append(out, runtimev2.DatasetRef{Dataset: ds, Version: 2})
	}
	return out
}

func (p *NormalizedProvider) GetDataset(ctx context.Context, eval runtimev2.EvalContext, ref runtimev2.DatasetRef) runtimev2.DatasetResult {
	_ = eval

	if p == nil {
		return runtimev2.DatasetResult{
			Error: &runtimev2.DatasetError{
				Kind:    runtimev2.DatasetErrorKind_MISSING_DATASET,
				Message: "normalized dataset provider is nil",
			},
		}
	}

	datasetKey := strings.TrimSpace(ref.Dataset)
	if datasetKey == "" {
		return runtimev2.DatasetResult{
			Error: &runtimev2.DatasetError{
				Kind:    runtimev2.DatasetErrorKind_MISSING_DATASET,
				Message: "dataset ref is missing dataset key",
			},
		}
	}

	version := ref.Version
	var versionPtr *int
	if version > 0 {
		versionPtr = &version
	}

	rows, err := p.getDatasetRows(ctx, datasetKey, versionPtr)
	return runtimeResultFromRowsOrError(rows, err)
}

func (p *NormalizedProvider) getDatasetRows(ctx context.Context, datasetKey string, datasetVersion *int) ([]any, error) {
	if p == nil {
		return nil, engine.DatasetError{Kind: engine.DatasetErrorMissingDataset, Err: errors.New("normalized dataset provider is nil")}
	}

	key := strings.TrimSpace(datasetKey)
	v, err := requireDatasetVersion(key, datasetVersion)
	if err != nil {
		return nil, err
	}
	if p.Q == nil {
		return nil, engine.DatasetError{Kind: engine.DatasetErrorSyncFailed, Err: errors.New("db queries is nil")}
	}

	switch key {
	case "normalized:identities":
		if v == 2 {
			return p.loadIdentitiesV2(ctx)
		}
		return p.loadIdentitiesV1(ctx)
	case "normalized:entitlement_assignments":
		if v == 2 {
			return p.loadEntitlementAssignmentsV2(ctx)
		}
		return p.loadEntitlementAssignmentsV1(ctx)
	default:
		return nil, engine.DatasetError{Kind: engine.DatasetErrorMissingDataset, Err: fmt.Errorf("unsupported dataset key %q", key)}
	}
}

func requireDatasetVersion(datasetKey string, datasetVersion *int) (int, error) {
	v := 1
	if datasetVersion != nil {
		v = *datasetVersion
	}
	if v == 1 || v == 2 {
		return v, nil
	}
	return 0, engine.DatasetError{
		Kind: engine.DatasetErrorMissingDataset,
		Err:  fmt.Errorf("%s: unsupported dataset_version %d", strings.TrimSpace(datasetKey), v),
	}
}

func (p *NormalizedProvider) loadIdentitiesV1(ctx context.Context) ([]any, error) {
	rows, err := p.Q.ListNormalizedIdentitiesV1(ctx)
	if err != nil {
		return nil, engine.DatasetError{Kind: engine.DatasetErrorSyncFailed, Err: err}
	}

	out := make([]any, 0, len(rows))
	for _, row := range rows {
		out = append(out, map[string]any{
			"id":           strconv.FormatInt(row.IdpUserID, 10),
			"external_id":  strings.TrimSpace(row.IdpUserExternalID),
			"email":        strings.TrimSpace(row.IdpUserEmail),
			"display_name": strings.TrimSpace(row.IdpUserDisplayName),
			"status":       normalizeIdentityStatus(strings.TrimSpace(row.IdpUserStatus)),
		})
	}
	return out, nil
}

func (p *NormalizedProvider) loadIdentitiesV2(ctx context.Context) ([]any, error) {
	rows, err := p.Q.ListNormalizedIdentitiesV2(ctx)
	if err != nil {
		return nil, engine.DatasetError{Kind: engine.DatasetErrorSyncFailed, Err: err}
	}

	out := make([]any, 0, len(rows))
	for _, row := range rows {
		out = append(out, map[string]any{
			"id":           strconv.FormatInt(row.IdentityID, 10),
			"kind":         strings.TrimSpace(row.IdentityKind),
			"email":        strings.TrimSpace(row.IdentityEmail),
			"display_name": strings.TrimSpace(row.IdentityDisplayName),
			"managed":      row.IdentityManaged,
			"authoritative_account": map[string]any{
				"source_kind": strings.TrimSpace(row.AuthoritativeSourceKind),
				"source_name": strings.TrimSpace(row.AuthoritativeSourceName),
				"external_id": strings.TrimSpace(row.AuthoritativeExternalID),
			},
		})
	}
	return out, nil
}

func normalizeIdentityStatus(status string) string {
	s := strings.TrimSpace(status)
	switch {
	case strings.EqualFold(s, "ACTIVE"):
		return "active"
	case strings.EqualFold(s, "DEPROVISIONED"):
		return "deprovisioned"
	case strings.EqualFold(s, "inactive"):
		return "inactive"
	case strings.EqualFold(s, "service"):
		return "inactive"
	case strings.EqualFold(s, "bot"):
		return "inactive"
	case s == "":
		return ""
	default:
		return "inactive"
	}
}

func (p *NormalizedProvider) loadEntitlementAssignmentsV1(ctx context.Context) ([]any, error) {
	rows, err := p.Q.ListNormalizedEntitlementAssignmentsV1(ctx)
	if err != nil {
		return nil, engine.DatasetError{Kind: engine.DatasetErrorSyncFailed, Err: err}
	}

	out := make([]any, 0, len(rows))
	for _, row := range rows {
		tags := entitlementTags(strings.TrimSpace(row.EntitlementKind), strings.TrimSpace(row.EntitlementPermission))
		out = append(out, map[string]any{
			"resource_id": fmt.Sprintf("entitlement:%d", row.EntitlementID),
			"identity": map[string]any{
				"id":           strconv.FormatInt(row.IdpUserID, 10),
				"email":        strings.TrimSpace(row.IdpUserEmail),
				"display_name": strings.TrimSpace(row.IdpUserDisplayName),
				"status":       normalizeIdentityStatus(strings.TrimSpace(row.IdpUserStatus)),
			},
			"app_user": map[string]any{
				"source_kind": strings.TrimSpace(row.SourceKind),
				"source_name": strings.TrimSpace(row.SourceName),
				"external_id": strings.TrimSpace(row.AppUserExternalID),
			},
			"entitlement": map[string]any{
				"kind":       strings.TrimSpace(row.EntitlementKind),
				"resource":   strings.TrimSpace(row.EntitlementResource),
				"permission": strings.TrimSpace(row.EntitlementPermission),
				"tags":       tags,
			},
		})
	}
	return out, nil
}

func (p *NormalizedProvider) loadEntitlementAssignmentsV2(ctx context.Context) ([]any, error) {
	rows, err := p.Q.ListNormalizedEntitlementAssignmentsV2(ctx)
	if err != nil {
		return nil, engine.DatasetError{Kind: engine.DatasetErrorSyncFailed, Err: err}
	}

	out := make([]any, 0, len(rows))
	for _, row := range rows {
		tags := entitlementTags(strings.TrimSpace(row.EntitlementKind), strings.TrimSpace(row.EntitlementPermission))
		out = append(out, map[string]any{
			"resource_id": fmt.Sprintf("entitlement:%d", row.EntitlementID),
			"identity": map[string]any{
				"id":           strconv.FormatInt(row.IdentityID, 10),
				"kind":         strings.TrimSpace(row.IdentityKind),
				"email":        strings.TrimSpace(row.IdentityEmail),
				"display_name": strings.TrimSpace(row.IdentityDisplayName),
				"managed":      row.IdentityManaged,
			},
			"app_user": map[string]any{
				"source_kind": strings.TrimSpace(row.SourceKind),
				"source_name": strings.TrimSpace(row.SourceName),
				"external_id": strings.TrimSpace(row.AppUserExternalID),
			},
			"entitlement": map[string]any{
				"kind":       strings.TrimSpace(row.EntitlementKind),
				"resource":   strings.TrimSpace(row.EntitlementResource),
				"permission": strings.TrimSpace(row.EntitlementPermission),
				"tags":       tags,
			},
		})
	}
	return out, nil
}

func entitlementTags(kind, permission string) []string {
	var tags []string
	if kind != "" {
		tags = append(tags, kind)
	}
	lower := strings.ToLower(permission)
	if strings.Contains(lower, "admin") || strings.Contains(lower, "owner") {
		tags = append(tags, "admin")
	}
	return tags
}
