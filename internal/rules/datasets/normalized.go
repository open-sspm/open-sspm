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
	ListNormalizedIdentitiesV3(context.Context) ([]gen.ListNormalizedIdentitiesV3Row, error)
	ListNormalizedEntitlementAssignmentsV1(context.Context) ([]gen.ListNormalizedEntitlementAssignmentsV1Row, error)
	ListNormalizedEntitlementAssignmentsV2(context.Context) ([]gen.ListNormalizedEntitlementAssignmentsV2Row, error)
	ListNormalizedEntitlementAssignmentsV3(context.Context) ([]gen.ListNormalizedEntitlementAssignmentsV3Row, error)
}

func (p *NormalizedProvider) Capabilities(ctx context.Context) []runtimev2.DatasetRef {
	_ = ctx
	if p == nil {
		return nil
	}
	out := make([]runtimev2.DatasetRef, 0, len(normalizedCapabilities)*len(normalizedDatasetVersions))
	for _, ds := range normalizedCapabilities {
		for _, version := range normalizedDatasetVersions {
			out = append(out, runtimev2.DatasetRef{Dataset: ds, Version: version})
		}
	}
	return out
}

func (p *NormalizedProvider) GetDataset(ctx context.Context, eval runtimev2.EvalContext, ref runtimev2.DatasetRef) runtimev2.DatasetResult {
	_ = eval

	rows, err := p.getDatasetRows(ctx, strings.TrimSpace(ref.Dataset), ref.Version)
	return runtimeResultFromRowsOrError(rows, err)
}

func (p *NormalizedProvider) getDatasetRows(ctx context.Context, datasetKey string, version int) ([]any, error) {
	if p == nil {
		return nil, engine.DatasetError{Kind: engine.DatasetErrorMissingDataset, Err: errors.New("normalized dataset provider is nil")}
	}
	if datasetKey == "" {
		return nil, engine.DatasetError{Kind: engine.DatasetErrorMissingDataset, Err: errors.New("dataset ref is missing dataset key")}
	}
	version, err := requireNormalizedDatasetVersion(datasetKey, version)
	if err != nil {
		return nil, err
	}
	if p.Q == nil {
		return nil, engine.DatasetError{Kind: engine.DatasetErrorSyncFailed, Err: errors.New("db queries is nil")}
	}

	switch datasetKey {
	case "normalized:identities":
		switch version {
		case 1:
			return p.loadIdentitiesV1(ctx)
		case 2:
			return p.loadIdentitiesV2(ctx)
		default:
			return p.loadIdentitiesV3(ctx)
		}
	case "normalized:entitlement_assignments":
		switch version {
		case 1:
			return p.loadEntitlementAssignmentsV1(ctx)
		case 2:
			return p.loadEntitlementAssignmentsV2(ctx)
		default:
			return p.loadEntitlementAssignmentsV3(ctx)
		}
	default:
		return nil, engine.DatasetError{Kind: engine.DatasetErrorMissingDataset, Err: fmt.Errorf("unsupported dataset key %q", datasetKey)}
	}
}

func requireNormalizedDatasetVersion(datasetKey string, version int) (int, error) {
	if version == 0 {
		version = 1
	}
	for _, supported := range normalizedDatasetVersions {
		if version == supported {
			return version, nil
		}
	}
	return 0, engine.DatasetError{
		Kind: engine.DatasetErrorMissingDataset,
		Err:  fmt.Errorf("%s: unsupported dataset_version %d", strings.TrimSpace(datasetKey), version),
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
			"id":           strconv.FormatInt(row.IdentityID, 10),
			"external_id":  strings.TrimSpace(row.IdentityExternalID),
			"email":        strings.TrimSpace(row.IdentityEmail),
			"display_name": strings.TrimSpace(row.IdentityDisplayName),
			"status":       normalizeIdentityStatus(strings.TrimSpace(row.IdentityStatus)),
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

func (p *NormalizedProvider) loadIdentitiesV3(ctx context.Context) ([]any, error) {
	rows, err := p.Q.ListNormalizedIdentitiesV3(ctx)
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
			"posture":      strings.TrimSpace(row.IdentityPosture),
			"anchor": map[string]any{
				"state":       strings.TrimSpace(row.IdentityAnchorState),
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
				"id":           strconv.FormatInt(row.IdentityID, 10),
				"email":        strings.TrimSpace(row.IdentityEmail),
				"display_name": strings.TrimSpace(row.IdentityDisplayName),
				"status":       normalizeIdentityStatus(strings.TrimSpace(row.IdentityStatus)),
			},
			"account": map[string]any{
				"source_kind": strings.TrimSpace(row.AccountSourceKind),
				"source_name": strings.TrimSpace(row.AccountSourceName),
				"external_id": strings.TrimSpace(row.AccountExternalID),
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
			"account": map[string]any{
				"source_kind": strings.TrimSpace(row.AccountSourceKind),
				"source_name": strings.TrimSpace(row.AccountSourceName),
				"external_id": strings.TrimSpace(row.AccountExternalID),
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

func (p *NormalizedProvider) loadEntitlementAssignmentsV3(ctx context.Context) ([]any, error) {
	rows, err := p.Q.ListNormalizedEntitlementAssignmentsV3(ctx)
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
				"posture":      strings.TrimSpace(row.IdentityPosture),
				"anchor": map[string]any{
					"state":       strings.TrimSpace(row.IdentityAnchorState),
					"source_kind": strings.TrimSpace(row.AuthoritativeSourceKind),
					"source_name": strings.TrimSpace(row.AuthoritativeSourceName),
					"external_id": strings.TrimSpace(row.AuthoritativeExternalID),
				},
			},
			"account": map[string]any{
				"source_kind": strings.TrimSpace(row.AccountSourceKind),
				"source_name": strings.TrimSpace(row.AccountSourceName),
				"external_id": strings.TrimSpace(row.AccountExternalID),
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
