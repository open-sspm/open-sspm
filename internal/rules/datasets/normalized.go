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
	ListNormalizedIdentities(context.Context) ([]gen.ListNormalizedIdentitiesRow, error)
	ListNormalizedEntitlementAssignments(context.Context) ([]gen.ListNormalizedEntitlementAssignmentsRow, error)
}

func (p *NormalizedProvider) Capabilities(ctx context.Context) []runtimev2.DatasetRef {
	_ = ctx
	if p == nil {
		return nil
	}
	out := make([]runtimev2.DatasetRef, 0, len(normalizedCapabilities))
	for _, ds := range normalizedCapabilities {
		out = append(out, runtimev2.DatasetRef{Dataset: ds, Version: 1})
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
	if version > 1 {
		return nil, engine.DatasetError{Kind: engine.DatasetErrorMissingDataset, Err: fmt.Errorf("%s: unsupported dataset_version %d", datasetKey, version)}
	}
	if p.Q == nil {
		return nil, engine.DatasetError{Kind: engine.DatasetErrorSyncFailed, Err: errors.New("db queries is nil")}
	}

	switch datasetKey {
	case "normalized:identities":
		return p.loadIdentities(ctx)
	case "normalized:entitlement_assignments":
		return p.loadEntitlementAssignments(ctx)
	default:
		return nil, engine.DatasetError{Kind: engine.DatasetErrorMissingDataset, Err: fmt.Errorf("unsupported dataset key %q", datasetKey)}
	}
}

func (p *NormalizedProvider) loadIdentities(ctx context.Context) ([]any, error) {
	rows, err := p.Q.ListNormalizedIdentities(ctx)
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

func (p *NormalizedProvider) loadEntitlementAssignments(ctx context.Context) ([]any, error) {
	rows, err := p.Q.ListNormalizedEntitlementAssignments(ctx)
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
