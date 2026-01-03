package datasets

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	runtimev1 "github.com/open-sspm/open-sspm-spec/gen/go/opensspm/runtime/v1"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/rules/engine"
)

type NormalizedProvider struct {
	Q *gen.Queries

	identitiesOnce sync.Once
	identitiesRows []any
	identitiesErr  error

	assignmentsOnce sync.Once
	assignmentsRows []any
	assignmentsErr  error
}

func (p *NormalizedProvider) Capabilities(ctx context.Context) []runtimev1.DatasetRef {
	_ = ctx
	if p == nil {
		return nil
	}
	out := make([]runtimev1.DatasetRef, 0, len(normalizedCapabilitiesV1))
	for _, ds := range normalizedCapabilitiesV1 {
		out = append(out, runtimev1.DatasetRef{Dataset: ds, Version: 1})
	}
	return out
}

func (p *NormalizedProvider) GetDataset(ctx context.Context, eval runtimev1.EvalContext, ref runtimev1.DatasetRef) runtimev1.DatasetResult {
	_ = eval

	if p == nil {
		return runtimev1.DatasetResult{
			Error: &runtimev1.DatasetError{
				Kind:    runtimev1.DatasetErrorKind_MISSING_DATASET,
				Message: "normalized dataset provider is nil",
			},
		}
	}

	datasetKey := strings.TrimSpace(ref.Dataset)
	if datasetKey == "" {
		return runtimev1.DatasetResult{
			Error: &runtimev1.DatasetError{
				Kind:    runtimev1.DatasetErrorKind_MISSING_DATASET,
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
	if err := requireDatasetVersion(key, datasetVersion); err != nil {
		return nil, err
	}

	switch key {
	case "normalized:identities":
		p.identitiesOnce.Do(func() {
			p.identitiesRows, p.identitiesErr = p.loadIdentities(ctx)
		})
		if p.identitiesErr != nil {
			return nil, engine.DatasetError{Kind: engine.DatasetErrorSyncFailed, Err: p.identitiesErr}
		}
		if p.identitiesRows == nil {
			return []any{}, nil
		}
		return p.identitiesRows, nil

	case "normalized:entitlement_assignments":
		p.assignmentsOnce.Do(func() {
			p.assignmentsRows, p.assignmentsErr = p.loadEntitlementAssignments(ctx)
		})
		if p.assignmentsErr != nil {
			return nil, engine.DatasetError{Kind: engine.DatasetErrorSyncFailed, Err: p.assignmentsErr}
		}
		if p.assignmentsRows == nil {
			return []any{}, nil
		}
		return p.assignmentsRows, nil

	default:
		return nil, engine.DatasetError{Kind: engine.DatasetErrorMissingDataset, Err: fmt.Errorf("unsupported dataset key %q", key)}
	}
}

func requireDatasetVersion(datasetKey string, datasetVersion *int) error {
	v := 1
	if datasetVersion != nil {
		v = *datasetVersion
	}
	if v == 1 {
		return nil
	}
	return engine.DatasetError{
		Kind: engine.DatasetErrorMissingDataset,
		Err:  fmt.Errorf("%s: unsupported dataset_version %d", strings.TrimSpace(datasetKey), v),
	}
}

func (p *NormalizedProvider) loadIdentities(ctx context.Context) ([]any, error) {
	if p.Q == nil {
		return nil, errors.New("db queries is nil")
	}

	users, err := p.Q.ListIdPUsers(ctx)
	if err != nil {
		return nil, err
	}

	rows := make([]any, 0, len(users))
	for _, u := range users {
		id := strconv.FormatInt(u.ID, 10)
		rows = append(rows, map[string]any{
			"id":           id,
			"external_id":  strings.TrimSpace(u.ExternalID),
			"email":        strings.TrimSpace(u.Email),
			"display_name": strings.TrimSpace(u.DisplayName),
			"status":       normalizeIdentityStatus(strings.TrimSpace(u.Status)),
		})
	}
	return rows, nil
}

func normalizeIdentityStatus(status string) string {
	s := strings.TrimSpace(status)
	switch {
	case strings.EqualFold(s, "ACTIVE"):
		return "active"
	case strings.EqualFold(s, "DEPROVISIONED"):
		return "deprovisioned"
	case s == "":
		return ""
	default:
		return "inactive"
	}
}

func (p *NormalizedProvider) loadEntitlementAssignments(ctx context.Context) ([]any, error) {
	if p.Q == nil {
		return nil, errors.New("db queries is nil")
	}

	rows, err := p.Q.ListNormalizedEntitlementAssignments(ctx)
	if err != nil {
		return nil, err
	}

	out := make([]any, 0, len(rows))
	for _, r := range rows {
		id := strconv.FormatInt(r.IdpUserID, 10)
		tags := entitlementTags(strings.TrimSpace(r.EntitlementKind), strings.TrimSpace(r.EntitlementPermission))

		out = append(out, map[string]any{
			"resource_id": fmt.Sprintf("entitlement:%d", r.EntitlementID),
			"identity": map[string]any{
				"id":           id,
				"email":        strings.TrimSpace(r.IdpUserEmail),
				"display_name": strings.TrimSpace(r.IdpUserDisplayName),
				"status":       normalizeIdentityStatus(strings.TrimSpace(r.IdpUserStatus)),
			},
			"app_user": map[string]any{
				"source_kind": strings.TrimSpace(r.SourceKind),
				"source_name": strings.TrimSpace(r.SourceName),
				"external_id": strings.TrimSpace(r.AppUserExternalID),
			},
			"entitlement": map[string]any{
				"kind":       strings.TrimSpace(r.EntitlementKind),
				"resource":   strings.TrimSpace(r.EntitlementResource),
				"permission": strings.TrimSpace(r.EntitlementPermission),
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
