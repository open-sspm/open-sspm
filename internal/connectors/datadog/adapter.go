package datadog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	datadogsdk "github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	datadogv2 "github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/connectors/registry"
)

const (
	datadogPageSize    = 100
	datadogHTTPTimeout = 120 * time.Second
	datadogMaxRetries  = 3
)

type datadogAdapter interface {
	ListAccounts(context.Context) ([]Account, error)
	ListRoles(context.Context) ([]Role, error)
	ListRoleMembers(context.Context, string) ([]string, error)
}

type Account struct {
	ExternalID     string
	Email          string
	DisplayName    string
	Status         string
	AccountKind    string
	EntityCategory string
	LastLoginAt    *time.Time
	RawJSON        []byte
}

type Role struct {
	ID      string
	Name    string
	RawJSON []byte
}

type sdkAdapter struct {
	client *datadogsdk.APIClient
	site   string
	apiKey string
	appKey string
}

func newSDKAdapter(cfg configstore.DatadogConfig) (*sdkAdapter, error) {
	cfg = cfg.Normalized()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	sdkCfg := datadogsdk.NewConfiguration()
	sdkCfg.HTTPClient = &http.Client{Timeout: datadogHTTPTimeout}
	sdkCfg.UserAgent = "open-sspm"
	sdkCfg.RetryConfiguration.EnableRetry = true
	sdkCfg.RetryConfiguration.MaxRetries = datadogMaxRetries
	sdkCfg.RetryConfiguration.HTTPRetryTimeout = datadogHTTPTimeout

	return &sdkAdapter{
		client: datadogsdk.NewAPIClient(sdkCfg),
		site:   cfg.Site,
		apiKey: cfg.APIKey,
		appKey: cfg.AppKey,
	}, nil
}

func (a *sdkAdapter) ListAccounts(ctx context.Context) ([]Account, error) {
	api := datadogv2.NewUsersApi(a.client)
	params := *datadogv2.NewListUsersOptionalParameters().WithPageSize(datadogPageSize)
	items, cancel := api.ListUsersWithPagination(a.requestContext(ctx), params)
	defer cancel()

	out := make([]Account, 0)
	for item := range items {
		if item.Error != nil {
			return nil, formatDatadogAPIError("list datadog accounts", item.Error)
		}
		account, err := mapDatadogAccount(item.Item)
		if err != nil {
			return nil, err
		}
		if strings.TrimSpace(account.ExternalID) == "" {
			continue
		}
		out = append(out, account)
	}
	return out, nil
}

func (a *sdkAdapter) ListRoles(ctx context.Context) ([]Role, error) {
	api := datadogv2.NewRolesApi(a.client)
	out := make([]Role, 0)

	for page := int64(0); ; page++ {
		params := *datadogv2.NewListRolesOptionalParameters().
			WithPageSize(datadogPageSize).
			WithPageNumber(page)
		resp, _, err := api.ListRoles(a.requestContext(ctx), params)
		if err != nil {
			return nil, formatDatadogAPIError("list datadog roles", err)
		}
		if len(resp.Data) == 0 {
			break
		}
		for _, item := range resp.Data {
			role, err := mapDatadogRole(item)
			if err != nil {
				return nil, err
			}
			if strings.TrimSpace(role.ID) == "" {
				continue
			}
			out = append(out, role)
		}
	}

	return out, nil
}

func (a *sdkAdapter) ListRoleMembers(ctx context.Context, roleID string) ([]string, error) {
	roleID = strings.TrimSpace(roleID)
	if roleID == "" {
		return nil, errors.New("datadog role id is required")
	}

	api := datadogv2.NewRolesApi(a.client)
	out := make([]string, 0)

	for page := int64(0); ; page++ {
		params := *datadogv2.NewListRoleUsersOptionalParameters().
			WithPageSize(datadogPageSize).
			WithPageNumber(page)
		resp, _, err := api.ListRoleUsers(a.requestContext(ctx), roleID, params)
		if err != nil {
			return nil, formatDatadogAPIError("list datadog role members", err)
		}
		if len(resp.Data) == 0 {
			break
		}
		for _, item := range resp.Data {
			externalID := datadogAccountExternalID(strings.TrimSpace(item.GetId()), datadogUserIsServiceAccount(item))
			if externalID == "" {
				continue
			}
			out = append(out, externalID)
		}
	}

	return out, nil
}

func (a *sdkAdapter) requestContext(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = context.WithValue(ctx, datadogsdk.ContextAPIKeys, map[string]datadogsdk.APIKey{
		"apiKeyAuth": {Key: a.apiKey},
		"appKeyAuth": {Key: a.appKey},
	})
	ctx = context.WithValue(ctx, datadogsdk.ContextServerIndex, 2)
	ctx = context.WithValue(ctx, datadogsdk.ContextServerVariables, map[string]string{"site": a.site})
	return ctx
}

func formatDatadogAPIError(prefix string, err error) error {
	if err == nil {
		return nil
	}

	var apiErr datadogsdk.GenericOpenAPIError
	if !errors.As(err, &apiErr) {
		return fmt.Errorf("%s: %w", prefix, err)
	}

	status := strings.TrimSpace(apiErr.Error())
	message := datadogAPIErrorMessage(apiErr.Body())
	if status == "" || message == "" {
		return fmt.Errorf("%s: %w", prefix, err)
	}

	return fmt.Errorf("%s: %s: %s", prefix, status, message)
}

func datadogAPIErrorMessage(body []byte) string {
	var payload struct {
		Errors  []string `json:"errors"`
		Error   string   `json:"error"`
		Message string   `json:"message"`
	}
	if err := json.Unmarshal(body, &payload); err == nil {
		for _, raw := range payload.Errors {
			if msg := strings.TrimSpace(raw); msg != "" {
				return msg
			}
		}
		if msg := strings.TrimSpace(payload.Error); msg != "" {
			return msg
		}
		if msg := strings.TrimSpace(payload.Message); msg != "" {
			return msg
		}
	}

	message := strings.TrimSpace(string(body))
	if message == "" {
		return ""
	}
	return strings.Join(strings.Fields(message), " ")
}

func mapDatadogAccount(item datadogv2.User) (Account, error) {
	id := strings.TrimSpace(item.GetId())
	attrs, _ := item.GetAttributesOk()

	displayName := datadogUserDisplayName(id, attrs)
	email := datadogUserEmail(attrs)
	serviceAccount := datadogUserIsServiceAccount(item)
	status := datadogUserStatusValue(attrs)
	lastLoginAt := datadogUserLastLoginAt(attrs)

	if serviceAccount && email == "" {
		email = displayName
	}

	if serviceAccount {
		rawJSON, err := json.Marshal(struct {
			Name   string `json:"name"`
			Email  string `json:"email,omitempty"`
			Status string `json:"status,omitempty"`
		}{
			Name:   displayName,
			Email:  email,
			Status: status,
		})
		if err != nil {
			return Account{}, err
		}
		return Account{
			ExternalID:     datadogAccountExternalID(id, true),
			Email:          email,
			DisplayName:    displayName,
			Status:         status,
			AccountKind:    registry.AccountKindService,
			EntityCategory: registry.EntityCategoryServiceAccount,
			RawJSON:        rawJSON,
		}, nil
	}

	rawJSON, err := json.Marshal(struct {
		UserName    string     `json:"user_name"`
		Status      string     `json:"status"`
		LastLoginAt *time.Time `json:"last_login_at,omitempty"`
	}{
		UserName:    displayName,
		Status:      status,
		LastLoginAt: lastLoginAt,
	})
	if err != nil {
		return Account{}, err
	}

	return Account{
		ExternalID:     datadogAccountExternalID(id, false),
		Email:          email,
		DisplayName:    displayName,
		Status:         status,
		AccountKind:    datadogUserAccountKind(displayName),
		EntityCategory: registry.EntityCategoryUser,
		LastLoginAt:    lastLoginAt,
		RawJSON:        rawJSON,
	}, nil
}

func mapDatadogRole(item datadogv2.Role) (Role, error) {
	id := strings.TrimSpace(item.GetId())
	attrs, _ := item.GetAttributesOk()
	name := ""
	if attrs != nil {
		name = strings.TrimSpace(attrs.GetName())
	}
	if name == "" {
		name = id
	}

	rawJSON, err := json.Marshal(struct {
		Name string `json:"name"`
	}{
		Name: name,
	})
	if err != nil {
		return Role{}, err
	}

	return Role{
		ID:      id,
		Name:    name,
		RawJSON: rawJSON,
	}, nil
}

func datadogUserDisplayName(id string, attrs *datadogv2.UserAttributes) string {
	if attrs == nil {
		return strings.TrimSpace(id)
	}
	if handle := strings.TrimSpace(attrs.GetHandle()); handle != "" {
		return handle
	}
	if name := strings.TrimSpace(attrs.GetName()); name != "" {
		return name
	}
	if email := strings.TrimSpace(attrs.GetEmail()); email != "" {
		return email
	}
	return strings.TrimSpace(id)
}

func datadogUserEmail(attrs *datadogv2.UserAttributes) string {
	if attrs == nil {
		return ""
	}
	email := strings.TrimSpace(attrs.GetEmail())
	if email != "" {
		return email
	}
	return strings.TrimSpace(attrs.GetHandle())
}

func datadogUserIsServiceAccount(item datadogv2.User) bool {
	attrs, ok := item.GetAttributesOk()
	if !ok || attrs == nil {
		return false
	}
	serviceAccount, ok := attrs.GetServiceAccountOk()
	return ok && serviceAccount != nil && *serviceAccount
}

func datadogUserStatusValue(attrs *datadogv2.UserAttributes) string {
	if attrs == nil {
		return ""
	}
	if status := strings.TrimSpace(attrs.GetStatus()); status != "" {
		return status
	}
	disabled, ok := attrs.GetDisabledOk()
	if ok && disabled != nil {
		if *disabled {
			return "Inactive"
		}
		return "Active"
	}
	return ""
}

func datadogUserLastLoginAt(attrs *datadogv2.UserAttributes) *time.Time {
	if attrs == nil {
		return nil
	}
	return attrs.LastLoginTime.Get()
}
