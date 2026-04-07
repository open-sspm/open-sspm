package entra

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	abstractions "github.com/microsoft/kiota-abstractions-go"
	absauth "github.com/microsoft/kiota-abstractions-go/authentication"
	absser "github.com/microsoft/kiota-abstractions-go/serialization"
	msgraphsdkgo "github.com/microsoftgraph/msgraph-sdk-go"
	msgraphcore "github.com/microsoftgraph/msgraph-sdk-go-core"
	"github.com/microsoftgraph/msgraph-sdk-go/applications"
	"github.com/microsoftgraph/msgraph-sdk-go/auditlogs"
	"github.com/microsoftgraph/msgraph-sdk-go/directoryobjects"
	"github.com/microsoftgraph/msgraph-sdk-go/groups"
	msgraphmodels "github.com/microsoftgraph/msgraph-sdk-go/models"
	"github.com/microsoftgraph/msgraph-sdk-go/oauth2permissiongrants"
	"github.com/microsoftgraph/msgraph-sdk-go/rolemanagement"
	"github.com/microsoftgraph/msgraph-sdk-go/serviceprincipals"
	"github.com/microsoftgraph/msgraph-sdk-go/users"
)

const (
	defaultGraphBase   = "https://graph.microsoft.com/v1.0"
	defaultTokenScope  = "https://graph.microsoft.com/.default"
	directoryAuditsTop = int32(200)
	defaultPageSize    = int32(999)
)

type User = msgraphmodels.Userable
type PasswordCredential = msgraphmodels.PasswordCredentialable
type KeyCredential = msgraphmodels.KeyCredentialable
type Application = msgraphmodels.Applicationable
type ServicePrincipal = msgraphmodels.ServicePrincipalable
type AppRole = msgraphmodels.AppRoleable
type Group = msgraphmodels.Groupable
type DirectoryOwner = msgraphmodels.DirectoryObjectable
type DirectoryAuditInitiatedBy = msgraphmodels.AuditActivityInitiatorable
type DirectoryAuditTargetResource = msgraphmodels.TargetResourceable
type DirectoryAuditModifiedProperty = msgraphmodels.ModifiedPropertyable
type DirectoryAuditEvent = msgraphmodels.DirectoryAuditable
type SignInEvent = msgraphmodels.SignInable
type OAuth2PermissionGrant = msgraphmodels.OAuth2PermissionGrantable
type ServicePrincipalAppRoleAssignment = msgraphmodels.AppRoleAssignmentable
type DirectoryRole = msgraphmodels.UnifiedRoleDefinitionable
type DirectoryRoleAssignment = msgraphmodels.UnifiedRoleAssignmentable

type Options struct {
	RequestAdapter abstractions.RequestAdapter
}

type Client struct {
	graph *msgraphsdkgo.GraphServiceClient
}

func New(tenantID, clientID, clientSecret string) (*Client, error) {
	return NewWithOptions(tenantID, clientID, clientSecret, Options{})
}

func NewWithOptions(tenantID, clientID, clientSecret string, opts Options) (*Client, error) {
	if opts.RequestAdapter != nil {
		return newClientFromAdapter(opts.RequestAdapter)
	}

	tenantID = normalizeGUID(tenantID)
	clientID = normalizeGUID(clientID)
	clientSecret = strings.TrimSpace(clientSecret)

	if tenantID == "" {
		return nil, errors.New("entra tenant id is required")
	}
	if clientID == "" {
		return nil, errors.New("entra client id is required")
	}
	if clientSecret == "" {
		return nil, errors.New("entra client secret is required")
	}

	credential, err := azidentity.NewClientSecretCredential(tenantID, clientID, clientSecret, nil)
	if err != nil {
		return nil, fmt.Errorf("create entra credential: %w", err)
	}

	graph, err := msgraphsdkgo.NewGraphServiceClientWithCredentials(credential, []string{defaultTokenScope})
	if err != nil {
		return nil, fmt.Errorf("create entra graph client: %w", err)
	}

	return &Client{graph: graph}, nil
}

func newClientFromAdapter(adapter abstractions.RequestAdapter) (*Client, error) {
	if adapter == nil {
		return nil, errors.New("entra request adapter is required")
	}
	return &Client{graph: msgraphsdkgo.NewGraphServiceClient(adapter)}, nil
}

func newTestClient(graphBaseURL string, httpClient *http.Client) (*Client, error) {
	adapter, err := newStaticTokenRequestAdapter(graphBaseURL, httpClient)
	if err != nil {
		return nil, err
	}
	return newClientFromAdapter(adapter)
}

func newStaticTokenRequestAdapter(graphBaseURL string, httpClient *http.Client) (abstractions.RequestAdapter, error) {
	graphBaseURL = strings.TrimRight(strings.TrimSpace(graphBaseURL), "/")
	if graphBaseURL == "" {
		return nil, errors.New("entra graph base url is required")
	}

	parsed, err := url.Parse(graphBaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse entra graph base url: %w", err)
	}
	validator, err := absauth.NewAllowedHostsValidatorErrorCheck([]string{parsed.Hostname()})
	if err != nil {
		return nil, fmt.Errorf("build entra allowed hosts validator: %w", err)
	}

	tokenProvider := &entraStaticAccessTokenProvider{
		token:     "test-token",
		validator: validator,
	}
	authProvider := absauth.NewBaseBearerTokenAuthenticationProvider(tokenProvider)
	adapter, err := msgraphsdkgo.NewGraphRequestAdapterWithParseNodeFactoryAndSerializationWriterFactoryAndHttpClient(authProvider, nil, nil, httpClient)
	if err != nil {
		return nil, fmt.Errorf("create entra test request adapter: %w", err)
	}
	adapter.SetBaseUrl(graphBaseURL)
	return adapter, nil
}

func (c *Client) ListUsers(ctx context.Context) ([]User, error) {
	result, err := c.graph.Users().Get(ctx, &users.UsersRequestBuilderGetRequestConfiguration{
		QueryParameters: &users.UsersRequestBuilderGetQueryParameters{
			Select: []string{"id", "displayName", "mail", "userPrincipalName", "otherMails", "proxyAddresses", "userType", "accountEnabled", "createdDateTime"},
			Top:    int32Ptr(defaultPageSize),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("list entra users: %w", err)
	}
	return collectPagedItems[User](ctx, result, c.graph.GetAdapter(), msgraphmodels.CreateUserCollectionResponseFromDiscriminatorValue)
}

func (c *Client) LookupUsersByIDs(ctx context.Context, ids []string) ([]User, error) {
	distinctIDs := distinctNonEmptyStrings(ids)
	if len(distinctIDs) == 0 {
		return []User{}, nil
	}

	out := make([]User, 0, len(distinctIDs))
	seen := make(map[string]struct{}, len(distinctIDs))
	for start := 0; start < len(distinctIDs); start += entraUserBatchSize {
		end := min(start+entraUserBatchSize, len(distinctIDs))

		body := directoryobjects.NewGetByIdsPostRequestBody()
		body.SetIds(distinctIDs[start:end])
		body.SetTypes([]string{"user"})

		result, err := c.graph.DirectoryObjects().GetByIds().Post(ctx, body, nil)
		if err != nil {
			return nil, fmt.Errorf("lookup entra users by ids: %w", err)
		}
		if result == nil {
			continue
		}

		for _, object := range result.GetValue() {
			user, ok := object.(msgraphmodels.Userable)
			if !ok {
				continue
			}
			userID := entityID(user)
			if userID == "" {
				continue
			}
			if _, exists := seen[userID]; exists {
				continue
			}
			seen[userID] = struct{}{}
			out = append(out, user)
		}
	}

	return out, nil
}

func (c *Client) ListApplications(ctx context.Context) ([]Application, error) {
	result, err := c.graph.Applications().Get(ctx, &applications.ApplicationsRequestBuilderGetRequestConfiguration{
		QueryParameters: &applications.ApplicationsRequestBuilderGetQueryParameters{
			Select: []string{"id", "appId", "displayName", "publisherDomain", "verifiedPublisher", "createdDateTime", "passwordCredentials", "keyCredentials"},
			Top:    int32Ptr(defaultPageSize),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("list entra applications: %w", err)
	}
	return collectPagedItems[Application](ctx, result, c.graph.GetAdapter(), msgraphmodels.CreateApplicationCollectionResponseFromDiscriminatorValue)
}

func (c *Client) ListServicePrincipals(ctx context.Context) ([]ServicePrincipal, error) {
	result, err := c.graph.ServicePrincipals().Get(ctx, &serviceprincipals.ServicePrincipalsRequestBuilderGetRequestConfiguration{
		QueryParameters: &serviceprincipals.ServicePrincipalsRequestBuilderGetQueryParameters{
			Select: []string{"id", "appId", "displayName", "publisherName", "verifiedPublisher", "accountEnabled", "servicePrincipalType", "createdDateTime", "appRoles", "passwordCredentials", "keyCredentials"},
			Top:    int32Ptr(defaultPageSize),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("list entra service principals: %w", err)
	}
	return collectPagedItems[ServicePrincipal](ctx, result, c.graph.GetAdapter(), msgraphmodels.CreateServicePrincipalCollectionResponseFromDiscriminatorValue)
}

func (c *Client) ListServicePrincipalAssignedTo(ctx context.Context, servicePrincipalID string) ([]ServicePrincipalAppRoleAssignment, error) {
	servicePrincipalID = strings.TrimSpace(servicePrincipalID)
	if servicePrincipalID == "" {
		return nil, errors.New("service principal id is required")
	}

	result, err := c.graph.ServicePrincipals().ByServicePrincipalId(servicePrincipalID).AppRoleAssignedTo().Get(ctx, &serviceprincipals.ItemAppRoleAssignedToRequestBuilderGetRequestConfiguration{
		QueryParameters: &serviceprincipals.ItemAppRoleAssignedToRequestBuilderGetQueryParameters{
			Select: []string{"id", "appRoleId", "createdDateTime", "principalDisplayName", "principalId", "principalType", "resourceDisplayName", "resourceId"},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("list entra service principal assignments %s: %w", servicePrincipalID, err)
	}
	return collectPagedItems[ServicePrincipalAppRoleAssignment](ctx, result, c.graph.GetAdapter(), msgraphmodels.CreateAppRoleAssignmentCollectionResponseFromDiscriminatorValue)
}

func (c *Client) ListGroups(ctx context.Context) ([]Group, error) {
	result, err := c.graph.Groups().Get(ctx, &groups.GroupsRequestBuilderGetRequestConfiguration{
		QueryParameters: &groups.GroupsRequestBuilderGetQueryParameters{
			Select: []string{"id", "displayName", "mail", "mailEnabled", "securityEnabled", "groupTypes"},
			Top:    int32Ptr(defaultPageSize),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("list entra groups: %w", err)
	}
	return collectPagedItems[Group](ctx, result, c.graph.GetAdapter(), msgraphmodels.CreateGroupCollectionResponseFromDiscriminatorValue)
}

func (c *Client) ListGroupTransitiveUserMembers(ctx context.Context, groupID string) ([]User, error) {
	groupID = strings.TrimSpace(groupID)
	if groupID == "" {
		return nil, errors.New("group id is required")
	}

	headers := abstractions.NewRequestHeaders()
	headers.Add("ConsistencyLevel", "eventual")

	result, err := c.graph.Groups().ByGroupId(groupID).TransitiveMembers().GraphUser().Get(ctx, &groups.ItemTransitiveMembersGraphUserRequestBuilderGetRequestConfiguration{
		Headers: headers,
		QueryParameters: &groups.ItemTransitiveMembersGraphUserRequestBuilderGetQueryParameters{
			Count:  boolPtr(true),
			Select: []string{"id", "displayName", "mail", "userPrincipalName", "otherMails", "proxyAddresses", "userType", "accountEnabled", "createdDateTime"},
			Top:    int32Ptr(defaultPageSize),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("list entra transitive group members %s: %w", groupID, err)
	}
	return collectPagedItems[User](ctx, result, c.graph.GetAdapter(), msgraphmodels.CreateUserCollectionResponseFromDiscriminatorValue, headers)
}

func (c *Client) ListDirectoryRoles(ctx context.Context) ([]DirectoryRole, error) {
	result, err := c.graph.RoleManagement().Directory().RoleDefinitions().Get(ctx, &rolemanagement.DirectoryRoleDefinitionsRequestBuilderGetRequestConfiguration{
		QueryParameters: &rolemanagement.DirectoryRoleDefinitionsRequestBuilderGetQueryParameters{
			Select: []string{"id", "displayName", "templateId", "isBuiltIn"},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("list entra directory roles: %w", err)
	}
	return collectPagedItems[DirectoryRole](ctx, result, c.graph.GetAdapter(), msgraphmodels.CreateUnifiedRoleDefinitionCollectionResponseFromDiscriminatorValue)
}

func (c *Client) ListDirectoryRoleAssignments(ctx context.Context) ([]DirectoryRoleAssignment, error) {
	result, err := c.graph.RoleManagement().Directory().RoleAssignments().Get(ctx, &rolemanagement.DirectoryRoleAssignmentsRequestBuilderGetRequestConfiguration{
		QueryParameters: &rolemanagement.DirectoryRoleAssignmentsRequestBuilderGetQueryParameters{
			Expand: []string{"principal"},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("list entra directory role assignments: %w", err)
	}
	return collectPagedItems[DirectoryRoleAssignment](ctx, result, c.graph.GetAdapter(), msgraphmodels.CreateUnifiedRoleAssignmentCollectionResponseFromDiscriminatorValue)
}

func (c *Client) ListApplicationOwners(ctx context.Context, applicationID string) ([]DirectoryOwner, error) {
	applicationID = strings.TrimSpace(applicationID)
	if applicationID == "" {
		return nil, errors.New("application id is required")
	}

	result, err := c.graph.Applications().ByApplicationId(applicationID).Owners().Get(ctx, &applications.ItemOwnersRequestBuilderGetRequestConfiguration{
		QueryParameters: &applications.ItemOwnersRequestBuilderGetQueryParameters{
			Select: []string{"id", "displayName", "mail", "userPrincipalName", "appId"},
			Top:    int32Ptr(defaultPageSize),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("list entra application owners %s: %w", applicationID, err)
	}
	return collectPagedItems[DirectoryOwner](ctx, result, c.graph.GetAdapter(), msgraphmodels.CreateDirectoryObjectCollectionResponseFromDiscriminatorValue)
}

func (c *Client) ListServicePrincipalOwners(ctx context.Context, servicePrincipalID string) ([]DirectoryOwner, error) {
	servicePrincipalID = strings.TrimSpace(servicePrincipalID)
	if servicePrincipalID == "" {
		return nil, errors.New("service principal id is required")
	}

	result, err := c.graph.ServicePrincipals().ByServicePrincipalId(servicePrincipalID).Owners().Get(ctx, &serviceprincipals.ItemOwnersRequestBuilderGetRequestConfiguration{
		QueryParameters: &serviceprincipals.ItemOwnersRequestBuilderGetQueryParameters{
			Select: []string{"id", "displayName", "mail", "userPrincipalName", "appId"},
			Top:    int32Ptr(defaultPageSize),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("list entra service principal owners %s: %w", servicePrincipalID, err)
	}
	return collectPagedItems[DirectoryOwner](ctx, result, c.graph.GetAdapter(), msgraphmodels.CreateDirectoryObjectCollectionResponseFromDiscriminatorValue)
}

func (c *Client) ListDirectoryAudits(ctx context.Context, since *time.Time) ([]DirectoryAuditEvent, error) {
	query := &auditlogs.DirectoryAuditsRequestBuilderGetQueryParameters{
		Orderby: []string{"activityDateTime desc"},
		Select:  []string{"id", "category", "result", "activityDisplayName", "activityDateTime", "initiatedBy", "targetResources"},
		Top:     int32Ptr(directoryAuditsTop),
	}
	if since != nil && !since.IsZero() {
		filter := "activityDateTime ge " + since.UTC().Format(time.RFC3339)
		query.Filter = &filter
	}

	result, err := c.graph.AuditLogs().DirectoryAudits().Get(ctx, &auditlogs.DirectoryAuditsRequestBuilderGetRequestConfiguration{
		QueryParameters: query,
	})
	if err != nil {
		return nil, fmt.Errorf("list entra directory audits: %w", err)
	}
	return collectPagedItems[DirectoryAuditEvent](ctx, result, c.graph.GetAdapter(), msgraphmodels.CreateDirectoryAuditCollectionResponseFromDiscriminatorValue)
}

func (c *Client) ListSignIns(ctx context.Context, since *time.Time) ([]SignInEvent, error) {
	query := &auditlogs.SignInsRequestBuilderGetQueryParameters{
		Orderby: []string{"createdDateTime desc"},
		Select:  []string{"id", "createdDateTime", "appId", "appDisplayName", "resourceDisplayName", "userId", "userDisplayName", "userPrincipalName"},
		Top:     int32Ptr(defaultPageSize),
	}
	if since != nil && !since.IsZero() {
		filter := "createdDateTime ge " + since.UTC().Format(time.RFC3339)
		query.Filter = &filter
	}

	result, err := c.graph.AuditLogs().SignIns().Get(ctx, &auditlogs.SignInsRequestBuilderGetRequestConfiguration{
		QueryParameters: query,
	})
	if err != nil {
		return nil, fmt.Errorf("list entra sign-ins: %w", err)
	}
	return collectPagedItems[SignInEvent](ctx, result, c.graph.GetAdapter(), msgraphmodels.CreateSignInCollectionResponseFromDiscriminatorValue)
}

func (c *Client) ListOAuth2PermissionGrants(ctx context.Context) ([]OAuth2PermissionGrant, error) {
	result, err := c.graph.Oauth2PermissionGrants().Get(ctx, &oauth2permissiongrants.Oauth2PermissionGrantsRequestBuilderGetRequestConfiguration{
		QueryParameters: &oauth2permissiongrants.Oauth2PermissionGrantsRequestBuilderGetQueryParameters{
			Select: []string{"id", "clientId", "consentType", "principalId", "resourceId", "scope"},
			Top:    int32Ptr(defaultPageSize),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("list entra oauth2 permission grants: %w", err)
	}
	return collectPagedItems[OAuth2PermissionGrant](ctx, result, c.graph.GetAdapter(), msgraphmodels.CreateOAuth2PermissionGrantCollectionResponseFromDiscriminatorValue)
}

func collectPagedItems[T any](ctx context.Context, result any, adapter abstractions.RequestAdapter, constructor absser.ParsableFactory, headers ...*abstractions.RequestHeaders) ([]T, error) {
	if result == nil {
		return nil, nil
	}

	items := make([]T, 0)
	iterator, err := msgraphcore.NewPageIterator[T](result, adapter, constructor)
	if err != nil {
		return nil, err
	}
	if len(headers) > 0 && headers[0] != nil {
		iterator.SetHeaders(headers[0])
	}
	err = iterator.Iterate(ctx, func(item T) bool {
		items = append(items, item)
		return true
	})
	if err != nil {
		return nil, err
	}
	return items, nil
}

func normalizeGUID(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.TrimPrefix(s, "{")
	s = strings.TrimSuffix(s, "}")
	return strings.TrimSpace(s)
}

func distinctNonEmptyStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func int32Ptr(v int32) *int32 {
	return &v
}

func boolPtr(v bool) *bool {
	return &v
}

func entityID(entity msgraphmodels.Entityable) string {
	if entity == nil || entity.GetId() == nil {
		return ""
	}
	return strings.TrimSpace(*entity.GetId())
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func timeValueString(value *time.Time) string {
	if value == nil {
		return ""
	}
	return value.UTC().Format(time.RFC3339)
}

type valueStringer interface {
	String() string
}

func uuidValueString[T valueStringer](value *T) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace((*value).String())
}

func bytesValueString(value []byte) string {
	if len(value) == 0 {
		return ""
	}
	return base64.StdEncoding.EncodeToString(value)
}

var graphSerializationInit sync.Once
var graphSerializationInitErr error

func ensureGraphSerializationRegistered() error {
	graphSerializationInit.Do(func() {
		provider := absauth.NewBaseBearerTokenAuthenticationProvider(&entraStaticAccessTokenProvider{validator: &absauth.AllowedHostsValidator{}})
		adapter, err := msgraphsdkgo.NewGraphRequestAdapter(provider)
		if err != nil {
			graphSerializationInitErr = fmt.Errorf("create graph request adapter for serialization registration: %w", err)
			return
		}
		_ = msgraphsdkgo.NewGraphServiceClient(adapter)
	})
	return graphSerializationInitErr
}

func serializeSDKModel(model absser.Parsable) ([]byte, error) {
	if model == nil {
		return nil, nil
	}
	if err := ensureGraphSerializationRegistered(); err != nil {
		return nil, err
	}
	raw, err := absser.Serialize("application/json", model)
	if err != nil {
		return nil, fmt.Errorf("serialize graph model %T: %w", model, err)
	}
	return raw, nil
}

func mergeSerializedSDKModel(model absser.Parsable, extras map[string]any) ([]byte, error) {
	raw, err := serializeSDKModel(model)
	if err != nil {
		return nil, err
	}
	if len(extras) == 0 {
		return raw, nil
	}

	payload := map[string]any{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &payload); err != nil {
			return nil, fmt.Errorf("decode serialized graph model %T for merge: %w", model, err)
		}
	}
	for key, value := range extras {
		payload[key] = value
	}
	merged, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("encode merged graph model %T: %w", model, err)
	}
	return merged, nil
}

type entraStaticAccessTokenProvider struct {
	token     string
	validator *absauth.AllowedHostsValidator
}

func (p *entraStaticAccessTokenProvider) GetAuthorizationToken(context.Context, *url.URL, map[string]interface{}) (string, error) {
	return p.token, nil
}

func (p *entraStaticAccessTokenProvider) GetAllowedHostsValidator() *absauth.AllowedHostsValidator {
	if p.validator == nil {
		return &absauth.AllowedHostsValidator{}
	}
	return p.validator
}
