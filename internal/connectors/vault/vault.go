package vault

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	neturl "net/url"
	"slices"
	"strings"
	"time"

	vaultapi "github.com/hashicorp/vault/api"
)

const (
	vaultAuthTypeToken   = "token"
	vaultAuthTypeAppRole = "approle"
)

type Options struct {
	Address          string
	Namespace        string
	AuthType         string
	Token            string
	AppRoleMountPath string
	AppRoleRoleID    string
	AppRoleSecretID  string
	TLSSkipVerify    bool
	TLSCACertPEM     string
}

type Entity struct {
	ID       string
	Name     string
	Disabled bool
	Policies []string
	Metadata map[string]string
	Aliases  []EntityAlias
	RawJSON  []byte
}

type EntityAlias struct {
	Name      string
	MountType string
	Metadata  map[string]string
}

type Group struct {
	ID              string
	Name            string
	Policies        []string
	MemberEntityIDs []string
	Metadata        map[string]string
	RawJSON         []byte
}

type Policy struct {
	Name string
}

type AuthMount struct {
	Path        string
	Type        string
	Description string
	RawJSON     []byte
}

type SecretsMount struct {
	Path        string
	Type        string
	Description string
	RawJSON     []byte
}

type AuthRole struct {
	Name      string
	MountPath string
	AuthType  string
	Policies  []string
	RawJSON   []byte
}

type Client struct {
	client      *vaultapi.Client
	namespace   string
	addressHost string
}

func New(opts Options) (*Client, error) {
	address := strings.TrimSpace(opts.Address)
	if address == "" {
		return nil, errors.New("vault address is required")
	}
	authType := strings.ToLower(strings.TrimSpace(opts.AuthType))
	if authType == "" {
		authType = vaultAuthTypeToken
	}

	cfg := vaultapi.DefaultConfig()
	cfg.Address = address
	cfg.HttpClient = &http.Client{
		Timeout:   120 * time.Second,
		Transport: buildHTTPTransport(opts.TLSSkipVerify, strings.TrimSpace(opts.TLSCACertPEM)),
	}
	addressHost := ""
	if parsed, err := neturl.Parse(address); err == nil {
		addressHost = strings.ToLower(strings.TrimSpace(parsed.Hostname()))
	}

	client, err := vaultapi.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("vault client setup: %w", err)
	}
	namespace := strings.TrimSpace(opts.Namespace)
	if namespace != "" {
		client.SetNamespace(namespace)
	}

	switch authType {
	case vaultAuthTypeToken:
		token := strings.TrimSpace(opts.Token)
		if token == "" {
			return nil, errors.New("vault token is required")
		}
		client.SetToken(token)
	case vaultAuthTypeAppRole:
		roleID := strings.TrimSpace(opts.AppRoleRoleID)
		secretID := strings.TrimSpace(opts.AppRoleSecretID)
		mountPath := normalizeMountPath(opts.AppRoleMountPath)
		if mountPath == "" {
			mountPath = "approle"
		}
		if roleID == "" {
			return nil, errors.New("vault AppRole role ID is required")
		}
		if secretID == "" {
			return nil, errors.New("vault AppRole secret ID is required")
		}
		loginPath := "auth/" + mountPath + "/login"
		secret, err := client.Logical().Write(loginPath, map[string]any{
			"role_id":   roleID,
			"secret_id": secretID,
		})
		if err != nil {
			return nil, fmt.Errorf("vault approle login at %s: %w", loginPath, err)
		}
		if secret == nil || secret.Auth == nil || strings.TrimSpace(secret.Auth.ClientToken) == "" {
			return nil, errors.New("vault approle login succeeded without client token")
		}
		client.SetToken(secret.Auth.ClientToken)
	default:
		return nil, errors.New("vault auth type is invalid")
	}

	return &Client{
		client:      client,
		namespace:   namespace,
		addressHost: addressHost,
	}, nil
}

func (c *Client) ListEntities(ctx context.Context) ([]Entity, error) {
	ids, err := c.listKeys(ctx, "identity/entity/id")
	if err != nil {
		return nil, err
	}
	out := make([]Entity, 0, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		data, err := c.read(ctx, "identity/entity/id/"+pathEscape(id))
		if err != nil {
			return nil, err
		}
		entity := Entity{
			ID:       firstNonEmpty(mapString(data, "id"), id),
			Name:     mapString(data, "name"),
			Disabled: mapBool(data, "disabled"),
			Policies: dedupeNonEmpty(stringSlice(data["policies"])),
			Metadata: stringMap(data["metadata"]),
			Aliases:  parseEntityAliases(data["aliases"]),
			RawJSON:  marshalJSON(data),
		}
		out = append(out, entity)
	}
	return out, nil
}

func (c *Client) ListGroups(ctx context.Context) ([]Group, error) {
	ids, err := c.listKeys(ctx, "identity/group/id")
	if err != nil {
		return nil, err
	}
	out := make([]Group, 0, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		data, err := c.read(ctx, "identity/group/id/"+pathEscape(id))
		if err != nil {
			return nil, err
		}
		group := Group{
			ID:              firstNonEmpty(mapString(data, "id"), id),
			Name:            mapString(data, "name"),
			Policies:        dedupeNonEmpty(stringSlice(data["policies"])),
			MemberEntityIDs: dedupeNonEmpty(stringSlice(data["member_entity_ids"])),
			Metadata:        stringMap(data["metadata"]),
			RawJSON:         marshalJSON(data),
		}
		out = append(out, group)
	}
	return out, nil
}

func (c *Client) ListACLPolicies(ctx context.Context) ([]Policy, error) {
	names, err := c.client.Sys().ListPoliciesWithContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("vault list ACL policies: %w", c.withNamespaceHint(err))
	}
	out := make([]Policy, 0, len(names))
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		out = append(out, Policy{Name: name})
	}
	slices.SortFunc(out, func(a, b Policy) int { return strings.Compare(a.Name, b.Name) })
	return out, nil
}

func (c *Client) ListAuthMounts(ctx context.Context) ([]AuthMount, error) {
	mounts, err := c.client.Sys().ListAuthWithContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("vault list auth mounts: %w", c.withNamespaceHint(err))
	}
	out := make([]AuthMount, 0, len(mounts))
	for path, mount := range mounts {
		if mount == nil {
			continue
		}
		normalizedPath := normalizeMountPath(path)
		if normalizedPath == "" {
			continue
		}
		out = append(out, AuthMount{
			Path:        normalizedPath,
			Type:        strings.TrimSpace(mount.Type),
			Description: strings.TrimSpace(mount.Description),
			RawJSON: marshalJSON(map[string]any{
				"path":        normalizedPath,
				"type":        mount.Type,
				"description": mount.Description,
				"local":       mount.Local,
				"seal_wrap":   mount.SealWrap,
				"options":     mount.Options,
				"config":      mount.Config,
			}),
		})
	}
	slices.SortFunc(out, func(a, b AuthMount) int { return strings.Compare(a.Path, b.Path) })
	return out, nil
}

func (c *Client) ListSecretsMounts(ctx context.Context) ([]SecretsMount, error) {
	mounts, err := c.client.Sys().ListMountsWithContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("vault list secrets mounts: %w", c.withNamespaceHint(err))
	}
	out := make([]SecretsMount, 0, len(mounts))
	for path, mount := range mounts {
		if mount == nil {
			continue
		}
		normalizedPath := normalizeMountPath(path)
		if normalizedPath == "" {
			continue
		}
		out = append(out, SecretsMount{
			Path:        normalizedPath,
			Type:        strings.TrimSpace(mount.Type),
			Description: strings.TrimSpace(mount.Description),
			RawJSON: marshalJSON(map[string]any{
				"path":                    normalizedPath,
				"type":                    mount.Type,
				"description":             mount.Description,
				"external_entropy_access": mount.ExternalEntropyAccess,
				"local":                   mount.Local,
				"options":                 mount.Options,
				"seal_wrap":               mount.SealWrap,
				"config":                  mount.Config,
			}),
		})
	}
	slices.SortFunc(out, func(a, b SecretsMount) int { return strings.Compare(a.Path, b.Path) })
	return out, nil
}

func (c *Client) ListAuthRoles(ctx context.Context, mounts []AuthMount) ([]AuthRole, error) {
	out := make([]AuthRole, 0)
	for _, mount := range mounts {
		if !supportsAuthRoles(mount.Type) {
			continue
		}
		rolePath := "auth/" + mount.Path + "/role"
		roleNames, err := c.listKeys(ctx, rolePath)
		if err != nil {
			return nil, fmt.Errorf("vault list auth roles for mount %s: %w", mount.Path, err)
		}
		for _, roleName := range roleNames {
			roleName = strings.TrimSpace(roleName)
			if roleName == "" {
				continue
			}
			data, err := c.read(ctx, rolePath+"/"+pathEscape(roleName))
			if err != nil {
				return nil, fmt.Errorf("vault read auth role %s on mount %s: %w", roleName, mount.Path, err)
			}
			role := AuthRole{
				Name:      roleName,
				MountPath: mount.Path,
				AuthType:  strings.TrimSpace(mount.Type),
				Policies: dedupeNonEmpty(append(
					stringSlice(data["token_policies"]),
					stringSlice(data["policies"])...,
				)),
				RawJSON: marshalJSON(map[string]any{
					"mount_path": mount.Path,
					"auth_type":  mount.Type,
					"role_name":  roleName,
					"role":       data,
				}),
			}
			out = append(out, role)
		}
	}
	slices.SortFunc(out, func(a, b AuthRole) int {
		left := a.AuthType + ":" + a.MountPath + ":" + a.Name
		right := b.AuthType + ":" + b.MountPath + ":" + b.Name
		return strings.Compare(left, right)
	})
	return out, nil
}

func (c *Client) listKeys(ctx context.Context, path string) ([]string, error) {
	secret, err := c.client.Logical().ListWithContext(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("vault list %s: %w", path, c.withNamespaceHint(err))
	}
	if secret == nil || secret.Data == nil {
		return nil, nil
	}

	if raw, ok := secret.Data["keys"]; ok {
		return dedupeNonEmpty(stringSlice(raw)), nil
	}
	if raw, ok := secret.Data["key_info"]; ok {
		keyInfo, ok := raw.(map[string]any)
		if !ok {
			return nil, nil
		}
		keys := make([]string, 0, len(keyInfo))
		for key := range keyInfo {
			keys = append(keys, key)
		}
		return dedupeNonEmpty(keys), nil
	}
	if raw, ok := secret.Data["policies"]; ok {
		return dedupeNonEmpty(stringSlice(raw)), nil
	}
	return nil, nil
}

func (c *Client) read(ctx context.Context, path string) (map[string]any, error) {
	secret, err := c.client.Logical().ReadWithContext(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("vault read %s: %w", path, c.withNamespaceHint(err))
	}
	if secret == nil || secret.Data == nil {
		return map[string]any{}, nil
	}
	return secret.Data, nil
}

func supportsAuthRoles(authType string) bool {
	switch strings.ToLower(strings.TrimSpace(authType)) {
	case "approle", "kubernetes", "jwt", "oidc":
		return true
	default:
		return false
	}
}

func pathEscape(value string) string {
	return neturl.PathEscape(strings.TrimSpace(value))
}

func parseEntityAliases(raw any) []EntityAlias {
	items, ok := raw.([]any)
	if !ok {
		return nil
	}
	out := make([]EntityAlias, 0, len(items))
	for _, item := range items {
		entry, ok := item.(map[string]any)
		if !ok {
			continue
		}
		out = append(out, EntityAlias{
			Name:      mapString(entry, "name"),
			MountType: mapString(entry, "mount_type"),
			Metadata:  stringMap(entry["metadata"]),
		})
	}
	return out
}

func stringSlice(raw any) []string {
	switch v := raw.(type) {
	case nil:
		return nil
	case []string:
		return v
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if item == nil {
				continue
			}
			out = append(out, strings.TrimSpace(fmt.Sprint(item)))
		}
		return out
	default:
		value := strings.TrimSpace(fmt.Sprint(v))
		if value == "" {
			return nil
		}
		return []string{value}
	}
}

func stringMap(raw any) map[string]string {
	value, ok := raw.(map[string]any)
	if !ok {
		if alreadyTyped, ok := raw.(map[string]string); ok {
			return alreadyTyped
		}
		return map[string]string{}
	}
	out := make(map[string]string, len(value))
	for key, item := range value {
		trimmed := strings.TrimSpace(fmt.Sprint(item))
		if trimmed == "" {
			continue
		}
		out[strings.TrimSpace(key)] = trimmed
	}
	return out
}

func mapString(data map[string]any, key string) string {
	if data == nil {
		return ""
	}
	raw, ok := data[key]
	if !ok {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(raw))
}

func mapBool(data map[string]any, key string) bool {
	if data == nil {
		return false
	}
	raw, ok := data[key]
	if !ok {
		return false
	}
	switch value := raw.(type) {
	case bool:
		return value
	default:
		return strings.EqualFold(strings.TrimSpace(fmt.Sprint(value)), "true")
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func dedupeNonEmpty(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func normalizeMountPath(path string) string {
	return strings.Trim(strings.TrimSpace(path), "/")
}

func (c *Client) withNamespaceHint(err error) error {
	if err == nil {
		return nil
	}
	if strings.TrimSpace(c.namespace) != "" {
		return err
	}
	if !strings.HasSuffix(strings.ToLower(strings.TrimSpace(c.addressHost)), ".hashicorp.cloud") {
		return err
	}
	msg := strings.ToLower(err.Error())
	if !strings.Contains(msg, "permission denied") && !strings.Contains(msg, "403") {
		return err
	}
	return fmt.Errorf("%w (tip: set namespace to \"admin\" for HCP Vault Dedicated)", err)
}

func marshalJSON(value any) []byte {
	encoded, err := json.Marshal(value)
	if err != nil {
		return []byte("{}")
	}
	return encoded
}

func buildHTTPTransport(skipVerify bool, caCertPEM string) http.RoundTripper {
	base, _ := http.DefaultTransport.(*http.Transport)
	if base == nil {
		return http.DefaultTransport
	}
	transport := base.Clone()
	if transport.TLSClientConfig == nil {
		transport.TLSClientConfig = &tls.Config{}
	} else {
		transport.TLSClientConfig = transport.TLSClientConfig.Clone()
	}
	transport.TLSClientConfig.MinVersion = tls.VersionTLS12
	transport.TLSClientConfig.InsecureSkipVerify = skipVerify
	if strings.TrimSpace(caCertPEM) != "" {
		pool := x509.NewCertPool()
		if pool.AppendCertsFromPEM([]byte(caCertPEM)) {
			transport.TLSClientConfig.RootCAs = pool
		}
	}
	return transport
}
