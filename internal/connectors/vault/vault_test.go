package vault

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestVaultClientTokenInventory(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/identity/entity/id":
			writeJSON(t, w, map[string]any{"data": map[string]any{"keys": []string{"entity-1"}}})
		case r.URL.Path == "/v1/identity/entity/id/entity-1":
			writeJSON(t, w, map[string]any{
				"data": map[string]any{
					"id":       "entity-1",
					"name":     "build-bot",
					"disabled": false,
					"policies": []string{"default", "ci"},
					"metadata": map[string]any{"email": "bot@example.com"},
				},
			})
		case r.URL.Path == "/v1/identity/group/id":
			writeJSON(t, w, map[string]any{"data": map[string]any{"keys": []string{"group-1"}}})
		case r.URL.Path == "/v1/identity/group/id/group-1":
			writeJSON(t, w, map[string]any{
				"data": map[string]any{
					"id":                "group-1",
					"name":              "engineers",
					"policies":          []string{"eng-policy"},
					"member_entity_ids": []string{"entity-1"},
				},
			})
		case r.URL.Path == "/v1/sys/policies/acl":
			writeJSON(t, w, map[string]any{"data": map[string]any{"keys": []string{"default", "ci", "eng-policy"}}})
		case r.URL.Path == "/v1/sys/auth":
			writeJSON(t, w, map[string]any{
				"data": map[string]any{
					"approle/": map[string]any{
						"type":        "approle",
						"description": "AppRole auth",
					},
				},
			})
		case r.URL.Path == "/v1/sys/mounts":
			writeJSON(t, w, map[string]any{
				"data": map[string]any{
					"kv/": map[string]any{
						"type":        "kv",
						"description": "KV storage",
					},
				},
			})
		case r.URL.Path == "/v1/auth/approle/role":
			writeJSON(t, w, map[string]any{"data": map[string]any{"keys": []string{"ci-role"}}})
		case r.URL.Path == "/v1/auth/approle/role/ci-role":
			writeJSON(t, w, map[string]any{
				"data": map[string]any{
					"token_policies": []string{"ci"},
				},
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client, err := New(Options{
		Address:  server.URL,
		AuthType: vaultAuthTypeToken,
		Token:    "s.token",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx := context.Background()
	entities, err := client.ListEntities(ctx)
	if err != nil {
		t.Fatalf("ListEntities() error = %v", err)
	}
	if len(entities) != 1 || entities[0].ID != "entity-1" {
		t.Fatalf("unexpected entities: %+v", entities)
	}

	groups, err := client.ListGroups(ctx)
	if err != nil {
		t.Fatalf("ListGroups() error = %v", err)
	}
	if len(groups) != 1 || groups[0].Name != "engineers" {
		t.Fatalf("unexpected groups: %+v", groups)
	}

	policies, err := client.ListACLPolicies(ctx)
	if err != nil {
		t.Fatalf("ListACLPolicies() error = %v", err)
	}
	if len(policies) != 3 {
		t.Fatalf("expected 3 policies, got %d", len(policies))
	}

	authMounts, err := client.ListAuthMounts(ctx)
	if err != nil {
		t.Fatalf("ListAuthMounts() error = %v", err)
	}
	if len(authMounts) != 1 || authMounts[0].Path != "approle" {
		t.Fatalf("unexpected auth mounts: %+v", authMounts)
	}

	secretMounts, err := client.ListSecretsMounts(ctx)
	if err != nil {
		t.Fatalf("ListSecretsMounts() error = %v", err)
	}
	if len(secretMounts) != 1 || secretMounts[0].Path != "kv" {
		t.Fatalf("unexpected secret mounts: %+v", secretMounts)
	}

	roles, err := client.ListAuthRoles(ctx, authMounts)
	if err != nil {
		t.Fatalf("ListAuthRoles() error = %v", err)
	}
	if len(roles) != 1 || roles[0].Name != "ci-role" {
		t.Fatalf("unexpected auth roles: %+v", roles)
	}
}

func TestVaultClientAppRoleLogin(t *testing.T) {
	t.Parallel()

	var loginCalled bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/auth/platform-approle/login" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Method != http.MethodPost && r.Method != http.MethodPut {
			t.Errorf("expected POST or PUT login method, got %s", r.Method)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		defer r.Body.Close()
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode login body: %v", err)
		}
		if strings.TrimSpace(body["role_id"].(string)) != "role-id" {
			t.Fatalf("unexpected role_id: %v", body["role_id"])
		}
		if strings.TrimSpace(body["secret_id"].(string)) != "secret-id" {
			t.Fatalf("unexpected secret_id: %v", body["secret_id"])
		}
		loginCalled = true
		writeJSON(t, w, map[string]any{"auth": map[string]any{"client_token": "token-from-approle"}})
	}))
	defer server.Close()

	_, err := New(Options{
		Address:          server.URL,
		AuthType:         vaultAuthTypeAppRole,
		AppRoleMountPath: "platform-approle",
		AppRoleRoleID:    "role-id",
		AppRoleSecretID:  "secret-id",
	})
	if err != nil {
		t.Fatalf("New(approle) error = %v", err)
	}
	if !loginCalled {
		t.Fatalf("expected approle login endpoint to be called")
	}
}

func TestVaultClientListEntitiesError(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/identity/entity/id" {
			w.WriteHeader(http.StatusForbidden)
			writeJSON(t, w, map[string]any{"errors": []string{"permission denied"}})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client, err := New(Options{
		Address: server.URL,
		Token:   "s.token",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	_, err = client.ListEntities(context.Background())
	if err == nil {
		t.Fatalf("expected ListEntities() to fail when Vault denies list permission")
	}
}

func TestVaultClientNamespaceHintForHCP(t *testing.T) {
	t.Parallel()

	client := &Client{
		namespace:   "",
		addressHost: "cluster.hashicorp.cloud",
	}
	err := client.withNamespaceHint(errors.New("permission denied"))
	if err == nil {
		t.Fatalf("expected wrapped error")
	}
	if !strings.Contains(err.Error(), "namespace to \"admin\"") {
		t.Fatalf("expected namespace hint in error, got %q", err.Error())
	}

	client.namespace = "admin"
	err = client.withNamespaceHint(errors.New("permission denied"))
	if err == nil {
		t.Fatalf("expected passthrough error")
	}
	if strings.Contains(err.Error(), "namespace to \"admin\"") {
		t.Fatalf("did not expect namespace hint when namespace is set")
	}
}

func writeJSON(t *testing.T, w http.ResponseWriter, payload map[string]any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}
