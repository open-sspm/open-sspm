package github

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

var ErrNoSAMLIdentityProvider = errors.New("github saml identity provider is not configured")
var ErrNoEnterpriseSAMLIdentityProvider = errors.New("github enterprise saml identity provider is not configured")

type SAMLExternalIdentity struct {
	Login  string
	NameID string
	// SCIMUserName is the SCIM identity username (often the SCIM userName).
	SCIMUserName string
	// SCIMEmail is the first SCIM identity email value (when present).
	SCIMEmail string
	// UserEmail is the GitHub user email from the GraphQL user.email field.
	UserEmail string
}

func (c *Client) ListOrgSAMLExternalIdentities(ctx context.Context, org string) ([]SAMLExternalIdentity, error) {
	if c.BaseURL == "" || c.Token == "" {
		return nil, errors.New("github base URL and token are required")
	}

	type gqlPayload struct {
		Data struct {
			Organization *struct {
				SAMLIdentityProvider *struct {
					ExternalIdentities struct {
						Edges []struct {
							Node struct {
								SamlIdentity *struct {
									NameID string `json:"nameId"`
								} `json:"samlIdentity"`
								ScimIdentity *struct {
									Username string `json:"username"`
									Emails   []struct {
										Value string `json:"value"`
									} `json:"emails"`
								} `json:"scimIdentity"`
								User *struct {
									Login string `json:"login"`
									Email string `json:"email"`
								} `json:"user"`
							} `json:"node"`
						} `json:"edges"`
						PageInfo struct {
							HasNextPage bool   `json:"hasNextPage"`
							EndCursor   string `json:"endCursor"`
						} `json:"pageInfo"`
					} `json:"externalIdentities"`
				} `json:"samlIdentityProvider"`
			} `json:"organization"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}

	const query = `query($org: String!, $cursor: String) {
  organization(login: $org) {
    samlIdentityProvider {
      externalIdentities(first: 100, after: $cursor, membersOnly: true) {
        edges {
          node {
            samlIdentity { nameId }
            scimIdentity { username emails { value } }
            user { login email }
          }
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
  }
}`

	var out []SAMLExternalIdentity
	cursor := ""
	for {
		var payload gqlPayload
		vars := map[string]any{
			"org": org,
		}
		if cursor != "" {
			vars["cursor"] = cursor
		} else {
			vars["cursor"] = nil
		}
		if err := c.doGraphQL(ctx, query, vars, &payload); err != nil {
			return nil, err
		}
		if len(payload.Errors) > 0 {
			msg := strings.TrimSpace(payload.Errors[0].Message)
			if msg == "" {
				msg = "unknown error"
			}
			return nil, fmt.Errorf("github graphql error: %s", msg)
		}
		if payload.Data.Organization == nil {
			return nil, fmt.Errorf("github graphql error: organization %q not found", org)
		}
		if payload.Data.Organization.SAMLIdentityProvider == nil {
			return nil, ErrNoSAMLIdentityProvider
		}

		for _, edge := range payload.Data.Organization.SAMLIdentityProvider.ExternalIdentities.Edges {
			if edge.Node.User == nil {
				continue
			}
			login := strings.TrimSpace(edge.Node.User.Login)
			userEmail := strings.TrimSpace(edge.Node.User.Email)
			nameID := ""
			if edge.Node.SamlIdentity != nil {
				nameID = strings.TrimSpace(edge.Node.SamlIdentity.NameID)
			}
			scimUserName := ""
			if edge.Node.ScimIdentity != nil {
				scimUserName = strings.TrimSpace(edge.Node.ScimIdentity.Username)
			}
			scimEmail := ""
			if edge.Node.ScimIdentity != nil {
				for _, e := range edge.Node.ScimIdentity.Emails {
					if v := strings.TrimSpace(e.Value); v != "" {
						scimEmail = v
						break
					}
				}
			}
			if login == "" || (nameID == "" && scimUserName == "" && scimEmail == "" && userEmail == "") {
				continue
			}
			out = append(out, SAMLExternalIdentity{Login: login, NameID: nameID, SCIMUserName: scimUserName, SCIMEmail: scimEmail, UserEmail: userEmail})
		}

		if !payload.Data.Organization.SAMLIdentityProvider.ExternalIdentities.PageInfo.HasNextPage {
			break
		}
		cursor = payload.Data.Organization.SAMLIdentityProvider.ExternalIdentities.PageInfo.EndCursor
		if cursor == "" {
			break
		}
	}

	return out, nil
}

func (c *Client) ListEnterpriseSAMLExternalIdentities(ctx context.Context, enterprise string) ([]SAMLExternalIdentity, error) {
	if c.BaseURL == "" || c.Token == "" {
		return nil, errors.New("github base URL and token are required")
	}

	type gqlPayload struct {
		Data struct {
			Enterprise *struct {
				OwnerInfo *struct {
					SAMLIdentityProvider *struct {
						ExternalIdentities struct {
							Edges []struct {
								Node struct {
									SamlIdentity *struct {
										NameID string `json:"nameId"`
									} `json:"samlIdentity"`
									ScimIdentity *struct {
										Username string `json:"username"`
										Emails   []struct {
											Value string `json:"value"`
										} `json:"emails"`
									} `json:"scimIdentity"`
									User *struct {
										Login string `json:"login"`
										Email string `json:"email"`
									} `json:"user"`
								} `json:"node"`
							} `json:"edges"`
							PageInfo struct {
								HasNextPage bool   `json:"hasNextPage"`
								EndCursor   string `json:"endCursor"`
							} `json:"pageInfo"`
						} `json:"externalIdentities"`
					} `json:"samlIdentityProvider"`
				} `json:"ownerInfo"`
			} `json:"enterprise"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}

	const query = `query($enterprise: String!, $cursor: String) {
  enterprise(slug: $enterprise) {
    ownerInfo {
      samlIdentityProvider {
        externalIdentities(first: 100, after: $cursor, membersOnly: true) {
          edges {
            node {
              samlIdentity { nameId }
              scimIdentity { username emails { value } }
              user { login email }
            }
          }
          pageInfo {
            hasNextPage
            endCursor
          }
        }
      }
    }
  }
}`

	var out []SAMLExternalIdentity
	cursor := ""
	for {
		var payload gqlPayload
		vars := map[string]any{
			"enterprise": enterprise,
		}
		if cursor != "" {
			vars["cursor"] = cursor
		} else {
			vars["cursor"] = nil
		}

		if err := c.doGraphQL(ctx, query, vars, &payload); err != nil {
			return nil, err
		}
		if len(payload.Errors) > 0 {
			msg := strings.TrimSpace(payload.Errors[0].Message)
			if msg == "" {
				msg = "unknown error"
			}
			return nil, fmt.Errorf("github graphql error: %s", msg)
		}
		if payload.Data.Enterprise == nil {
			return nil, fmt.Errorf("github graphql error: enterprise %q not found", enterprise)
		}
		if payload.Data.Enterprise.OwnerInfo == nil || payload.Data.Enterprise.OwnerInfo.SAMLIdentityProvider == nil {
			return nil, ErrNoEnterpriseSAMLIdentityProvider
		}

		for _, edge := range payload.Data.Enterprise.OwnerInfo.SAMLIdentityProvider.ExternalIdentities.Edges {
			if edge.Node.User == nil {
				continue
			}
			login := strings.TrimSpace(edge.Node.User.Login)
			userEmail := strings.TrimSpace(edge.Node.User.Email)
			nameID := ""
			if edge.Node.SamlIdentity != nil {
				nameID = strings.TrimSpace(edge.Node.SamlIdentity.NameID)
			}
			scimUserName := ""
			if edge.Node.ScimIdentity != nil {
				scimUserName = strings.TrimSpace(edge.Node.ScimIdentity.Username)
			}
			scimEmail := ""
			if edge.Node.ScimIdentity != nil {
				for _, e := range edge.Node.ScimIdentity.Emails {
					if v := strings.TrimSpace(e.Value); v != "" {
						scimEmail = v
						break
					}
				}
			}
			if login == "" || (nameID == "" && scimUserName == "" && scimEmail == "" && userEmail == "") {
				continue
			}
			out = append(out, SAMLExternalIdentity{Login: login, NameID: nameID, SCIMUserName: scimUserName, SCIMEmail: scimEmail, UserEmail: userEmail})
		}

		if !payload.Data.Enterprise.OwnerInfo.SAMLIdentityProvider.ExternalIdentities.PageInfo.HasNextPage {
			break
		}
		cursor = payload.Data.Enterprise.OwnerInfo.SAMLIdentityProvider.ExternalIdentities.PageInfo.EndCursor
		if cursor == "" {
			break
		}
	}

	return out, nil
}

type SCIMEmail struct {
	Value   string `json:"value"`
	Primary bool   `json:"primary"`
}

type SCIMUser struct {
	ID          string
	UserName    string
	DisplayName string
	Active      bool
	Emails      []SCIMEmail

	// GitHubLogin may be populated from GitHub SCIM extensions when available.
	GitHubLogin string

	RawJSON []byte
}

func (u SCIMUser) PreferredEmail() string {
	for _, e := range u.Emails {
		if strings.TrimSpace(e.Value) == "" {
			continue
		}
		if e.Primary {
			return strings.TrimSpace(e.Value)
		}
	}
	for _, e := range u.Emails {
		if v := strings.TrimSpace(e.Value); v != "" {
			return v
		}
	}
	if looksLikeEmail(u.UserName) {
		return strings.TrimSpace(u.UserName)
	}
	return ""
}

func (c *Client) ListOrgSCIMUsers(ctx context.Context, org string) ([]SCIMUser, error) {
	if c.BaseURL == "" || c.Token == "" {
		return nil, errors.New("github base URL and token are required")
	}

	startIndex := 1
	count := 100
	var out []SCIMUser
	for {
		endpoint := fmt.Sprintf("%s/scim/v2/organizations/%s/Users?startIndex=%d&count=%d", c.BaseURL, url.PathEscape(org), startIndex, count)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+c.Token)
		req.Header.Set("Accept", "application/scim+json")
		req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
		req.Header.Set("User-Agent", "open-sspm")

		resp, err := c.HTTP.Do(req)
		if err != nil {
			return nil, err
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, err
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, formatGitHubAPIError("github scim failed", endpoint, resp, body)
		}

		var payload struct {
			TotalResults int               `json:"totalResults"`
			StartIndex   int               `json:"startIndex"`
			ItemsPerPage int               `json:"itemsPerPage"`
			Resources    []json.RawMessage `json:"Resources"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			return nil, err
		}

		for _, raw := range payload.Resources {
			var core struct {
				ID          string      `json:"id"`
				UserName    string      `json:"userName"`
				DisplayName string      `json:"displayName"`
				Active      bool        `json:"active"`
				Emails      []SCIMEmail `json:"emails"`
			}
			if err := json.Unmarshal(raw, &core); err != nil {
				return nil, err
			}

			var fields map[string]json.RawMessage
			_ = json.Unmarshal(raw, &fields)

			login := ""
			for _, key := range []string{
				"urn:ietf:params:scim:schemas:extension:github:2.0:User",
				"urn:ietf:params:scim:schemas:extension:github:2.0:User:User",
			} {
				ext, ok := fields[key]
				if !ok || len(ext) == 0 {
					continue
				}
				var extPayload struct {
					Login    string `json:"login"`
					UserName string `json:"userName"`
					Username string `json:"username"`
				}
				if err := json.Unmarshal(ext, &extPayload); err != nil {
					continue
				}
				switch {
				case strings.TrimSpace(extPayload.Login) != "":
					login = strings.TrimSpace(extPayload.Login)
				case strings.TrimSpace(extPayload.UserName) != "":
					login = strings.TrimSpace(extPayload.UserName)
				case strings.TrimSpace(extPayload.Username) != "":
					login = strings.TrimSpace(extPayload.Username)
				}
				if login != "" {
					break
				}
			}

			out = append(out, SCIMUser{
				ID:          strings.TrimSpace(core.ID),
				UserName:    strings.TrimSpace(core.UserName),
				DisplayName: strings.TrimSpace(core.DisplayName),
				Active:      core.Active,
				Emails:      core.Emails,
				GitHubLogin: login,
				RawJSON:     raw,
			})
		}

		total := payload.TotalResults
		if total <= 0 || payload.ItemsPerPage <= 0 {
			break
		}
		next := payload.StartIndex + payload.ItemsPerPage
		if next > total {
			break
		}
		startIndex = next
	}

	return out, nil
}

func looksLikeEmail(v string) bool {
	v = strings.TrimSpace(v)
	if v == "" || strings.ContainsAny(v, " \t\r\n") {
		return false
	}
	at := strings.IndexByte(v, '@')
	if at <= 0 || at >= len(v)-1 {
		return false
	}
	if strings.Contains(v[at+1:], "@") {
		return false
	}
	return strings.Contains(v[at+1:], ".")
}
