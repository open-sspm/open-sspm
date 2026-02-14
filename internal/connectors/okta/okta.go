package okta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	sdk "github.com/okta/okta-sdk-golang/v6/okta"
)

type Client struct {
	BaseURL string
	Token   string
	api     *sdk.APIClient
}

type User struct {
	ID          string
	Email       string
	DisplayName string
	Status      string
	LastLoginAt *time.Time
	RawJSON     []byte
}

type LastLogin struct {
	At     *time.Time
	IP     string
	Region string
}

type Group struct {
	ID      string
	Name    string
	Type    string
	RawJSON []byte
}

type App struct {
	ID         string
	Label      string
	Name       string
	Status     string
	SignOnMode string
	RawJSON    []byte
}

type UserAppAssignment struct {
	App         App
	Scope       string
	ProfileJSON []byte
	RawJSON     []byte
}

type AppUserAssignment struct {
	UserID      string
	Scope       string
	ProfileJSON []byte
	RawJSON     []byte
}

type AppGroupAssignment struct {
	AppID       string
	Group       Group
	Priority    int32
	ProfileJSON []byte
	RawJSON     []byte
}

type SystemLogEvent struct {
	ID            string
	EventType     string
	Published     time.Time
	AppID         string
	AppName       string
	AppDomain     string
	ActorID       string
	ActorEmail    string
	ActorName     string
	GrantedScopes []string
	RawJSON       []byte
}

// New creates a new Okta client. It validates that both baseURL and token are
// provided and returns an error if the SDK configuration fails.
func New(baseURL, token string) (*Client, error) {
	base := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	token = strings.TrimSpace(token)

	if base == "" {
		return nil, errors.New("okta base URL is required")
	}
	if token == "" {
		return nil, errors.New("okta token is required")
	}

	cfg, err := sdk.NewConfiguration(
		sdk.WithOrgUrl(base),
		sdk.WithToken(token),
		sdk.WithCache(false),
		sdk.WithRequestTimeout(120),
		sdk.WithRateLimitMaxBackOff(30),
		sdk.WithRateLimitMaxRetries(4),
	)
	if err != nil {
		return nil, fmt.Errorf("okta sdk config: %w", err)
	}
	api := sdk.NewAPIClient(cfg)
	return &Client{BaseURL: base, Token: token, api: api}, nil
}

func (c *Client) ensureClient() error {
	if c.api == nil {
		return errors.New("okta client is not initialized")
	}
	return nil
}

func (c *Client) GetLastLogin(ctx context.Context, userID string) (LastLogin, error) {
	if err := c.ensureClient(); err != nil {
		return LastLogin{}, err
	}
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return LastLogin{}, errors.New("okta user id is required")
	}

	filter := fmt.Sprintf(`eventType eq "user.session.start" and actor.id eq "%s"`, userID)
	req := c.api.SystemLogAPI.ListLogEvents(ctx).
		Filter(filter).
		Limit(1).
		SortOrder("DESCENDING")
	events, resp, err := req.Execute()
	if err != nil {
		return LastLogin{}, formatOktaError(err, resp)
	}
	if len(events) == 0 {
		return LastLogin{}, nil
	}

	event := events[0]

	var at *time.Time
	if event.Published != nil && !event.Published.IsZero() {
		at = event.Published
	}

	ip := ""
	region := ""
	if event.Client != nil {
		ip = strings.TrimSpace(event.Client.GetIpAddress())
		geo := event.Client.GetGeographicalContext()
		region = formatOktaRegion(geo)
	}

	return LastLogin{At: at, IP: ip, Region: region}, nil
}

func (c *Client) ListSystemLogEventsSince(ctx context.Context, since time.Time) ([]SystemLogEvent, error) {
	if err := c.ensureClient(); err != nil {
		return nil, err
	}

	since = since.UTC()
	req := c.api.SystemLogAPI.ListLogEvents(ctx).
		Since(since.Format(time.RFC3339)).
		SortOrder("ASCENDING").
		Limit(1000)

	events, resp, err := req.Execute()
	if err != nil {
		return nil, formatOktaError(err, resp)
	}

	out := make([]SystemLogEvent, 0, len(events))
	for {
		for _, event := range events {
			mapped, mapErr := mapSystemLogEvent(event)
			if mapErr != nil {
				return nil, mapErr
			}
			if mapped.ID == "" {
				continue
			}
			out = append(out, mapped)
		}
		if resp == nil || !resp.HasNextPage() {
			break
		}
		var next []sdk.LogEvent
		resp, err = resp.Next(&next)
		if err != nil {
			return nil, formatOktaError(err, resp)
		}
		events = next
	}

	return out, nil
}

func formatOktaRegion(geo sdk.LogGeographicalContext) string {
	state := strings.TrimSpace(geo.GetState())
	country := strings.TrimSpace(geo.GetCountry())
	if state != "" && country != "" {
		return state + ", " + country
	}
	if state != "" {
		return state
	}
	if country != "" {
		return country
	}
	return ""
}

func (c *Client) ListUsers(ctx context.Context) ([]User, error) {
	if err := c.ensureClient(); err != nil {
		return nil, err
	}

	req := c.api.UserAPI.ListUsers(ctx).
		Limit(200).
		Fields("id,status,lastLogin,profile:(email,login,displayName,firstName,lastName)").
		ContentType("application/json; okta-response=omitCredentials,omitCredentialsLinks,omitTransitioningToStatus")
	users, resp, err := req.Execute()
	if err != nil {
		return nil, formatOktaError(err, resp)
	}
	var out []User
	for {
		for _, u := range users {
			mapped, err := mapOktaUser(u)
			if err != nil {
				return nil, err
			}
			out = append(out, mapped)
		}
		if resp == nil || !resp.HasNextPage() {
			break
		}
		var next []sdk.User
		resp, err = resp.Next(&next)
		if err != nil {
			return nil, formatOktaError(err, resp)
		}
		users = next
	}
	return out, nil
}

func (c *Client) ListGroups(ctx context.Context) ([]Group, error) {
	if err := c.ensureClient(); err != nil {
		return nil, err
	}

	req := c.api.GroupAPI.ListGroups(ctx).Limit(200)
	groups, resp, err := req.Execute()
	if err != nil {
		return nil, formatOktaError(err, resp)
	}
	var out []Group
	for {
		for _, g := range groups {
			mapped, err := mapOktaGroup(g)
			if err != nil {
				return nil, err
			}
			out = append(out, mapped)
		}
		if resp == nil || !resp.HasNextPage() {
			break
		}
		var next []sdk.Group
		resp, err = resp.Next(&next)
		if err != nil {
			return nil, formatOktaError(err, resp)
		}
		groups = next
	}
	return out, nil
}

func (c *Client) ListGroupUserIDs(ctx context.Context, groupID string) ([]string, error) {
	if err := c.ensureClient(); err != nil {
		return nil, err
	}

	groupID = strings.TrimSpace(groupID)
	if groupID == "" {
		return nil, errors.New("okta group id is required")
	}

	req := c.api.GroupAPI.ListGroupUsers(ctx, groupID).Limit(200)
	users, resp, err := req.Execute()
	if err != nil {
		return nil, formatOktaError(err, resp)
	}
	var out []string
	for {
		for _, u := range users {
			id := strings.TrimSpace(u.GetId())
			if id == "" {
				continue
			}
			out = append(out, id)
		}
		if resp == nil || !resp.HasNextPage() {
			break
		}
		var next []sdk.User
		resp, err = resp.Next(&next)
		if err != nil {
			return nil, formatOktaError(err, resp)
		}
		users = next
	}
	return out, nil
}

func (c *Client) ListApps(ctx context.Context) ([]App, error) {
	if err := c.ensureClient(); err != nil {
		return nil, err
	}

	req := c.api.ApplicationAPI.ListApplications(ctx).Limit(200)
	apps, resp, err := req.Execute()
	if err != nil {
		return listAppsFromRaw(resp, err)
	}

	out := make([]App, 0, len(apps))
	for _, app := range apps {
		mapped, err := mapOktaApp(app)
		if err != nil {
			return nil, err
		}
		out = append(out, mapped)
	}

	for resp != nil && resp.HasNextPage() {
		var nextRaw []json.RawMessage
		resp, err = resp.Next(&nextRaw)
		if err != nil {
			return nil, formatOktaError(err, resp)
		}
		page, mapErr := mapOktaAppsFromRaw(nextRaw)
		if mapErr != nil {
			return nil, mapErr
		}
		out = append(out, page...)
	}

	return out, nil
}

func (c *Client) ListApplicationUsers(ctx context.Context, appID string) ([]AppUserAssignment, error) {
	if err := c.ensureClient(); err != nil {
		return nil, err
	}

	appID = strings.TrimSpace(appID)
	if appID == "" {
		return nil, errors.New("okta app id is required")
	}

	req := c.api.ApplicationUsersAPI.ListApplicationUsers(ctx, appID).Limit(200)
	users, resp, err := req.Execute()
	if err != nil {
		return nil, formatOktaError(err, resp)
	}
	var out []AppUserAssignment
	for {
		for _, u := range users {
			mapped, err := mapOktaAppUserAssignment(u)
			if err != nil {
				return nil, err
			}
			out = append(out, mapped)
		}
		if resp == nil || !resp.HasNextPage() {
			break
		}
		var next []sdk.AppUser
		resp, err = resp.Next(&next)
		if err != nil {
			return nil, formatOktaError(err, resp)
		}
		users = next
	}
	return out, nil
}

func (c *Client) ListUserGroups(ctx context.Context, userID string) ([]Group, error) {
	if err := c.ensureClient(); err != nil {
		return nil, err
	}
	req := c.api.UserResourcesAPI.ListUserGroups(ctx, userID)
	groups, resp, err := req.Execute()
	if err != nil {
		return nil, formatOktaError(err, resp)
	}
	var out []Group
	for {
		for _, g := range groups {
			mapped, err := mapOktaGroup(g)
			if err != nil {
				return nil, err
			}
			out = append(out, mapped)
		}
		if resp == nil || !resp.HasNextPage() {
			break
		}
		var next []sdk.Group
		resp, err = resp.Next(&next)
		if err != nil {
			return nil, formatOktaError(err, resp)
		}
		groups = next
	}
	return out, nil
}

func (c *Client) ListAppsAssignedToUser(ctx context.Context, userID string) ([]UserAppAssignment, error) {
	if err := c.ensureClient(); err != nil {
		return nil, err
	}
	filter := fmt.Sprintf("user.id eq \"%s\"", userID)
	expand := fmt.Sprintf("user/%s", userID)
	req := c.api.ApplicationAPI.ListApplications(ctx).Filter(filter).Expand(expand).Limit(200)
	apps, resp, err := req.Execute()
	var out []UserAppAssignment
	if err != nil {
		out, err := listAppsAssignedToUserFromRaw(resp, err)
		if err != nil {
			return nil, err
		}
		return out, nil
	}
	for {
		for _, app := range apps {
			assignment, err := mapOktaUserAssignment(app)
			if err != nil {
				return nil, err
			}
			out = append(out, assignment)
		}
		if resp == nil || !resp.HasNextPage() {
			break
		}
		var next []sdk.ListApplications200ResponseInner
		resp, err = resp.Next(&next)
		if err != nil {
			return nil, formatOktaError(err, resp)
		}
		apps = next
	}
	return out, nil
}

func (c *Client) ListApplicationGroupAssignments(ctx context.Context, appID string) ([]AppGroupAssignment, error) {
	if err := c.ensureClient(); err != nil {
		return nil, err
	}
	req := c.api.ApplicationGroupsAPI.ListApplicationGroupAssignments(ctx, appID).Expand("group").Limit(200)
	assignments, resp, err := req.Execute()
	if err != nil {
		return nil, formatOktaError(err, resp)
	}
	var out []AppGroupAssignment
	for {
		for _, assignment := range assignments {
			mapped, err := mapOktaAppGroupAssignment(appID, assignment)
			if err != nil {
				return nil, err
			}
			out = append(out, mapped)
		}
		if resp == nil || !resp.HasNextPage() {
			break
		}
		var next []sdk.ApplicationGroupAssignment
		resp, err = resp.Next(&next)
		if err != nil {
			return nil, formatOktaError(err, resp)
		}
		assignments = next
	}
	return out, nil
}

func mapSystemLogEvent(event sdk.LogEvent) (SystemLogEvent, error) {
	raw, err := json.Marshal(event)
	if err != nil {
		return SystemLogEvent{}, err
	}

	published := time.Time{}
	if event.Published != nil {
		published = event.Published.UTC()
	}
	eventType := strings.TrimSpace(event.GetEventType())
	eventID := strings.TrimSpace(event.GetUuid())

	actor := event.GetActor()
	actorID := strings.TrimSpace(actor.GetId())
	actorEmail := strings.TrimSpace(actor.GetAlternateId())
	actorName := strings.TrimSpace(actor.GetDisplayName())

	var appID string
	var appName string
	var appDomain string
	for _, target := range event.Target {
		targetType := strings.ToLower(strings.TrimSpace(target.GetType()))
		if !strings.Contains(targetType, "app") {
			continue
		}
		if appID == "" {
			appID = strings.TrimSpace(target.GetId())
		}
		if appName == "" {
			appName = strings.TrimSpace(target.GetDisplayName())
		}
		if appDomain == "" {
			appDomain = strings.TrimSpace(target.GetAlternateId())
		}
		if details := target.GetDetailEntry(); details != nil {
			if appID == "" {
				appID = strings.TrimSpace(getStringValue(details["appId"]))
			}
			if appName == "" {
				appName = strings.TrimSpace(getStringValue(details["appName"]))
			}
			if appDomain == "" {
				appDomain = strings.TrimSpace(getStringValue(details["domain"]))
			}
		}
	}

	debugContext := event.GetDebugContext()
	scopes := extractSystemLogScopes(debugContext.GetDebugData())

	if eventID == "" {
		eventID = strings.TrimSpace(eventType + ":" + published.Format(time.RFC3339Nano) + ":" + appID + ":" + actorID)
	}

	return SystemLogEvent{
		ID:            eventID,
		EventType:     eventType,
		Published:     published,
		AppID:         appID,
		AppName:       appName,
		AppDomain:     appDomain,
		ActorID:       actorID,
		ActorEmail:    actorEmail,
		ActorName:     actorName,
		GrantedScopes: scopes,
		RawJSON:       raw,
	}, nil
}

func extractSystemLogScopes(debugData map[string]any) []string {
	if len(debugData) == 0 {
		return nil
	}
	candidates := make([]string, 0, 8)
	keys := []string{"scopes", "scope", "grantedScopes", "requestedScopes", "scp"}
	for _, key := range keys {
		value, ok := debugData[key]
		if !ok {
			continue
		}
		switch typed := value.(type) {
		case string:
			candidates = append(candidates, splitScopes(typed)...)
		case []string:
			candidates = append(candidates, typed...)
		case []any:
			for _, raw := range typed {
				candidates = append(candidates, splitScopes(getStringValue(raw))...)
			}
		}
	}
	if len(candidates) == 0 {
		return nil
	}

	dedup := make([]string, 0, len(candidates))
	seen := map[string]struct{}{}
	for _, scope := range candidates {
		scope = strings.ToLower(strings.TrimSpace(scope))
		if scope == "" {
			continue
		}
		if _, ok := seen[scope]; ok {
			continue
		}
		seen[scope] = struct{}{}
		dedup = append(dedup, scope)
	}
	return dedup
}

func splitScopes(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ' ' || r == ',' || r == ';'
	})
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func mapOktaUser(u sdk.User) (User, error) {
	profile := u.Profile
	email := ""
	login := ""
	display := ""
	first := ""
	last := ""
	if profile != nil {
		email = profile.GetEmail()
		login = profile.GetLogin()
		display = profile.GetDisplayName()
		first = profile.GetFirstName()
		last = profile.GetLastName()
	}
	if email == "" {
		email = login
	}
	if display == "" {
		display = strings.TrimSpace(first + " " + last)
	}
	raw, err := json.Marshal(u)
	if err != nil {
		return User{}, err
	}
	var lastLoginAt *time.Time
	if t, ok := u.GetLastLoginOk(); ok && t != nil && !t.IsZero() {
		lastLoginAt = t
	}
	return User{
		ID:          u.GetId(),
		Email:       email,
		DisplayName: display,
		Status:      u.GetStatus(),
		LastLoginAt: lastLoginAt,
		RawJSON:     raw,
	}, nil
}

func mapOktaGroup(g sdk.Group) (Group, error) {
	name := ""
	if g.Profile != nil {
		if g.Profile.OktaUserGroupProfile != nil {
			name = g.Profile.OktaUserGroupProfile.GetName()
		} else if g.Profile.OktaActiveDirectoryGroupProfile != nil {
			name = g.Profile.OktaActiveDirectoryGroupProfile.GetName()
		}
	}
	raw, err := json.Marshal(g)
	if err != nil {
		return Group{}, err
	}
	return Group{
		ID:      g.GetId(),
		Name:    name,
		Type:    g.GetType(),
		RawJSON: raw,
	}, nil
}

func mapOktaApp(app sdk.ListApplications200ResponseInner) (App, error) {
	raw, err := json.Marshal(app)
	if err != nil {
		return App{}, err
	}
	return mapOktaAppPayload(raw)
}

func mapOktaAppPayload(raw []byte) (App, error) {
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return App{}, err
	}
	return App{
		ID:         getStringValue(payload["id"]),
		Label:      getStringValue(payload["label"]),
		Name:       getStringValue(payload["name"]),
		Status:     getStringValue(payload["status"]),
		SignOnMode: getStringValue(payload["signOnMode"]),
		RawJSON:    raw,
	}, nil
}

func mapOktaAppUserAssignment(appUser sdk.AppUser) (AppUserAssignment, error) {
	raw, err := json.Marshal(appUser)
	if err != nil {
		return AppUserAssignment{}, err
	}
	return AppUserAssignment{
		UserID:      strings.TrimSpace(appUser.GetId()),
		Scope:       strings.TrimSpace(appUser.GetScope()),
		ProfileJSON: encodeJSON(appUser.Profile),
		RawJSON:     raw,
	}, nil
}

func mapOktaUserAssignment(app sdk.ListApplications200ResponseInner) (UserAppAssignment, error) {
	raw, err := json.Marshal(app)
	if err != nil {
		return UserAppAssignment{}, err
	}
	return mapOktaUserAssignmentPayload(raw)
}

func mapOktaUserAssignmentRaw(raw []byte) (UserAppAssignment, error) {
	return mapOktaUserAssignmentPayload(raw)
}

func mapOktaUserAssignmentPayload(raw []byte) (UserAppAssignment, error) {
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return UserAppAssignment{}, err
	}
	appInfo := App{
		ID:         getStringValue(payload["id"]),
		Label:      getStringValue(payload["label"]),
		Name:       getStringValue(payload["name"]),
		Status:     getStringValue(payload["status"]),
		SignOnMode: getStringValue(payload["signOnMode"]),
		RawJSON:    raw,
	}
	scope, profile, assignmentRaw := extractEmbeddedUserAssignment(payload)
	return UserAppAssignment{
		App:         appInfo,
		Scope:       scope,
		ProfileJSON: encodeJSON(profile),
		RawJSON:     assignmentRaw,
	}, nil
}

func mapOktaAppGroupAssignment(appID string, assignment sdk.ApplicationGroupAssignment) (AppGroupAssignment, error) {
	raw, err := json.Marshal(assignment)
	if err != nil {
		return AppGroupAssignment{}, err
	}
	group := mapEmbeddedGroup(assignment, assignment.GetId())
	return AppGroupAssignment{
		AppID:       appID,
		Group:       group,
		Priority:    assignment.GetPriority(),
		ProfileJSON: encodeJSON(assignment.GetProfile()),
		RawJSON:     raw,
	}, nil
}

func extractEmbeddedUserAssignment(payload map[string]any) (string, map[string]any, []byte) {
	embedded, _ := payload["_embedded"].(map[string]any)
	if embedded == nil {
		return "", nil, []byte("{}")
	}
	userObj, ok := embedded["user"]
	if !ok {
		return "", nil, []byte("{}")
	}
	userMap, _ := userObj.(map[string]any)
	if userMap == nil {
		return "", nil, []byte("{}")
	}
	userMap = normalizeEmbeddedUser(userMap)
	scope, _ := userMap["scope"].(string)
	profile, _ := userMap["profile"].(map[string]any)
	return scope, profile, encodeJSON(userMap)
}

func normalizeEmbeddedUser(user map[string]any) map[string]any {
	if user == nil {
		return nil
	}
	if _, ok := user["profile"]; ok {
		return user
	}
	if _, ok := user["scope"]; ok {
		return user
	}
	if len(user) == 1 {
		for _, v := range user {
			if nested, ok := v.(map[string]any); ok {
				return nested
			}
		}
	}
	return user
}

func mapEmbeddedGroup(assignment sdk.ApplicationGroupAssignment, fallbackID string) Group {
	group := Group{ID: fallbackID}
	embedded := assignment.GetEmbedded()
	if embedded != nil {
		if rawGroup, ok := embedded["group"]; ok {
			groupRaw := rawGroup
			group.ID = getStringValue(groupRaw["id"])
			group.Type = getStringValue(groupRaw["type"])
			if profile, ok := groupRaw["profile"].(map[string]any); ok {
				group.Name = getStringValue(profile["name"])
			}
			group.RawJSON = encodeJSON(groupRaw)
		}
	}
	if group.RawJSON == nil {
		group.RawJSON = []byte("{}")
	}
	if group.ID == "" {
		group.ID = fallbackID
	}
	return group
}

func getStringValue(v any) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case fmt.Stringer:
		return val.String()
	default:
		return ""
	}
}

func encodeJSON(v any) []byte {
	if v == nil {
		return []byte("{}")
	}
	b, err := json.Marshal(v)
	if err != nil {
		return []byte("{}")
	}
	if string(b) == "null" {
		return []byte("{}")
	}
	return b
}

func listAppsAssignedToUserFromRaw(resp *sdk.APIResponse, err error) ([]UserAppAssignment, error) {
	var apiErr *sdk.GenericOpenAPIError
	if !errors.As(err, &apiErr) {
		return nil, formatOktaError(err, resp)
	}
	if resp == nil || resp.Response == nil {
		return nil, formatOktaError(err, resp)
	}
	statusCode := resp.Response.StatusCode
	if statusCode < 200 || statusCode >= 300 {
		return nil, formatOktaError(err, resp)
	}

	rawItems, parseErr := parseListApplicationsRaw(apiErr.Body())
	if parseErr != nil {
		return nil, formatOktaError(err, resp)
	}
	out, mapErr := mapOktaUserAssignmentsFromRaw(rawItems)
	if mapErr != nil {
		return nil, mapErr
	}

	for resp != nil && resp.HasNextPage() {
		var nextRaw []json.RawMessage
		resp, err = resp.Next(&nextRaw)
		if err != nil {
			return nil, formatOktaError(err, resp)
		}
		page, mapErr := mapOktaUserAssignmentsFromRaw(nextRaw)
		if mapErr != nil {
			return nil, mapErr
		}
		out = append(out, page...)
	}

	return out, nil
}

func parseListApplicationsRaw(body []byte) ([]json.RawMessage, error) {
	if len(body) == 0 {
		return nil, fmt.Errorf("empty response body")
	}
	var rawItems []json.RawMessage
	if err := json.Unmarshal(body, &rawItems); err != nil {
		return nil, err
	}
	return rawItems, nil
}

func listAppsFromRaw(resp *sdk.APIResponse, err error) ([]App, error) {
	var apiErr *sdk.GenericOpenAPIError
	if !errors.As(err, &apiErr) {
		return nil, formatOktaError(err, resp)
	}
	if resp == nil || resp.Response == nil {
		return nil, formatOktaError(err, resp)
	}
	statusCode := resp.Response.StatusCode
	if statusCode < 200 || statusCode >= 300 {
		return nil, formatOktaError(err, resp)
	}

	rawItems, parseErr := parseListApplicationsRaw(apiErr.Body())
	if parseErr != nil {
		return nil, formatOktaError(err, resp)
	}
	out, mapErr := mapOktaAppsFromRaw(rawItems)
	if mapErr != nil {
		return nil, mapErr
	}

	for resp != nil && resp.HasNextPage() {
		var nextRaw []json.RawMessage
		resp, err = resp.Next(&nextRaw)
		if err != nil {
			return nil, formatOktaError(err, resp)
		}
		page, mapErr := mapOktaAppsFromRaw(nextRaw)
		if mapErr != nil {
			return nil, mapErr
		}
		out = append(out, page...)
	}

	return out, nil
}

func mapOktaAppsFromRaw(rawItems []json.RawMessage) ([]App, error) {
	if len(rawItems) == 0 {
		return nil, nil
	}
	out := make([]App, 0, len(rawItems))
	for _, raw := range rawItems {
		app, err := mapOktaAppPayload(raw)
		if err != nil {
			return nil, err
		}
		out = append(out, app)
	}
	return out, nil
}

func mapOktaUserAssignmentsFromRaw(rawItems []json.RawMessage) ([]UserAppAssignment, error) {
	if len(rawItems) == 0 {
		return nil, nil
	}
	out := make([]UserAppAssignment, 0, len(rawItems))
	for _, raw := range rawItems {
		assignment, err := mapOktaUserAssignmentRaw(raw)
		if err != nil {
			return nil, err
		}
		out = append(out, assignment)
	}
	return out, nil
}

func formatOktaError(err error, resp *sdk.APIResponse) error {
	if err == nil {
		return nil
	}
	status := ""
	statusCode := 0
	if resp != nil && resp.Response != nil {
		status = resp.Response.Status
		statusCode = resp.Response.StatusCode
	}
	var apiErr *sdk.GenericOpenAPIError
	if errors.As(err, &apiErr) {
		if statusCode >= 200 && statusCode < 300 {
			msg := strings.TrimSpace(apiErr.Error())
			if msg != "" {
				if status != "" {
					return fmt.Errorf("okta api decode error: %s: %s", status, msg)
				}
				return fmt.Errorf("okta api decode error: %s", msg)
			}
		}

		if model := apiErr.Model(); model != nil {
			switch v := model.(type) {
			case sdk.Error:
				summary := strings.TrimSpace(v.GetErrorSummary())
				if summary != "" {
					if status != "" {
						return fmt.Errorf("okta api error: %s: %s", status, summary)
					}
					return fmt.Errorf("okta api error: %s", summary)
				}
			case *sdk.Error:
				summary := strings.TrimSpace(v.GetErrorSummary())
				if summary != "" {
					if status != "" {
						return fmt.Errorf("okta api error: %s: %s", status, summary)
					}
					return fmt.Errorf("okta api error: %s", summary)
				}
			}
		}
		body := strings.TrimSpace(string(apiErr.Body()))
		const maxBody = 4096
		if len(body) > maxBody {
			body = body[:maxBody] + fmt.Sprintf("... (truncated, %d bytes)", len(body))
		}
		if body != "" {
			if status != "" {
				return fmt.Errorf("okta api error: %s: %s", status, body)
			}
			return fmt.Errorf("okta api error: %s", body)
		}
	}
	if status != "" {
		return fmt.Errorf("okta api error: %s: %w", status, err)
	}
	return err
}
