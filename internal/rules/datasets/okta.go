package datasets

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"

	runtimev1 "github.com/open-sspm/open-sspm-spec/gen/go/opensspm/runtime/v1"
	"github.com/open-sspm/open-sspm/internal/connectors/oktaapi"
	"github.com/open-sspm/open-sspm/internal/rules/engine"
)

type OktaProvider struct {
	Client  oktaapi.Client
	BaseURL string
	Token   string

	signOnOnce sync.Once
	signOnRows []any
	signOnErr  error

	passwordOnce sync.Once
	passwordRows []any
	passwordErr  error

	accessPoliciesOnce sync.Once
	accessPolicies     []oktaapi.Policy
	accessPoliciesErr  error

	accessRulesMu    sync.Mutex
	accessRulesCache map[string]*oktaAccessRulesCache

	appSignInOnce sync.Once
	appSignInRows []any
	appSignInErr  error

	appsOnce sync.Once
	appsRows []any
	appsErr  error

	authenticatorsOnce sync.Once
	authenticatorsRows []any
	authenticatorsErr  error

	adminConsoleOnce sync.Once
	adminConsoleRows []any
	adminConsoleErr  error

	brandsOnce sync.Once
	brandsRows []any
	brandsErr  error

	brandPagesMu sync.Mutex
	brandPages   map[string]*oktaBrandSignInPageCache

	logStreamsOnce sync.Once
	logStreamsRows []any
	logStreamsErr  error
}

type oktaAccessRulesCache struct {
	once sync.Once
	rows []oktaapi.AccessPolicyRule
	err  error
}

type oktaBrandSignInPageCache struct {
	once sync.Once
	page map[string]any
	err  error
}

func (p *OktaProvider) Capabilities(ctx context.Context) []runtimev1.DatasetRef {
	_ = ctx
	if p == nil {
		return nil
	}
	out := make([]runtimev1.DatasetRef, 0, len(oktaCapabilitiesV1))
	for _, ds := range oktaCapabilitiesV1 {
		out = append(out, runtimev1.DatasetRef{Dataset: ds, Version: 1})
	}
	return out
}

func (p *OktaProvider) GetDataset(ctx context.Context, eval runtimev1.EvalContext, ref runtimev1.DatasetRef) runtimev1.DatasetResult {
	_ = eval

	if p == nil {
		return runtimev1.DatasetResult{
			Error: &runtimev1.DatasetError{
				Kind:    runtimev1.DatasetErrorKind_MISSING_DATASET,
				Message: "okta dataset provider is nil",
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

func (p *OktaProvider) getDatasetRows(ctx context.Context, datasetKey string, datasetVersion *int) ([]any, error) {
	if p == nil {
		return nil, engine.DatasetError{Kind: engine.DatasetErrorMissingDataset, Err: errors.New("okta dataset provider is nil")}
	}

	key := strings.TrimSpace(datasetKey)
	if err := p.requireVersion(key, datasetVersion); err != nil {
		return nil, err
	}

	switch key {
	case "okta:policies/sign-on":
		p.signOnOnce.Do(func() {
			p.signOnRows, p.signOnErr = p.loadSignOnPolicies(ctx)
		})
		return p.signOnRowsOrErr("okta:policies/sign-on", p.signOnRows, p.signOnErr)

	case "okta:policies/password":
		p.passwordOnce.Do(func() {
			p.passwordRows, p.passwordErr = p.loadPasswordPolicies(ctx)
		})
		return p.signOnRowsOrErr("okta:policies/password", p.passwordRows, p.passwordErr)

	case "okta:policies/app-signin":
		p.appSignInOnce.Do(func() {
			p.appSignInRows, p.appSignInErr = p.loadAppSignInPolicies(ctx)
		})
		return p.signOnRowsOrErr("okta:policies/app-signin", p.appSignInRows, p.appSignInErr)

	case "okta:apps":
		p.appsOnce.Do(func() {
			p.appsRows, p.appsErr = p.loadApps(ctx)
		})
		return p.signOnRowsOrErr("okta:apps", p.appsRows, p.appsErr)

	case "okta:authenticators":
		p.authenticatorsOnce.Do(func() {
			p.authenticatorsRows, p.authenticatorsErr = p.loadAuthenticators(ctx)
		})
		return p.signOnRowsOrErr("okta:authenticators", p.authenticatorsRows, p.authenticatorsErr)

	case "okta:apps/admin-console-settings":
		p.adminConsoleOnce.Do(func() {
			p.adminConsoleRows, p.adminConsoleErr = p.loadAdminConsoleSettings(ctx)
		})
		return p.signOnRowsOrErr("okta:apps/admin-console-settings", p.adminConsoleRows, p.adminConsoleErr)

	case "okta:brands/signin-page":
		p.brandsOnce.Do(func() {
			p.brandsRows, p.brandsErr = p.loadBrandSignInPages(ctx)
		})
		return p.signOnRowsOrErr("okta:brands/signin-page", p.brandsRows, p.brandsErr)

	case "okta:log-streams":
		p.logStreamsOnce.Do(func() {
			p.logStreamsRows, p.logStreamsErr = p.loadLogStreams(ctx)
		})
		return p.signOnRowsOrErr("okta:log-streams", p.logStreamsRows, p.logStreamsErr)

	default:
		return nil, engine.DatasetError{Kind: engine.DatasetErrorMissingDataset, Err: fmt.Errorf("unsupported dataset key %q", key)}
	}
}

func (p *OktaProvider) requireVersion(datasetKey string, datasetVersion *int) error {
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

func (p *OktaProvider) signOnRowsOrErr(datasetKey string, rows []any, err error) ([]any, error) {
	if err == nil {
		if rows == nil {
			return []any{}, nil
		}
		return rows, nil
	}
	return nil, p.asDatasetError(datasetKey, err)
}

func (p *OktaProvider) asDatasetError(datasetKey string, err error) error {
	if err == nil {
		return nil
	}

	var apiErr *oktaapi.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.StatusCode {
		case 401, 403:
			return engine.DatasetError{Kind: engine.DatasetErrorPermissionDenied, Err: err}
		case 404:
			return engine.DatasetError{Kind: engine.DatasetErrorMissingDataset, Err: err}
		default:
			return engine.DatasetError{Kind: engine.DatasetErrorSyncFailed, Err: err}
		}
	}

	if strings.TrimSpace(p.BaseURL) == "" || strings.TrimSpace(p.Token) == "" || p.Client == nil {
		return engine.DatasetError{Kind: engine.DatasetErrorMissingIntegration, Err: fmt.Errorf("%s: %w", strings.TrimSpace(datasetKey), err)}
	}

	return engine.DatasetError{Kind: engine.DatasetErrorSyncFailed, Err: fmt.Errorf("%s: %w", strings.TrimSpace(datasetKey), err)}
}

func (p *OktaProvider) loadSignOnPolicies(ctx context.Context) ([]any, error) {
	if p.Client == nil {
		return nil, errors.New("okta api client is nil")
	}

	policies, err := p.Client.ListPolicies(ctx, "OKTA_SIGN_ON")
	if err != nil {
		return nil, err
	}

	policy, ok, _ := selectSignOnPolicy(policies)
	if !ok {
		return []any{}, nil
	}

	rules, err := p.Client.ListOktaSignOnPolicyRules(ctx, strings.TrimSpace(policy.ID))
	if err != nil {
		return nil, err
	}

	topRuleID := ""
	if top, ok := topSignOnRule(rules); ok {
		topRuleID = strings.TrimSpace(top.ID)
	}

	rows := make([]any, 0, len(rules))
	for _, r := range rules {
		priority := 0
		if r.Priority != nil {
			priority = int(*r.Priority)
		}

		session := map[string]any{}
		if r.Session.MaxSessionIdleMinutes != nil {
			session["maxSessionIdleMinutes"] = int(*r.Session.MaxSessionIdleMinutes)
		}
		if r.Session.MaxSessionLifetimeMinutes != nil {
			v := int(*r.Session.MaxSessionLifetimeMinutes)
			// Okta uses 0 to mean "unbounded". Map to a large sentinel so
			// lte-based checks fail by default.
			if v == 0 {
				v = 1000000
			}
			session["maxSessionLifetimeMinutes"] = v
		}
		if r.Session.UsePersistentCookie != nil {
			session["usePersistentCookie"] = *r.Session.UsePersistentCookie
		}

		system := false
		if r.System != nil {
			system = *r.System
		}

		row := map[string]any{
			"resource_id": fmt.Sprintf("okta_policy_rule:%s", strings.TrimSpace(r.ID)),
			"display":     strings.TrimSpace(r.Name),
			"policy_id":   strings.TrimSpace(policy.ID),
			"policy_name": strings.TrimSpace(policy.Name),
			"policy": map[string]any{
				"id":     strings.TrimSpace(policy.ID),
				"type":   strings.TrimSpace(policy.Type),
				"name":   strings.TrimSpace(policy.Name),
				"status": strings.TrimSpace(policy.Status),
				"system": policy.System != nil && *policy.System,
			},
			"id":          strings.TrimSpace(r.ID),
			"name":        strings.TrimSpace(r.Name),
			"status":      strings.TrimSpace(r.Status),
			"priority":    priority,
			"system":      system,
			"is_top_rule": strings.TrimSpace(r.ID) != "" && strings.TrimSpace(r.ID) == topRuleID,
			"actions": map[string]any{
				"signon": map[string]any{
					"session": session,
				},
			},
		}
		rows = append(rows, row)
	}

	return rows, nil
}

func (p *OktaProvider) loadPasswordPolicies(ctx context.Context) ([]any, error) {
	if p.Client == nil {
		return nil, errors.New("okta api client is nil")
	}

	policies, err := p.Client.ListPasswordPolicies(ctx)
	if err != nil {
		return nil, err
	}

	rows := make([]any, 0, len(policies))
	for _, pol := range policies {
		priority := 0
		if pol.Priority != nil {
			priority = int(*pol.Priority)
		}
		system := false
		if pol.System != nil {
			system = *pol.System
		}

		row := map[string]any{
			"resource_id": fmt.Sprintf("okta_password_policy:%s", strings.TrimSpace(pol.ID)),
			"display":     strings.TrimSpace(pol.Name),
			"id":          strings.TrimSpace(pol.ID),
			"name":        strings.TrimSpace(pol.Name),
			"status":      strings.TrimSpace(pol.Status),
			"priority":    priority,
			"system":      system,
		}

		complexity := map[string]any{}
		if pol.Settings.Complexity.MinLength != nil {
			complexity["minLength"] = int(*pol.Settings.Complexity.MinLength)
		}
		if pol.Settings.Complexity.MinUpperCase != nil {
			complexity["minUpperCase"] = int(*pol.Settings.Complexity.MinUpperCase)
		}
		if pol.Settings.Complexity.MinLowerCase != nil {
			complexity["minLowerCase"] = int(*pol.Settings.Complexity.MinLowerCase)
		}
		if pol.Settings.Complexity.MinNumber != nil {
			complexity["minNumber"] = int(*pol.Settings.Complexity.MinNumber)
		}
		if pol.Settings.Complexity.MinSymbol != nil {
			complexity["minSymbol"] = int(*pol.Settings.Complexity.MinSymbol)
		}
		if pol.Settings.Complexity.CommonDictionaryExclude != nil {
			complexity["dictionary"] = map[string]any{
				"common": map[string]any{
					"exclude": *pol.Settings.Complexity.CommonDictionaryExclude,
				},
			}
		}

		age := map[string]any{}
		if pol.Settings.Age.MinAgeMinutes != nil {
			age["minAgeMinutes"] = int(*pol.Settings.Age.MinAgeMinutes)
		}
		if pol.Settings.Age.MaxAgeDays != nil {
			v := int(*pol.Settings.Age.MaxAgeDays)
			// Okta uses 0 to mean "unbounded". Map to a large sentinel so
			// lte-based checks fail by default.
			if v == 0 {
				v = 1000000
			}
			age["maxAgeDays"] = v
		}
		if pol.Settings.Age.HistoryCount != nil {
			age["historyCount"] = int(*pol.Settings.Age.HistoryCount)
		}

		lockout := map[string]any{}
		if pol.Settings.Lockout.MaxAttempts != nil {
			v := int(*pol.Settings.Lockout.MaxAttempts)
			// Okta uses 0 to mean "unbounded". Map to a large sentinel so
			// lte-based checks fail by default.
			if v == 0 {
				v = 1000000
			}
			lockout["maxAttempts"] = v
		}
		if pol.Settings.Lockout.AutoUnlockMinutes != nil {
			lockout["autoUnlockMinutes"] = int(*pol.Settings.Lockout.AutoUnlockMinutes)
		}

		row["settings"] = map[string]any{
			"password": map[string]any{
				"complexity": complexity,
				"age":        age,
				"lockout":    lockout,
			},
		}

		rows = append(rows, row)
	}

	return rows, nil
}

func (p *OktaProvider) loadAccessPolicies(ctx context.Context) ([]oktaapi.Policy, error) {
	p.accessPoliciesOnce.Do(func() {
		if p.Client == nil {
			p.accessPoliciesErr = errors.New("okta api client is nil")
			return
		}
		p.accessPolicies, p.accessPoliciesErr = p.Client.ListPolicies(ctx, "ACCESS_POLICY")
	})
	return p.accessPolicies, p.accessPoliciesErr
}

func (p *OktaProvider) loadAccessPolicyRules(ctx context.Context, policyID string) ([]oktaapi.AccessPolicyRule, error) {
	policyID = strings.TrimSpace(policyID)
	if policyID == "" {
		return nil, errors.New("okta policy id is required")
	}

	p.accessRulesMu.Lock()
	if p.accessRulesCache == nil {
		p.accessRulesCache = make(map[string]*oktaAccessRulesCache)
	}
	cache := p.accessRulesCache[policyID]
	if cache == nil {
		cache = &oktaAccessRulesCache{}
		p.accessRulesCache[policyID] = cache
	}
	p.accessRulesMu.Unlock()

	cache.once.Do(func() {
		if p.Client == nil {
			cache.err = errors.New("okta api client is nil")
			return
		}
		cache.rows, cache.err = p.Client.ListAccessPolicyRules(ctx, policyID)
	})

	return cache.rows, cache.err
}

func (p *OktaProvider) loadAppSignInPolicies(ctx context.Context) ([]any, error) {
	appLabelsByPolicy := make(map[string][]any)
	if apps, err := p.loadApps(ctx); err == nil {
		for _, row := range apps {
			m, ok := row.(map[string]any)
			if !ok {
				continue
			}
			policyID := strings.TrimSpace(getString(m, "access_policy_id"))
			if policyID == "" {
				continue
			}
			label := strings.TrimSpace(getString(m, "label"))
			if label == "" {
				continue
			}
			appLabelsByPolicy[policyID] = append(appLabelsByPolicy[policyID], label)
		}

		for policyID := range appLabelsByPolicy {
			labels := appLabelsByPolicy[policyID]
			sort.SliceStable(labels, func(i, j int) bool {
				li, _ := labels[i].(string)
				lj, _ := labels[j].(string)
				return li < lj
			})
			uniq := labels[:0]
			var prev string
			for _, v := range labels {
				s, _ := v.(string)
				if s == "" || s == prev {
					continue
				}
				prev = s
				uniq = append(uniq, s)
			}
			appLabelsByPolicy[policyID] = uniq
		}
	}

	policies, err := p.loadAccessPolicies(ctx)
	if err != nil {
		return nil, err
	}

	var rows []any
	for _, pol := range policies {
		policyID := strings.TrimSpace(pol.ID)
		if policyID == "" {
			continue
		}

		rules, err := p.loadAccessPolicyRules(ctx, policyID)
		if err != nil {
			return nil, err
		}

		topID := ""
		if top, ok := topAccessPolicyRule(rules); ok {
			topID = strings.TrimSpace(top.ID)
		}

		for _, r := range rules {
			ruleID := strings.TrimSpace(r.ID)
			if ruleID == "" {
				continue
			}

			priority := 0
			if r.Priority != nil {
				priority = int(*r.Priority)
			}
			system := false
			if r.System != nil {
				system = *r.System
			}

			mfaRequired, _ := accessPolicyRequiresMFA(r.AppSignOnVerificationMethod)
			phishResistant, _ := accessPolicyRequiresPhishingResistant(r.AppSignOnVerificationMethod)

			rows = append(rows, map[string]any{
				"resource_id": fmt.Sprintf("okta_policy_rule:%s", ruleID),
				"display":     strings.TrimSpace(r.Name),
				"policy_id":   policyID,
				"policy_name": strings.TrimSpace(pol.Name),
				"app_labels":  appLabelsByPolicy[policyID],
				"policy": map[string]any{
					"id":     policyID,
					"type":   strings.TrimSpace(pol.Type),
					"name":   strings.TrimSpace(pol.Name),
					"status": strings.TrimSpace(pol.Status),
					"system": pol.System != nil && *pol.System,
				},
				"id":                          ruleID,
				"name":                        strings.TrimSpace(r.Name),
				"status":                      strings.TrimSpace(r.Status),
				"priority":                    priority,
				"system":                      system,
				"is_top_rule":                 ruleID == topID,
				"requires_mfa":                mfaRequired,
				"requires_phishing_resistant": phishResistant,
				"verification_method": map[string]any{
					"type":        strings.TrimSpace(r.AppSignOnVerificationMethod.Type),
					"factor_mode": strings.TrimSpace(r.AppSignOnVerificationMethod.FactorMode),
				},
			})
		}
	}

	sort.SliceStable(rows, func(i, j int) bool {
		mi, _ := rows[i].(map[string]any)
		mj, _ := rows[j].(map[string]any)
		pi := strings.TrimSpace(getString(mi, "policy_id"))
		pj := strings.TrimSpace(getString(mj, "policy_id"))
		if pi != pj {
			return pi < pj
		}
		priI := getInt(mi, "priority")
		priJ := getInt(mj, "priority")
		if priI != priJ {
			return priI < priJ
		}
		return getString(mi, "id") < getString(mj, "id")
	})

	return rows, nil
}

func (p *OktaProvider) loadApps(ctx context.Context) ([]any, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(p.BaseURL), "/")
	if baseURL == "" {
		return nil, errors.New("okta base URL is required")
	}
	token := strings.TrimSpace(p.Token)
	if token == "" {
		return nil, errors.New("okta token is required")
	}

	var out []any
	after := ""

	for {
		u, err := url.Parse(baseURL + "/api/v1/apps")
		if err != nil {
			return nil, err
		}
		q := u.Query()
		q.Set("limit", "200")
		if after != "" {
			q.Set("after", after)
		}
		u.RawQuery = q.Encode()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("Authorization", "SSWS "+token)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 300 {
			return nil, &oktaapi.APIError{StatusCode: resp.StatusCode, Status: resp.Status, Summary: oktaErrorSummaryFromBody(body)}
		}

		var apps []oktaListApplication
		if err := json.Unmarshal(body, &apps); err != nil {
			return nil, fmt.Errorf("decode okta applications: %w", err)
		}
		for _, app := range apps {
			id := strings.TrimSpace(app.ID)
			if id == "" {
				continue
			}
			accessPolicyID := ""
			if app.Links.AccessPolicy != nil {
				accessPolicyID = oktaPolicyIDFromHref(app.Links.AccessPolicy.Href)
			}
			out = append(out, map[string]any{
				"resource_id":      fmt.Sprintf("okta_app:%s", id),
				"display":          strings.TrimSpace(app.Label),
				"id":               id,
				"label":            strings.TrimSpace(app.Label),
				"name":             strings.TrimSpace(app.Name),
				"access_policy_id": accessPolicyID,
			})
		}

		nextAfter := oktaNextAfterFromLink(resp.Header.Get("Link"))
		if nextAfter == "" {
			break
		}
		after = nextAfter
	}

	return out, nil
}

func (p *OktaProvider) loadAuthenticators(ctx context.Context) ([]any, error) {
	if p.Client == nil {
		return nil, errors.New("okta api client is nil")
	}

	auths, err := p.Client.ListAuthenticators(ctx)
	if err != nil {
		return nil, err
	}

	rows := make([]any, 0, len(auths))
	for _, a := range auths {
		id := strings.TrimSpace(a.ID)
		if id == "" {
			continue
		}
		row := map[string]any{
			"resource_id": fmt.Sprintf("okta_authenticator:%s", id),
			"display":     strings.TrimSpace(a.Name),
			"id":          id,
			"key":         strings.TrimSpace(a.Key),
			"name":        strings.TrimSpace(a.Name),
			"status":      strings.TrimSpace(a.Status),
		}
		if a.OktaVerifyComplianceFips != nil {
			if v := strings.TrimSpace(*a.OktaVerifyComplianceFips); v != "" {
				row["okta_verify_compliance_fips"] = v
			}
		}
		rows = append(rows, row)
	}

	return rows, nil
}

func (p *OktaProvider) loadAdminConsoleSettings(ctx context.Context) ([]any, error) {
	if p.Client == nil {
		return nil, errors.New("okta api client is nil")
	}

	settings, err := p.Client.GetAdminConsoleSettings(ctx)
	if err != nil {
		return nil, err
	}

	row := map[string]any{
		"resource_id": "okta_first_party_app:admin-console",
		"display":     "Admin Console",
	}
	if settings.SessionIdleTimeoutMinutes != nil {
		row["session_idle_timeout_minutes"] = int(*settings.SessionIdleTimeoutMinutes)
	}
	if settings.SessionMaxLifetimeMinutes != nil {
		row["session_max_lifetime_minutes"] = int(*settings.SessionMaxLifetimeMinutes)
	}
	return []any{row}, nil
}

func (p *OktaProvider) loadBrandSignInPages(ctx context.Context) ([]any, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(p.BaseURL), "/")
	if baseURL == "" {
		return nil, errors.New("okta base URL is required")
	}
	token := strings.TrimSpace(p.Token)
	if token == "" {
		return nil, errors.New("okta token is required")
	}

	brands, err := oktaListJSONPages[oktaBrand](ctx, baseURL, token, "/api/v1/brands")
	if err != nil {
		return nil, err
	}

	rows := make([]any, 0, len(brands))
	for _, b := range brands {
		brandID := strings.TrimSpace(b.ID)
		if brandID == "" {
			continue
		}

		page, err := p.brandSignInPage(ctx, baseURL, token, brandID)
		if err != nil {
			return nil, err
		}

		rows = append(rows, map[string]any{
			"resource_id":  fmt.Sprintf("okta_brand:%s", brandID),
			"display":      strings.TrimSpace(b.Name),
			"id":           brandID,
			"name":         strings.TrimSpace(b.Name),
			"sign_in_page": page,
		})
	}

	return rows, nil
}

func (p *OktaProvider) brandSignInPage(ctx context.Context, baseURL, token, brandID string) (map[string]any, error) {
	brandID = strings.TrimSpace(brandID)
	if brandID == "" {
		return map[string]any{}, nil
	}

	p.brandPagesMu.Lock()
	if p.brandPages == nil {
		p.brandPages = make(map[string]*oktaBrandSignInPageCache)
	}
	cache := p.brandPages[brandID]
	if cache == nil {
		cache = &oktaBrandSignInPageCache{}
		p.brandPages[brandID] = cache
	}
	p.brandPagesMu.Unlock()

	cache.once.Do(func() {
		page, err := oktaGetJSON(ctx, baseURL, token, "/api/v1/brands/"+url.PathEscape(brandID)+"/pages/sign-in/customized")
		if err != nil {
			var apiErr *oktaapi.APIError
			if errors.As(err, &apiErr) && apiErr.StatusCode == 404 {
				cache.page = map[string]any{}
				cache.err = nil
				return
			}
			cache.err = err
			return
		}
		cache.page = page
	})

	if cache.err != nil {
		return nil, cache.err
	}
	if cache.page == nil {
		return map[string]any{}, nil
	}
	return cache.page, nil
}

func (p *OktaProvider) loadLogStreams(ctx context.Context) ([]any, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(p.BaseURL), "/")
	if baseURL == "" {
		return nil, errors.New("okta base URL is required")
	}
	token := strings.TrimSpace(p.Token)
	if token == "" {
		return nil, errors.New("okta token is required")
	}

	streams, err := oktaListJSONPages[map[string]any](ctx, baseURL, token, "/api/v1/logStreams")
	if err != nil {
		return nil, err
	}

	rows := make([]any, 0, len(streams))
	for _, s := range streams {
		id := strings.TrimSpace(getString(s, "id"))
		if id == "" {
			continue
		}
		name := strings.TrimSpace(getString(s, "name"))
		item := make(map[string]any, len(s)+2)
		for k, v := range s {
			item[k] = v
		}
		item["resource_id"] = fmt.Sprintf("okta_log_stream:%s", id)
		item["display"] = name
		rows = append(rows, item)
	}

	return rows, nil
}

type oktaBrand struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type oktaListApplication struct {
	ID    string `json:"id"`
	Label string `json:"label"`
	Name  string `json:"name"`
	Links struct {
		AccessPolicy *struct {
			Href string `json:"href"`
		} `json:"accessPolicy"`
	} `json:"_links"`
}

func oktaListJSONPages[T any](ctx context.Context, baseURL, token, path string) ([]T, error) {
	var out []T
	after := ""
	for {
		u, err := url.Parse(strings.TrimRight(baseURL, "/") + path)
		if err != nil {
			return nil, err
		}
		q := u.Query()
		q.Set("limit", "200")
		if after != "" {
			q.Set("after", after)
		}
		u.RawQuery = q.Encode()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("Authorization", "SSWS "+strings.TrimSpace(token))

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 300 {
			return nil, &oktaapi.APIError{StatusCode: resp.StatusCode, Status: resp.Status, Summary: oktaErrorSummaryFromBody(body)}
		}

		var page []T
		if err := json.Unmarshal(body, &page); err != nil {
			return nil, err
		}
		out = append(out, page...)

		nextAfter := oktaNextAfterFromLink(resp.Header.Get("Link"))
		if nextAfter == "" {
			break
		}
		after = nextAfter
	}
	return out, nil
}

func oktaGetJSON(ctx context.Context, baseURL, token, path string) (map[string]any, error) {
	u, err := url.Parse(strings.TrimRight(baseURL, "/") + path)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "SSWS "+strings.TrimSpace(token))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	body, readErr := io.ReadAll(resp.Body)
	resp.Body.Close()
	if readErr != nil {
		return nil, readErr
	}
	if resp.StatusCode >= 300 {
		return nil, &oktaapi.APIError{StatusCode: resp.StatusCode, Status: resp.Status, Summary: oktaErrorSummaryFromBody(body)}
	}

	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func oktaErrorSummaryFromBody(body []byte) string {
	type payload struct {
		ErrorSummary string `json:"errorSummary"`
	}
	var p payload
	if err := json.Unmarshal(body, &p); err == nil {
		if s := strings.TrimSpace(p.ErrorSummary); s != "" {
			return s
		}
	}
	s := strings.TrimSpace(string(body))
	if len(s) > 500 {
		s = s[:500] + "…"
	}
	return s
}

func oktaNextAfterFromLink(link string) string {
	link = strings.TrimSpace(link)
	if link == "" {
		return ""
	}
	parts := strings.Split(link, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if !strings.Contains(part, `rel="next"`) {
			continue
		}

		start := strings.Index(part, "<")
		end := strings.Index(part, ">")
		if start == -1 || end == -1 || end <= start+1 {
			continue
		}

		u, err := url.Parse(part[start+1 : end])
		if err != nil {
			continue
		}
		after := strings.TrimSpace(u.Query().Get("after"))
		if after != "" {
			return after
		}
	}
	return ""
}

func oktaPolicyIDFromHref(href string) string {
	href = strings.TrimSpace(href)
	if href == "" {
		return ""
	}
	u, err := url.Parse(href)
	if err != nil {
		return ""
	}
	path := strings.Trim(u.Path, "/")
	if path == "" {
		return ""
	}
	parts := strings.Split(path, "/")
	for i := 0; i+1 < len(parts); i++ {
		if parts[i] == "policies" {
			return strings.TrimSpace(parts[i+1])
		}
	}
	return ""
}

func topSignOnRule(rules []oktaapi.SignOnPolicyRule) (oktaapi.SignOnPolicyRule, bool) {
	var (
		found        bool
		best         oktaapi.SignOnPolicyRule
		bestPriority int32
	)
	for _, r := range rules {
		if r.Priority == nil {
			continue
		}
		p := *r.Priority
		if !found || p < bestPriority {
			found = true
			best = r
			bestPriority = p
		}
	}
	return best, found
}

func topAccessPolicyRule(rules []oktaapi.AccessPolicyRule) (oktaapi.AccessPolicyRule, bool) {
	var (
		found        bool
		best         oktaapi.AccessPolicyRule
		bestPriority int32
	)
	for _, r := range rules {
		if r.Priority == nil {
			continue
		}
		p := *r.Priority
		if !found || p < bestPriority {
			found = true
			best = r
			bestPriority = p
		}
	}
	return best, found
}

func selectSignOnPolicy(policies []oktaapi.Policy) (oktaapi.Policy, bool, string) {
	if len(policies) == 0 {
		return oktaapi.Policy{}, false, "no active OKTA_SIGN_ON policies found"
	}

	var systemPolicies []oktaapi.Policy
	for _, p := range policies {
		if p.System != nil && *p.System {
			systemPolicies = append(systemPolicies, p)
		}
	}
	if len(systemPolicies) == 1 {
		return systemPolicies[0], true, ""
	}
	if len(systemPolicies) > 1 {
		return oktaapi.Policy{}, false, "multiple system OKTA_SIGN_ON policies found; unable to select primary"
	}

	if len(policies) == 1 {
		return policies[0], true, ""
	}

	return oktaapi.Policy{}, false, "multiple OKTA_SIGN_ON policies found; unable to select primary"
}

func intPtr(v *int32) any {
	if v == nil {
		return nil
	}
	return int(*v)
}

func boolPtr(v *bool) bool {
	if v == nil {
		return false
	}
	return *v
}

func getString(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	v, ok := m[key]
	if !ok || v == nil {
		return ""
	}
	s, _ := v.(string)
	return strings.TrimSpace(s)
}

func getInt(m map[string]any, key string) int {
	if m == nil {
		return 0
	}
	v, ok := m[key]
	if !ok || v == nil {
		return 0
	}
	switch t := v.(type) {
	case int:
		return t
	case float64:
		return int(t)
	default:
		return 0
	}
}

func accessPolicyRequiresMFA(method oktaapi.AccessPolicyRuleVerificationMethod) (bool, bool) {
	switch strings.TrimSpace(method.Type) {
	case "ASSURANCE":
		if strings.EqualFold(strings.TrimSpace(method.FactorMode), "ANY") {
			return true, true
		}
		if strings.EqualFold(strings.TrimSpace(method.FactorMode), "1FA") {
			return false, true
		}
		if strings.EqualFold(strings.TrimSpace(method.FactorMode), "2FA") {
			return true, true
		}
		return false, false
	case "AUTH_METHOD_CHAIN":
		if len(method.Chains) == 0 {
			return false, false
		}
		return true, true
	default:
		return false, false
	}
}

func accessPolicyRequiresPhishingResistant(method oktaapi.AccessPolicyRuleVerificationMethod) (bool, bool) {
	switch strings.TrimSpace(method.Type) {
	case "ASSURANCE":
		if len(method.Constraints) == 0 {
			return false, false
		}
		for _, c := range method.Constraints {
			if c.Possession == nil {
				continue
			}
			v := strings.TrimSpace(c.Possession.PhishingResistant)
			if v == "" {
				continue
			}
			return strings.EqualFold(v, "REQUIRED"), true
		}
		return false, true
	case "AUTH_METHOD_CHAIN":
		if len(method.Chains) == 0 {
			return false, false
		}
		found := false
		required := false
		for _, chain := range method.Chains {
			for _, am := range chain.AuthenticationMethods {
				v := strings.TrimSpace(am.PhishingResistant)
				if v == "" {
					continue
				}
				found = true
				if strings.EqualFold(v, "REQUIRED") {
					required = true
				}
			}
		}
		if !found {
			return false, true
		}
		return required, true
	default:
		return false, false
	}
}
