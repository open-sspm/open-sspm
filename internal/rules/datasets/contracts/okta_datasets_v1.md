# Okta datasets (contract v1)

This document describes the current contract v1 for `okta:*` datasets emitted by the rules engine dataset provider.

All datasets are returned as an array of JSON objects (`[]any`) and are intended to be accessed via JSON Pointer paths.

## `okta:policies/sign-on` (v1)

Row fields (selected):

- `/resource_id` (string) — `okta_policy_rule:<rule_id>`
- `/display` (string) — rule name
- `/id` (string) — policy rule id
- `/name` (string) — policy rule name
- `/status` (string, optional) — rule status (vendor-defined)
- `/is_top_rule` (bool)
- `/system` (bool)
- `/priority` (number)
- `/policy/id` (string) — parent policy id
- `/policy/name` (string) — parent policy name
- `/actions/signon/session/maxSessionIdleMinutes` (number, optional)
- `/actions/signon/session/maxSessionLifetimeMinutes` (number, optional)
- `/actions/signon/session/usePersistentCookie` (bool, optional)

## `okta:policies/password` (v1)

Row fields (selected):

- `/resource_id` (string) — `okta_password_policy:<policy_id>`
- `/display` (string) — policy name
- `/id` (string)
- `/name` (string)
- `/status` (string)
- `/priority` (number)
- `/system` (bool)
- `/settings/password/complexity/minLength` (number, optional)
- `/settings/password/complexity/minUpperCase` (number, optional)
- `/settings/password/complexity/minLowerCase` (number, optional)
- `/settings/password/complexity/minNumber` (number, optional)
- `/settings/password/complexity/minSymbol` (number, optional)
- `/settings/password/complexity/dictionary/common/exclude` (bool, optional)
- `/settings/password/age/minAgeMinutes` (number, optional)
- `/settings/password/age/maxAgeDays` (number, optional)
- `/settings/password/age/historyCount` (number, optional)
- `/settings/password/lockout/maxAttempts` (number, optional)
- `/settings/password/lockout/autoUnlockMinutes` (number, optional)

## `okta:policies/app-signin` (v1)

Row fields (selected):

- `/resource_id` (string) — `okta_policy_rule:<rule_id>`
- `/display` (string) — rule name
- `/policy_id` (string)
- `/policy_name` (string)
- `/app_labels` (array of strings, optional)
- `/is_top_rule` (bool)
- `/requires_mfa` (bool)
- `/requires_phishing_resistant` (bool)

## `okta:apps` (v1)

Row fields (selected):

- `/resource_id` (string) — `okta_app:<app_id>`
- `/display` (string) — app label
- `/id` (string)
- `/label` (string)
- `/name` (string)
- `/access_policy_id` (string)

## `okta:authenticators` (v1)

Row fields (selected):

- `/resource_id` (string) — `okta_authenticator:<authenticator_id>`
- `/display` (string) — authenticator name
- `/id` (string)
- `/key` (string)
- `/name` (string)
- `/status` (string)
- `/okta_verify_compliance_fips` (string, optional)

## `okta:apps/admin-console-settings` (v1)

This dataset returns a single row.

Row fields:

- `/resource_id` (string) — `okta_first_party_app:admin-console`
- `/display` (string) — `Admin Console`
- `/session_idle_timeout_minutes` (number, optional)
- `/session_max_lifetime_minutes` (number, optional)

## `okta:brands/signin-page` (v1)

Row fields (selected):

- `/resource_id` (string) — `okta_brand:<brand_id>`
- `/display` (string) — brand name
- `/id` (string) — brand id
- `/name` (string) — brand name
- `/sign_in_page` (object) — raw response from `GET /api/v1/brands/{brandId}/pages/sign-in/customized` (may be empty)

## `okta:log-streams` (v1)

Rows are the raw response objects from `GET /api/v1/logStreams` with two additional fields:

- `/resource_id` (string) — `okta_log_stream:<id>`
- `/display` (string) — `name` (if present)
