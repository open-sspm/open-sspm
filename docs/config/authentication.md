# Authentication

Open-SSPM includes built-in email/password authentication with server-side sessions stored in PostgreSQL.

## How Authentication Works

1. A user signs in with email and password.
2. Open-SSPM verifies the credentials against `auth_users`.
3. A server-side session is created and referenced by a cookie.
4. Subsequent requests use that session for authentication.

## Cookie and Session Security

### AUTH_COOKIE_SECURE

Controls whether session and CSRF cookies require HTTPS.

| Value | Behavior |
|-------|----------|
| `0` | Cookies work over HTTP |
| `1` | Cookies require HTTPS |

```bash
AUTH_COOKIE_SECURE=1
```

### Session Lifetime

Current defaults:

- **Idle timeout:** 12 hours
- **Maximum lifetime:** 14 days

Sessions are stored in Postgres and invalidated on logout.

## Trusted Proxies

Login rate limiting uses the client IP derived from `X-Forwarded-For`. If your direct upstream proxy uses public IPs, set `TRUSTED_PROXY_CIDRS`:

```bash
TRUSTED_PROXY_CIDRS=35.191.0.0/16,130.211.0.0/22
```

By default, private, link-local, and loopback ranges are trusted.

## Creating Users

### First Admin User

Repo-local example:

```bash
printf '%s\n' 'change-me-now' | go run ./cmd/open-sspm users bootstrap-admin \
  --email admin@example.com \
  --password-stdin
```

Kubernetes example:

```yaml
bootstrapAdmin:
  enabled: true
  existingSecret:
    name: open-sspm-admin
```

`bootstrap-admin` creates the first admin user and exits successfully if an admin already exists.

### Additional Users

Create and manage additional users in **Settings → Users**.

Admins can:

- Create users
- Change roles
- Reset other users' passwords
- Disable or delete users

### Development Mode

For local development only:

```bash
DEV_SEED_ADMIN=1
```

This seeds `admin@admin.com` / `admin` when there are no auth users yet.

## Password Handling

### Requirements

The UI enforces:

- Minimum length: 8 characters
- Maximum length: 128 characters

### Storage

Passwords are hashed with Argon2id before being stored.

## Login Rate Limiting

The `/login` endpoint is rate limited by client IP.

Current server settings:

- Burst: 10 attempts
- Refill rate: 0.5 requests per second
- Memory entry expiry: 10 minutes

Configure `TRUSTED_PROXY_CIDRS` correctly so the limiter sees the real client IP behind your ingress or load balancer.

## Troubleshooting

### Cannot Log In

Check:

1. The email and password are correct
2. `AUTH_COOKIE_SECURE` matches your protocol
3. Browser cookies are enabled
4. The user is still active

### Session Expired

Sign in again. Idle sessions expire after 12 hours by default.

### Rate Limited

Wait for the limiter to refill, then retry. Repeated bad credentials will continue to consume the same bucket.

### Forgot an Admin Password

Preferred recovery paths:

1. Sign in as another admin and reset the password in **Settings → Users**
2. If no admin can sign in, recover directly in Postgres against the `auth_users` table, then sign in again

There is no built-in email-based password reset flow in the current application.
