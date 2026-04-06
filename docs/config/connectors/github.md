# GitHub Connector

The GitHub connector syncs organization members, teams, and repository permissions from your GitHub organization.

## What Gets Synced

- **Organization Members** - Users with access to your organization
- **Teams** - Team structure and memberships
- **Repositories** - Repository metadata
- **Permissions** - Team and individual access levels to repositories

## Prerequisites

- GitHub organization (personal accounts not supported)
- Personal access token with appropriate scopes

## Required Permissions

Create a personal access token with these scopes:

### Classic Tokens

- `read:org` - Read org and team membership
- `repo` - Access repository information (or `public_repo` for public repos only)

### Fine-Grained Tokens (Recommended)

- Organization permissions:
  - Members: Read-only
  - Administration: Read-only (optional, for SCIM email lookup)
- Repository permissions:
  - Metadata: Read-only
  - Administration: Read-only

## Setup Instructions

### Step 1: Create a Personal Access Token

**Classic Token:**

1. Go to GitHub → Settings → Developer settings → Personal access tokens → Tokens (classic)
2. Click "Generate new token (classic)"
3. Select scopes: `read:org`, `repo` (or `public_repo`)
4. Generate and copy the token

**Fine-Grained Token:**

1. Go to GitHub → Settings → Developer settings → Personal access tokens → Fine-grained tokens
2. Click "Generate new token"
3. Select your organization as the resource owner
4. Grant permissions:
   - Organization: Members (Read-only)
   - Repository: Metadata (Read-only)
5. Generate and copy the token

### Step 2: Get Your Organization Name

Your organization name is in the URL: `https://github.com/ORG-NAME`

### Step 3: Configure in Open-SSPM

1. Open Open-SSPM web UI
2. Go to Settings → Connectors
3. Click "Configure" on the GitHub card
4. Enter:
   - **Organization**: your org name (e.g., `acme-corp`)
   - **Token**: the personal access token
   - **Enable SCIM Lookup**: toggle on (optional, see below)
5. Click "Save"

## SCIM Email Lookup

GitHub organization members may not have public email addresses. Enable SCIM lookup to:

- Query your IdP (Okta/Azure AD) for user emails
- Improve identity matching accuracy
- Link more GitHub accounts to identities

**Requirements:**
- GitHub organization must have SCIM provisioning enabled
- Your IdP must be configured for GitHub provisioning

## Connector Settings

| Setting | Required | Description |
|---------|----------|-------------|
| Organization | Yes | GitHub organization name |
| Token | Yes | Personal access token |
| Enable SCIM Lookup | No | Look up emails via SCIM API |

## Environment Variables

Override the sync interval:

```bash
SYNC_GITHUB_INTERVAL=30m
```

Adjust concurrent workers (GitHub has aggressive rate limits):

```bash
SYNC_GITHUB_WORKERS=6
```

## Troubleshooting

### "Not Found" error

- Verify the organization name is correct (case-sensitive)
- Ensure the token has access to the organization

### "Bad credentials" error

- Token may be expired or revoked
- Generate a new token and update the connector

### Rate limiting

GitHub has strict rate limits (5,000 requests/hour for classic tokens). If you hit limits:

1. Increase sync interval: `SYNC_GITHUB_INTERVAL=60m`
2. Reduce worker count: `SYNC_GITHUB_WORKERS=1`
3. Use fine-grained tokens (may have higher limits)

### Missing repositories

- Check if repositories are private and token has `repo` scope
- Verify the organization member has access to those repositories

### Missing member emails

- GitHub users may not have public emails
- Enable SCIM lookup if your org uses SCIM provisioning
- Manually link unmatched accounts in the UI

## Data Retention

- Member and team data updates with each sync
- Repository permissions reflect current state
- Historical permission changes are not tracked

## Security Best Practices

1. **Use fine-grained tokens** - More secure than classic tokens
2. **Minimal permissions** - Only request read access
3. **Token rotation** - Regenerate tokens periodically
4. **Dedicated account** - Consider using a service account (not a personal account)
5. **Monitor token usage** - Review GitHub security logs

## GitHub Enterprise Server

For GitHub Enterprise Server (self-hosted), set the API base URL:

```
API Base URL: https://your-ghe-server.com/api/v3
```

Leave blank for GitHub.com (default: `https://api.github.com`).

## Next Steps

After configuring GitHub:

1. Run initial sync
2. Review organization members in Identities
3. Check which users have access to which repositories
4. Link any unmatched GitHub accounts to identities
