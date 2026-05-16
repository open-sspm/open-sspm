# Google Workspace Connector

The Google Workspace connector syncs users, groups, admin roles, and OAuth grants from your Google Workspace organization.

When `worker-tail` is running, Open-SSPM also tails Google Workspace Reports activities with a cursor-locked watermark overlap. This improves freshness for token/login audit evidence, but full reconciliation remains the repair path for current inventory.

## What Gets Synced

- **Users** - All users with profile information
- **Groups** - Groups and their memberships
- **Admin Roles** - Admin role assignments
- **OAuth Grants** - Third-party app authorizations (with discovery enabled)
- **Token Activity** - OAuth token audit events (with discovery enabled)

## Prerequisites

- Google Workspace Business Plus, Enterprise, or Education edition
- Google Cloud project with Admin SDK enabled
- Service account with domain-wide delegation

## Required Setup

### Step 1: Create a Google Cloud Project

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select existing
3. Enable the APIs:
   - Admin SDK API
   - Reports API (for discovery)

### Step 2: Create a Service Account

1. Go to IAM & Admin → Service Accounts
2. Click "Create Service Account"
3. Name: `open-sspm`
4. Grant roles: none needed (use domain-wide delegation instead)
5. Enable "Domain-wide delegation"
6. Note the Client ID (numeric ID)

### Step 3: Create and Download Keys

1. Select your service account
2. Go to Keys → Add Key → Create new key
3. Choose JSON format
4. Download and save the JSON key file

### Step 4: Authorize API Scopes

1. Go to [Google Admin Console](https://admin.google.com/)
2. Security → Access and data control → API controls
3. Click "Manage Domain Wide Delegation"
4. Click "Add new"
5. Enter the Client ID from Step 2
6. Add these OAuth scopes:

```
https://www.googleapis.com/auth/admin.directory.user.readonly
https://www.googleapis.com/auth/admin.directory.group.readonly
https://www.googleapis.com/auth/admin.directory.group.member.readonly
https://www.googleapis.com/auth/admin.directory.rolemanagement.readonly
https://www.googleapis.com/auth/admin.directory.user.security
https://www.googleapis.com/auth/admin.reports.audit.readonly
```

7. Click "Authorize"

### Step 5: Get Your Customer ID

1. In Google Admin Console, go to Account → Account settings
2. Find your "Customer ID" (starts with `C`)
3. Also note your primary domain

### Step 6: Configure in Open-SSPM

1. Open Open-SSPM web UI
2. Go to Settings → Connectors
3. Click "Configure" on the Google Workspace card
4. Enter:
   - **Customer ID**: Your Google customer ID
   - **Primary Domain**: Your domain (e.g., `example.com`)
   - **Delegated Admin Email**: A super admin email address
   - **Auth Type**: Service Account JSON
   - **Service Account JSON**: Paste the contents of the downloaded JSON key file
   - **Enable Discovery**: Toggle on (optional)
5. Click "Save"

## Alternative: ADC Authentication

Instead of uploading a JSON key, you can use Application Default Credentials (ADC):

1. Set up ADC on your Open-SSPM server:
   - Use workload identity (GKE)
   - Or use `gcloud auth application-default login`
2. In connector settings:
   - Select Auth Type: ADC
   - Enter the service account email

## Connector Settings

| Setting | Required | Description |
|---------|----------|-------------|
| Customer ID | Yes | Google customer ID (starts with C) |
| Primary Domain | No | Your domain (for display) |
| Delegated Admin Email | Yes | Super admin email for API access |
| Auth Type | Yes | Service Account JSON or ADC |
| Service Account JSON | Conditional | JSON key file contents |
| Service Account Email | Conditional | For ADC auth |
| Enable Discovery | No | Sync OAuth grants and token activity |

## Environment Variables

Override the sync interval:

```bash
SYNC_GOOGLE_WORKSPACE_INTERVAL=15m
SYNC_TAIL_INTERVAL=5m
```

## Troubleshooting

### "Not authorized" error

- Verify domain-wide delegation is enabled
- Check all 6 OAuth scopes are authorized in Admin Console
- Ensure the delegated admin email is a super admin

### "Customer not found" error

- Verify the customer ID is correct
- Customer ID starts with uppercase C (e.g., `C12345678`)

### Missing OAuth data

- Discovery must be enabled to sync OAuth grants
- Reports API must be enabled in Google Cloud
- Token activity may have a 24-48 hour delay
- Make sure `open-sspm worker-tail` is running for incremental Reports activity catch-up

### Service account issues

- Don't delete the service account - it will invalidate the connector
- If you regenerate keys, update the connector configuration

## Data Retention

- User and group data updates with each sync
- OAuth grants show current authorizations
- Token audit events may be retained based on Google Workspace settings

## Security Best Practices

1. **Dedicated service account** - Don't reuse for other purposes
2. **Minimal scopes** - Only authorize the 6 scopes listed
3. **Monitor delegation** - Review authorized apps in Google Admin Console
4. **Secure key file** - Don't commit the JSON key to version control
5. **Rotate keys** - Generate new keys periodically

## Google Workspace Editions

Required edition for full features:

| Feature | Required Edition |
|---------|-----------------|
| User/Group sync | Any Workspace edition |
| OAuth grants | Business Plus or higher |
| Token activity | Enterprise or Education |

## Next Steps

After configuring Google Workspace:

1. Run initial sync
2. Review users and groups in Identities
3. Check OAuth grants for third-party apps
4. Link any unmatched accounts to identities
