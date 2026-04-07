# Microsoft Entra ID Connector

The Microsoft Entra ID connector syncs users, groups, directory-role memberships, enterprise app access entitlements, app registrations, service principals, and optional discovery data from Microsoft Graph.

## What Gets Synced

- Users
- Groups
- Active directory-role memberships
- Effective enterprise app access entitlements
- App registrations
- Service principals
- Discovery evidence when enabled

## Prerequisites

- A Microsoft Entra tenant
- An application registration
- Admin consent for the required Microsoft Graph application permissions

## Known Working Graph Permissions

The in-app connector form currently calls out this working permission set for full sync:

- `User.Read.All`
- `Group.Read.All`
- `Application.Read.All`
- `AppRoleAssignment.Read.All`
- `RoleManagement.Read.Directory`

For discovery, also grant:

- `AuditLog.Read.All`
- `Directory.Read.All`
- `DelegatedPermissionGrant.Read.All`

## Setup Instructions

### 1. Register an Application

Create an Entra application registration for Open-SSPM and note:

- **Application (client) ID**
- **Directory (tenant) ID**

### 2. Create a Client Secret

Create a client secret and save the value immediately.

### 3. Grant Graph Permissions

Add the application permissions listed above and grant admin consent.

### 4. Configure in Open-SSPM

1. Go to **Settings → Connectors**
2. Open the Microsoft Entra ID connector
3. Enter:
   - **Tenant ID**
   - **Client ID**
   - **Client secret**
   - **Discovery enabled** if needed
4. Save the configuration
5. Trigger a sync

## Sync Tuning

```bash
SYNC_ENTRA_INTERVAL=15m
```

## Troubleshooting

### Invalid Client Secret

- Confirm the secret has not expired
- Recreate it if necessary

### Insufficient Privileges

- Verify the Graph application permissions were granted
- Re-run admin consent if needed

### Discovery Data Missing

- Enable discovery on the connector
- Confirm the extra discovery permissions were granted
- Make sure the discovery worker is running
