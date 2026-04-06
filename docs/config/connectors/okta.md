# Okta Connector

The Okta connector syncs users, groups, applications, and assignments from your Okta organization.

## What Gets Synced

- Users
- Groups
- Applications
- App assignments

## Discovery

When discovery is enabled, the connector also ingests Okta activity evidence used for SaaS discovery.

## Prerequisites

- Okta admin access
- An API token that can read the data you want Open-SSPM to sync

## Setup Instructions

### 1. Create an API Token

1. Open the Okta Admin Console
2. Go to **Security → API → Tokens**
3. Create a token for Open-SSPM
4. Copy the value

### 2. Determine Your Okta Domain

Use the Okta host without the `https://` prefix, for example:

- `yourcompany.okta.com`
- `yourcompany.oktapreview.com`

### 3. Configure in Open-SSPM

1. Go to **Settings → Connectors**
2. Open the Okta connector
3. Enter:
   - **Domain**
   - **API token**
   - **Discovery enabled** if you want SaaS discovery
4. Save the configuration
5. Trigger a sync

## Settings

| Setting | Required | Description |
|---------|----------|-------------|
| Domain | Yes | Okta host, for example `yourcompany.okta.com` |
| API Token | Yes | Okta API token |
| Discovery enabled | No | Enable discovery evidence ingestion |

## Sync Tuning

```bash
SYNC_OKTA_INTERVAL=15m
SYNC_OKTA_WORKERS=3
```

## Troubleshooting

### Invalid Token

- Regenerate the token and update the connector

### Access Denied

- Verify the token can read users, groups, applications, and assignments
- For discovery, verify the token owner can access the required log data

### Discovery Data Missing

- Enable discovery on the connector
- Make sure `SYNC_DISCOVERY_ENABLED=1`
- Run the discovery worker
