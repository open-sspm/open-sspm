# Datadog Connector

The Datadog connector syncs users and role assignments from your Datadog organization.

When `open-sspm worker --lane tail` is running, Open-SSPM tails Datadog Audit Logs through the Audit Logs API. Datadog Audit Event Forwarding is not enabled as a push capability because it is treated as preview/optional; full sync remains the inventory repair path.

## What Gets Synced

- Users
- Roles
- Role assignments

## Prerequisites

- Datadog organization access
- A Datadog API key
- A Datadog application key tied to a principal that can read users and roles

## Setup Instructions

### 1. Create an API Key

1. Open Datadog organization settings
2. Create a new API key for Open-SSPM
3. Copy the value

### 2. Create an Application Key

1. Create a new application key
2. Copy the value

### 3. Determine Your Site Value

Enter the Datadog host, not the short site nickname.

| Datadog URL | Value to Enter in Open-SSPM |
|-------------|-----------------------------|
| `https://app.datadoghq.com` | `datadoghq.com` |
| `https://us3.datadoghq.com` | `us3.datadoghq.com` |
| `https://us5.datadoghq.com` | `us5.datadoghq.com` |
| `https://app.datadoghq.eu` | `datadoghq.eu` |
| `https://app.ddog-gov.com` | `ddog-gov.com` |

Open-SSPM normalizes full URLs and hostnames, but using the host directly is the clearest input.

### 4. Configure in Open-SSPM

1. Go to **Settings → Connectors**
2. Open the Datadog connector
3. Enter:
   - **Site**: for example `datadoghq.com` or `us3.datadoghq.com`
   - **API key**
   - **Application key**
4. Save the configuration
5. Trigger a sync

## Settings

| Setting | Required | Description |
|---------|----------|-------------|
| Site | Yes | Datadog site host, such as `datadoghq.com` |
| API Key | Yes | Datadog API key |
| Application Key | Yes | Datadog application key |

## Sync Tuning

```bash
SYNC_DATADOG_INTERVAL=15m
SYNC_DATADOG_WORKERS=3
SYNC_TAIL_INTERVAL=5m
```

## Troubleshooting

### Invalid API or Application Key

- Recreate the key and update the connector
- Make sure the keys belong to the expected Datadog site

### Site Errors

- Use the site host, not a shorthand like `us1` or `eu`
- Check that the value matches your Datadog URL

### Missing Users

- Verify the key owner can read users and roles
- Trigger a fresh sync from **Settings → Connector health**

### Audit Events Are Not Fresh

- Confirm `open-sspm worker --lane tail` is running.
- Verify the application key owner can read Datadog audit logs.
