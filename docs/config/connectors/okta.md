# Okta Connector

The Okta connector syncs users, groups, applications, and assignments from your Okta organization. When discovery is enabled, it also ingests Okta System Log evidence for SaaS discovery.

## What Gets Synced

- Users
- Groups
- Applications
- App assignments
- Discovery evidence from SSO, OAuth consent, and app-assignment events

## Ingest Modes

Open-SSPM supports polling and push for Okta discovery. Polling remains the completeness and backfill path because Okta push delivery is at-least-once and Log Streaming has no replay.

| Mode | Requires | Best for | Notes |
|------|----------|----------|-------|
| Polling | Okta API token | Any deployment | Pulls the System Log API on a schedule. |
| Event Hook | Public HTTPS `/ingest/okta/events` endpoint and shared secret | Non-AWS deployments that can expose inbound HTTPS | Queues only discovery-relevant eligible events. |
| EventBridge | AWS EventBridge relay to `/ingest/okta/eventbridge` and shared secret | AWS customers who want full System Log push | Carries full System Log events, including `app.oauth2.signon`. |
| Hybrid | API token plus one push channel | Recommended production setup | Push improves latency; polling covers history and gaps. |

Open-SSPM does not store Okta push data as a long-term log archive. Push payloads are kept in a short-retention Postgres inbox for processing and dead-letter triage; non-discovery events are dropped before storage. Use a SIEM or object storage if you need full raw log retention.

## Prerequisites

- Okta admin access
- An API token for polling, full syncs, and backfill
- A public HTTPS endpoint if Event Hook push is enabled
- An AWS EventBridge relay if EventBridge push is enabled

## Setup Instructions

### 1. Determine Your Okta Domain

Use the Okta host without the `https://` prefix, for example:

- `yourcompany.okta.com`
- `yourcompany.oktapreview.com`

### 2. Create an API Token

1. Open the Okta Admin Console.
2. Go to **Security → API → Tokens**.
3. Create a token for Open-SSPM.
4. Copy the value.

The API token is required for normal identity syncs and for polling-based discovery. Keep it configured even when push is enabled so Open-SSPM can backfill gaps.

### 3. Configure Open-SSPM

1. Go to **Settings → Connectors**.
2. Open the Okta connector.
3. Enter the Okta domain and API token.
4. Enable **SaaS discovery** if you want discovery evidence.
5. Choose the discovery ingest mode.
6. Configure Event Hook or EventBridge secrets if a push mode is enabled.
7. Save the configuration and trigger a sync.

## Event Hook Push

Configure an Okta Event Hook that points to:

```text
https://<your-open-sspm-host>/ingest/okta/events
```

Use the same secret value in Okta and in **Event Hook secret**. Open-SSPM compares the request `Authorization` header to this secret and returns `401` or `403` for missing or mismatched secrets. Okta does not retry 4xx responses, so validate the secret carefully before enabling production delivery.

Treat the Event Hook secret as a bearer credential: anyone who learns it can submit events until the secret is rotated. Open-SSPM does not verify a separate vendor HMAC signature for Event Hooks, so expose the endpoint only over HTTPS and consider a reverse proxy, WAF rule, or IP allowlist when your deployment model supports it. Replayed delivery IDs are deduplicated by Okta event ID, but replay protection does not stop fabricated events from a caller with the secret.

Open-SSPM accepts and stores these discovery-relevant Event Hook events:

- `user.authentication.sso`
- `app.oauth2.as.consent.grant`
- `app.oauth2.as.consent.revoke`
- `app.oauth2.as.consent.revoke.implicit.as`
- `app.oauth2.as.consent.revoke.implicit.client`
- `app.oauth2.as.consent.revoke.implicit.scope`
- `app.oauth2.as.consent.revoke.implicit.user`
- `app.oauth2.as.consent.revoke.user`
- `app.oauth2.as.consent.revoke.user.client`
- `application.user_membership.add`
- `application.user_membership.remove`
- `application.user_membership.update`

Other valid Okta System Log events are acknowledged and dropped before storage.
Event Hooks do not deliver every Okta System Log event; for example, `app.oauth2.signon` is handled by the poller or EventBridge.

Verification note: Open-SSPM accepts an unauthenticated Okta verification `GET` only when exactly one Event Hook receiver secret is configured. Delivery `POST` requests always require `Authorization`.

## EventBridge Push

Okta Log Streaming can send the full System Log to AWS EventBridge. Route matching EventBridge events to:

```text
https://<your-open-sspm-host>/ingest/okta/eventbridge
```

The HTTP relay must pass the configured EventBridge secret in the `Authorization` header. Open-SSPM expects EventBridge envelopes with `detail-type` equal to `SystemLog` and `source` beginning with `aws.partner/okta.com/`. Keep the relay private to your AWS account where possible, rotate the shared secret periodically, and avoid forwarding unrelated EventBridge traffic to this endpoint.

EventBridge is optional. Customers who do not use AWS should use polling, Event Hooks, or hybrid polling plus Event Hooks.

## Settings

| Setting | Required | Description |
|---------|----------|-------------|
| Domain | Yes | Okta host, for example `yourcompany.okta.com`. |
| API Token | Required for polling/full sync | Okta API token. Keep configured for backfill and recovery. |
| Discovery enabled | No | Enable discovery evidence ingestion. |
| Discovery ingest mode | No | `polling`, `event_hook`, `eventbridge`, or `hybrid`. |
| Event Hook receiver | Required for Event Hook mode | Enables `/ingest/okta/events`. |
| Event Hook secret | Required for Event Hook mode | Shared secret expected in the `Authorization` header. |
| EventBridge receiver | Required for EventBridge mode | Enables `/ingest/okta/eventbridge`. |
| EventBridge secret | Required for EventBridge mode | Shared secret expected in the `Authorization` header. |

Push endpoints only accept deliveries when global discovery is enabled, the Okta connector is enabled, Okta discovery is enabled, and the configured ingest mode includes the delivery channel. For example, an Event Hook secret configured while the mode is `polling` is stored but not accepted by `/ingest/okta/events`.

## Sync Tuning

```bash
SYNC_OKTA_INTERVAL=15m
SYNC_OKTA_WORKERS=3
```

Push processing runs in the `serve` process and does not require a separate API service. The inbox processor polls queued rows every five seconds, reclaims stuck `processing` rows after five minutes, retries transient failures with exponential backoff (up to 10 attempts), then deletes processed and ignored rows after 30 days and dead-letter rows after 90 days.

After enabling a push channel, run an Okta discovery sync from **Settings → Connector Health**. That backfills recent history through the System Log API and keeps the source from looking healthy on push delivery alone.

## Troubleshooting

### Invalid Token

- Regenerate the token and update the connector.

### Access Denied

- Verify the token can read users, groups, applications, and assignments.
- For discovery, verify the token owner can access the required System Log data.

### Event Hook Verification Fails

- Confirm the public HTTPS endpoint reaches the Open-SSPM `serve` process.
- Confirm the Event Hook receiver is enabled.
- If Okta sends an `Authorization` header during verification, confirm it matches the configured Event Hook secret.

### Push Events Are Not Processed

- Check the connector configuration card for push ingest status, queue depth, and dead-letter count.
- Confirm the request path is `/ingest/okta/events` for Event Hooks or `/ingest/okta/eventbridge` for EventBridge.
- Confirm the `Authorization` header exactly matches the configured secret.
- For EventBridge, confirm the envelope has `detail-type: "SystemLog"` and an Okta partner `source`.

### Discovery Data Missing

- Enable discovery on the connector.
- Make sure `SYNC_DISCOVERY_ENABLED=1`.
- Keep polling or hybrid mode enabled for backfill and gap recovery.
- Trigger a sync from **Settings → Connector Health** after enabling push for the first time.
