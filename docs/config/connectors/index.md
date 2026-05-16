# Connectors

Connectors integrate Open-SSPM with your identity providers and SaaS applications.

## Connector Types

### Identity Providers

IdP connectors import users, groups, and application assignments.

| Connector | Discovery Support | Description |
|-----------|-------------------|-------------|
| [Okta](/config/connectors/okta) | Yes, polling or push-assisted; System Log tail | Users, groups, apps, and assignments |
| [Microsoft Entra ID](/config/connectors/entra) | Yes; delta remains in full lane | Users, groups, app registrations, and service principals |

### Connected Apps

| Connector | Description |
|-----------|-------------|
| [Google Workspace](/config/connectors/google-workspace) | Users, groups, admin roles, OAuth grants, token activity, and Reports tail |
| [GitHub](/config/connectors/github) | Organization members, teams, and repository permissions |
| [Datadog](/config/connectors/datadog) | Users, role assignments, and Audit Logs tail |
| [AWS Identity Center](/config/connectors/aws) | Users, groups, permission sets, account assignments, and CloudTrail tail |

## SaaS Discovery

Discovery uses activity evidence from supported identity providers to identify:

- Discovered apps
- High-usage or sensitive hotspots
- Governance opportunities

Discovery requires:

- `SYNC_DISCOVERY_ENABLED=1`
- The discovery worker running
- Discovery enabled on the relevant IdP connector

## Realtime Sync

Realtime and near-realtime support is capability-driven:

- Push deliveries enter the generic inbox and are processed by `worker-ingest`.
- Cursor-based provider tails run in `worker-tail`.
- Canonical event evidence is stored in `events` and `event_targets`.
- Event policy evaluation runs in shadow mode through `worker-riskpolicy`.

See [Real-Time Synchronization](/run/real-time-synchronization) for the current provider matrix.

## Account Linking

Open-SSPM automatically links accounts by matching email addresses case-insensitively.

When an account cannot be linked automatically, use the connector-specific **Unlinked** views exposed from the relevant account pages, such as:

- GitHub unlinked accounts
- Microsoft Entra ID unlinked users
- Google Workspace unlinked users
- AWS Identity Center unlinked users
- Datadog unlinked users

Manual links persist across syncs.

## Configuration Flow

1. Go to **Settings → Connectors**
2. Open the connector you want to configure
3. Enter the required credentials or metadata
4. Enable discovery where supported and needed
5. Save the configuration
6. Trigger a sync

## Manual Syncs

- **Settings → Resync data → Resync now** queues the global sync lanes
- **Settings → Connector health → Trigger sync** queues a sync for a specific connector

## Security Notes

- Connector secrets are encrypted at rest with `CONNECTOR_SECRET_KEY`
- Prefer read-only credentials where the source system supports them
- Rotate connector credentials periodically

## Troubleshooting

### Sync Failures

Check:

1. The connector credentials are still valid
2. The source-side permissions are sufficient
3. Background workers are running
4. The connector health page shows the latest error details

### Missing Data

Check:

1. The connector is configured and enabled
2. A sync has completed successfully
3. The relevant lane is running (full, discovery, ingest, tail, or riskpolicy)

## Next Steps

Choose a connector page below for provider-specific setup instructions.
