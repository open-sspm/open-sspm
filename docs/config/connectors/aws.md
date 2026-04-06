# AWS Identity Center Connector

The AWS Identity Center connector syncs users, groups, permission sets, and account assignments.

## What Gets Synced

- Users
- Groups and group memberships
- Permission sets
- Account assignments

## Authentication

The connector supports:

- **Runtime credentials (`default_chain`)** - Recommended. Uses the AWS SDK default credentials chain.
- **Access keys (`access_key`)** - Stored in the connector configuration.

On Kubernetes, runtime credentials usually means IRSA or another workload identity mechanism.

## Required IAM Permissions

Open-SSPM calls the AWS SDK operations below. A working policy therefore needs these actions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ssoadmin:ListInstances",
        "ssoadmin:ListPermissionSets",
        "ssoadmin:DescribePermissionSet",
        "ssoadmin:ListAccountsForProvisionedPermissionSet",
        "ssoadmin:ListAccountAssignments",
        "identitystore:ListUsers",
        "identitystore:ListGroups",
        "identitystore:ListGroupMemberships"
      ],
      "Resource": "*"
    }
  ]
}
```

## Setup Instructions

### 1. Identify the AWS Region

Use the region where AWS Identity Center is configured.

### 2. Gather Instance Metadata

If you have a single Identity Center instance in that region, Open-SSPM can discover it automatically via `ListInstances`.

If you have multiple instances, or you want to pin the configuration explicitly, provide:

- **Instance ARN**
- **Identity Store ID**

### 3. Configure Authentication

**Recommended: runtime credentials**

- EC2 instance profile
- ECS task role
- EKS IRSA / workload identity
- Standard AWS SDK environment or shared config credentials

**Alternative: access keys**

- Access key ID
- Secret access key
- Optional session token

### 4. Configure in Open-SSPM

1. Go to **Settings → Connectors**
2. Open the AWS Identity Center connector
3. Enter:
   - **Region**
   - **Display name** (optional)
   - **Credentials**: runtime credentials or access keys
   - **Instance ARN** (recommended in multi-instance environments)
   - **Identity store ID** (recommended in multi-instance environments)
4. Save the configuration
5. Trigger a sync

## Settings

| Setting | Required | Description |
|---------|----------|-------------|
| Region | Yes | AWS region for Identity Center |
| Display name | No | Friendly label for the source |
| Credentials | Yes | `default_chain` or `access_key` |
| Access key ID | Conditional | Required for `access_key` |
| Secret access key | Conditional | Required for `access_key` |
| Session token | No | Optional for temporary credentials |
| Instance ARN | Recommended | Required when multiple instances exist |
| Identity store ID | Recommended | Required when multiple instances exist |

## Sync Tuning

```bash
SYNC_AWS_INTERVAL=15m
```

## Troubleshooting

### Access Denied

- Check the IAM policy includes the listed `ssoadmin` and `identitystore` actions
- Verify the runtime role or access keys are the ones Open-SSPM is actually using

### Instance Not Found

- Confirm the region is correct
- Set both Instance ARN and Identity Store ID explicitly when more than one instance exists

### Empty Results

- Verify users and assignments actually exist in Identity Center
- Trigger a fresh sync from **Settings → Connector health**
