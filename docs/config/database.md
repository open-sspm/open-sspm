# Database Configuration

Open-SSPM requires PostgreSQL 14 or later.

## Database Requirements

- **Version:** PostgreSQL 14, 15, 16, or 17
- **Extensions:** None required
- **Storage:** Depends on your data volume

## Connection URL Format

```text
postgres://USERNAME:PASSWORD@HOST:PORT/DATABASE?sslmode=MODE
```

### SSL Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| `disable` | No SSL | Local development only |
| `require` | SSL required, no verification | Trusted internal networks |
| `verify-ca` | SSL with CA verification | Production |
| `verify-full` | SSL with CA and hostname verification | Higher-assurance production setups |

## Setup Options

### Option 1: Repo-Local Postgres

For local development, the repository includes a Docker Compose service for Postgres:

```bash
just dev-up
```

Default local URL:

```bash
DATABASE_URL=postgres://postgres:postgres@localhost:5432/opensspm?sslmode=disable
```

### Option 2: Managed PostgreSQL

Examples:

```bash
DATABASE_URL=postgres://opensspm:password@opensspm.abc123.us-east-1.rds.amazonaws.com:5432/opensspm?sslmode=require
DATABASE_URL=postgres://opensspm:password@opensspm.postgres.database.azure.com:5432/opensspm?sslmode=require
```

### Option 3: Self-Managed PostgreSQL

Create a database and user:

```sql
CREATE USER opensspm WITH PASSWORD 'your-secure-password';
CREATE DATABASE opensspm OWNER opensspm;
GRANT ALL PRIVILEGES ON DATABASE opensspm TO opensspm;
```

## Migrations

Open-SSPM uses SQL migrations to manage schema changes.

### Running Migrations

Repo-local:

```bash
just migrate
```

Direct CLI:

```bash
open-sspm admin migrate
```

Kubernetes:

- The Helm chart runs migrations automatically with a pre-install and pre-upgrade hook Job.

There is no separate `open-sspm admin migrate status` subcommand in the current CLI.

## Backup and Restore

### Backup

```bash
pg_dump "$DATABASE_URL" > opensspm-backup.sql
```

Compressed:

```bash
pg_dump "$DATABASE_URL" | gzip > opensspm-backup.sql.gz
```

### Restore

```bash
psql "$DATABASE_URL" < opensspm-backup.sql
gunzip < opensspm-backup.sql.gz | psql "$DATABASE_URL"
```

## Troubleshooting

### Connection Refused

Check:

1. PostgreSQL is running
2. The host and port in `DATABASE_URL` are correct
3. Firewall or security group rules allow the connection

### Authentication Failed

Check:

1. The username and password are correct
2. The database exists
3. The user can connect and has the required privileges

### Migration Failures

Check:

1. The database user can create and alter schema objects
2. The migration job or command is using the expected `DATABASE_URL`
3. Previous partially applied changes were handled before rerunning
