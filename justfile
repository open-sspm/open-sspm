# Automatically load .env file for local development
set dotenv-load := true

# Show available commands
default:
    @just --list

#
# Development Environment
#

# Start local Postgres with Docker Compose
dev-up:
    docker compose up -d

# Stop local Postgres
dev-down:
    docker compose down -v

# Live reload development server (requires air and templ)
dev:
    #!/usr/bin/env bash
    if ! command -v air >/dev/null 2>&1; then
        echo "air is not installed. Install with: go install github.com/air-verse/air@latest"
        exit 1
    fi
    if ! command -v templ >/dev/null 2>&1; then
        echo "templ is not installed. Install with: go install github.com/a-h/templ/cmd/templ@latest"
        exit 1
    fi
    air

#
# Database
#

# Run database migrations
migrate:
    go run ./cmd/open-sspm admin migrate

#
# Code Generation
#

# Generate SQLC code from queries
sqlc:
    sqlc generate

# Generate templ templates
templ:
    templ generate -path internal/http/views -lazy

# Watch templ templates for changes
templ-watch:
    templ generate -path internal/http/views -watch

#
# Testing & Linting
#

# Run all unit tests
test:
    go test ./...

# Run Go vet for static analysis
lint:
    go vet ./...

#
# Application Commands
#

# Start the API and web UI HTTP server
run:
    go run ./cmd/open-sspm api

# Start the API and web UI HTTP server
api:
    go run ./cmd/open-sspm api

# Run a background worker lane: full, discovery, event-inbox, tail, or evaluator
worker lane="full":
    go run ./cmd/open-sspm worker --lane={{lane}}

# Run one-off sync lanes: all, full, or discovery
sync lane="all":
    go run ./cmd/open-sspm admin sync --lane={{lane}}

# Validate security rules
validate-rules:
    go run ./cmd/open-sspm admin validate-rules

# Validate evaluator policy packs
validate-policy-packs:
    go run ./cmd/open-sspm admin validate-policy-packs

#
# UI / CSS
#

# Build CSS with Tailwind (production)
ui:
    npm run build:css

# Watch CSS for changes (development)
ui-watch:
    npm run watch:css

# Run the docs site locally
docs:
    npm run docs:dev

# Build the docs site
docs-build:
    npm run docs:build

# Sync vendored frontend runtime JS from node_modules
vendor-sync:
    npm run vendor:sync

# Verify vendored frontend runtime JS is in sync with node_modules
vendor-check:
    npm run vendor:check

# Run frontend JavaScript unit tests
ui-test:
    npm run test:js

#
# Maintenance
#

# Update Open SSPM specification
update-open-sspm-spec:
    bash scripts/update-open-sspm-spec.sh
