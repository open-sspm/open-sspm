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
    go run ./cmd/open-sspm migrate

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

# Start the HTTP server
run:
    go run ./cmd/open-sspm serve

# Run the background worker
worker:
    go run ./cmd/open-sspm worker

# Run a one-off sync
sync:
    go run ./cmd/open-sspm sync

# Validate security rules
validate-rules:
    go run ./cmd/open-sspm validate-rules

#
# UI / CSS
#

# Build CSS with Tailwind (production)
ui:
    npm run build:css

# Watch CSS for changes (development)
ui-watch:
    npm run watch:css

# Run frontend JavaScript unit tests
ui-test:
    npm run test:js

#
# Maintenance
#

# Update Open SSPM specification
update-open-sspm-spec:
    bash scripts/update-open-sspm-spec.sh
