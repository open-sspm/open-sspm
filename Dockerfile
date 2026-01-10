# syntax=docker/dockerfile:1

# ============================================
# FRONTEND STAGE
# ============================================
FROM node:22-alpine AS frontend
WORKDIR /app

# Install dependencies
COPY package.json package-lock.json ./
RUN npm ci

# Copy static source and build CSS
COPY web/static ./web/static
COPY internal/http/views ./internal/http/views
RUN npm run build:css

# ============================================
# GO BUILDER STAGE
# ============================================
FROM golang:1.25-alpine AS builder

# Install git (required for fetching some go modules)
RUN apk add --no-cache git

WORKDIR /app

# Copy go module files
COPY go.mod go.sum ./

# Download dependencies with cache mount
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

# Copy source code
COPY . .

# Build the Go binary
# Note: We rely on committed generated code (sqlc/templ) rather than regenerating it here
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /open-sspm ./cmd/open-sspm

# ============================================
# FINAL STAGE
# ============================================
FROM alpine:3.20 AS runner

# Install runtime dependencies
RUN apk add --no-cache ca-certificates wget

# Create non-root user
RUN addgroup --system --gid 1001 appgroup && \
    adduser --system --uid 1001 --ingroup appgroup appuser

WORKDIR /home/appuser

# Copy binary from builder
COPY --from=builder --chown=appuser:appgroup /open-sspm /usr/local/bin/open-sspm

# Copy static assets from frontend stage (contains built CSS)
COPY --from=frontend --chown=appuser:appgroup /app/web/static /home/appuser/web/static

# Copy DB migrations for `open-sspm migrate`
COPY --from=builder --chown=appuser:appgroup /app/db/migrations /home/appuser/db/migrations

# Switch to non-root user
USER appuser

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/healthz || exit 1

ENTRYPOINT ["open-sspm"]
