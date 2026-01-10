.PHONY: dev-up dev-down migrate sqlc templ templ-watch test run worker sync validate-rules ui ui-watch dev update-open-sspm-spec

dev-up:
	docker compose up -d

dev-down:
	docker compose down -v

migrate:
	go run ./cmd/open-sspm migrate

sqlc:
	sqlc generate

templ:
	templ generate -path internal/http/views -lazy

templ-watch:
	templ generate -path internal/http/views -watch

test:
	go test ./...

run:
	go run ./cmd/open-sspm serve

worker:
	go run ./cmd/open-sspm worker

sync:
	go run ./cmd/open-sspm sync

validate-rules:
	go run ./cmd/open-sspm validate-rules

ui:
	npm run build:css

ui-watch:
	npm run watch:css

dev:
	@command -v air >/dev/null 2>&1 || { echo "air is not installed. Install with: go install github.com/air-verse/air@latest"; exit 1; }
	@command -v templ >/dev/null 2>&1 || { echo "templ is not installed. Install with: go install github.com/a-h/templ/cmd/templ@latest"; exit 1; }
	air

update-open-sspm-spec:
	bash scripts/update-open-sspm-spec.sh
