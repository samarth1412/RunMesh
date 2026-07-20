.PHONY: test lint compose-up compose-down migrate web

test:
	go test ./cmd/... ./internal/... ./sdk/go/...
	cd sdk/python && python -m pytest
	cd web && npm test -- --run

lint:
	gofmt -w $$(find cmd internal sdk/go -name '*.go')
	go vet ./...
	cd sdk/python && ruff check . && mypy runmesh
	cd web && npm run lint

compose-up:
	docker compose up --build

compose-down:
	docker compose down --remove-orphans

migrate:
	docker compose run --rm migrate

integration:
	go test -tags=integration -count=1 ./tests/integration

web:
	cd web && npm run dev
