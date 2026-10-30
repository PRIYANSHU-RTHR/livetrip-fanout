GO ?= go
COMPOSE ?= docker compose -f deploy/docker-compose.yml
CMD ?= gateway
REDIS_ADDR ?= localhost:6379

.PHONY: build test test-redis redis lint run up down

build:
	$(GO) build ./...

test:
	$(GO) test -race ./...

# Integration tests; needs `make redis` first.
test-redis:
	REDIS_ADDR=$(REDIS_ADDR) $(GO) test -race -count=1 ./internal/eventlog/redis

redis:
	$(COMPOSE) up -d --wait redis

lint:
	$(GO) vet ./...

run:
	$(GO) run ./cmd/$(CMD)

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down
