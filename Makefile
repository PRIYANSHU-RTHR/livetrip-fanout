GO ?= go
COMPOSE ?= docker compose -f deploy/docker-compose.yml
CMD ?= gateway
REDIS_ADDR ?= localhost:6379
# e.g. make loadtest ARGS="-kill gateway-2"
ARGS ?=

.PHONY: build test bench test-redis redis lint run up down loadtest

build:
	$(GO) build ./...

test:
	$(GO) test -race ./...

# Benchmarks, then the measured memory per websocket connection.
bench:
	$(GO) test -run '^$$' -bench . ./internal/fanout ./internal/eventlog/memory
	$(GO) test -run TestMemoryPerConnection -count=1 -v ./internal/handler

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

# Needs the stack from `make up`.
loadtest:
	$(GO) run ./cmd/loadtest $(ARGS)
