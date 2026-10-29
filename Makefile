GO ?= go
COMPOSE ?= docker compose -f deploy/docker-compose.yml
CMD ?= gateway

.PHONY: build test lint run up down

build:
	$(GO) build ./...

test:
	$(GO) test -race ./...

lint:
	$(GO) vet ./...

run:
	$(GO) run ./cmd/$(CMD)

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down
