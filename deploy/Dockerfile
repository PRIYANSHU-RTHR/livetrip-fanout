# One Dockerfile for every binary: CMD picks which one (gateway, publisher, loadtest).
# Build context is the repo root.

FROM golang:1.27-alpine AS build
WORKDIR /src
# Dependencies first so code changes reuse the cached module layer.
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG CMD=gateway
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${CMD}

FROM alpine:3.20
# busybox wget is used by the compose healthchecks.
RUN adduser -D -H -u 10001 app
USER app
COPY --from=build /out/app /app
ENTRYPOINT ["/app"]
