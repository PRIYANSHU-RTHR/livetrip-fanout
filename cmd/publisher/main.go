// Command publisher is the cab-facing HTTP API. It writes events to the
// Redis event log, which sequences them and broadcasts them to gateways.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/auth"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/eventlog/redis"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/handler"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/obs"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/service"
)

type config struct {
	addr        string
	redisAddr   string
	poolSize    int
	sosPoolSize int
	maxInFlight int
	jwtSecret   string
	devTokens   bool
	logLevel    string
}

func loadConfig() config {
	return config{
		addr:        env("ADDR", ":8081"),
		redisAddr:   env("REDIS_ADDR", "localhost:6379"),
		poolSize:    envInt("REDIS_POOL_SIZE", 64),
		sosPoolSize: envInt("SOS_REDIS_POOL_SIZE", 8),
		maxInFlight: envInt("MAX_IN_FLIGHT", service.DefaultMaxInFlight),
		jwtSecret:   os.Getenv("JWT_SECRET"),
		devTokens:   os.Getenv("DEV_TOKENS") == "true",
		logLevel:    env("LOG_LEVEL", "info"),
	}
}

func main() {
	cfg := loadConfig()
	logger := obs.NewLogger(os.Stdout, cfg.logLevel)
	if err := run(logger, cfg); err != nil {
		logger.Error("publisher stopped", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger, cfg config) error {
	// Two pools on the same Redis: a location flood can use up every
	// connection in the first one without delaying an SOS.
	events := goredis.NewClient(&goredis.Options{Addr: cfg.redisAddr, PoolSize: cfg.poolSize})
	defer events.Close()
	sos := goredis.NewClient(&goredis.Options{
		Addr:         cfg.redisAddr,
		PoolSize:     cfg.sosPoolSize,
		MinIdleConns: cfg.sosPoolSize / 2, // kept open so an SOS never waits on a dial
	})
	defer sos.Close()
	metrics := obs.NewMetrics()
	events.AddHook(metrics.RedisHook("events"))
	sos.AddHook(metrics.RedisHook("sos"))

	pub := service.NewPublisher(
		redis.New(events, redis.Config{}),
		redis.New(sos, redis.Config{}),
		service.PublisherConfig{MaxInFlight: cfg.maxInFlight, Metrics: metrics},
	)

	mux := http.NewServeMux()
	handler.NewPublishHandler(pub, logger).Register(mux)
	if cfg.devTokens {
		// Anyone who can reach this route can mint any token: dev only.
		tokens, err := auth.NewTokenService([]byte(cfg.jwtSecret), auth.TokenConfig{})
		if err != nil {
			return fmt.Errorf("DEV_TOKENS needs JWT_SECRET: %w", err)
		}
		handler.NewDevTokenHandler(tokens).Register(mux)
		logger.Warn("dev token endpoint enabled", "route", "POST /v1/dev/token")
	}
	mux.Handle("GET /healthz", handler.Health(logger, map[string]handler.HealthCheck{
		"redis":     func(ctx context.Context) error { return events.Ping(ctx).Err() },
		"redis-sos": func(ctx context.Context) error { return sos.Ping(ctx).Err() },
	}))
	mux.Handle("GET /metrics", metrics.Handler())

	srv := &http.Server{
		Addr:              cfg.addr,
		Handler:           handler.Recover(logger, metrics, mux),
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	logger.Info("publisher listening", "addr", cfg.addr)

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	logger.Info("publisher shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	n, err := strconv.Atoi(os.Getenv(key))
	if err != nil || n <= 0 {
		return def
	}
	return n
}
