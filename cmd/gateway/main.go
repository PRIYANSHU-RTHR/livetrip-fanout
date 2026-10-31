// Command gateway holds client websocket connections, authorizes each
// subscribe, catches the client up from the Redis event log and fans live
// events out to the subscribed connections. It subscribes in Redis only to
// topics its own clients watch.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/api"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/auth"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/eventlog/redis"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/fanout"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/handler"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/obs"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/routing"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/service"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/web"
)

type config struct {
	addr      string
	redisAddr string
	jwtSecret string
	devTokens bool
	maxConns  int
	logLevel  string
	nodeID    string
}

func loadConfig() config {
	return config{
		addr:      env("ADDR", ":8080"),
		redisAddr: env("REDIS_ADDR", "localhost:6379"),
		jwtSecret: os.Getenv("JWT_SECRET"),
		devTokens: os.Getenv("DEV_TOKENS") == "true",
		maxConns:  envInt("MAX_CONNS", handler.DefaultMaxConns),
		logLevel:  env("LOG_LEVEL", "info"),
		nodeID:    env("NODE_ID", hostname()),
	}
}

func main() {
	cfg := loadConfig()
	logger := obs.NewLogger(os.Stdout, cfg.logLevel)
	if err := run(logger, cfg); err != nil {
		logger.Error("gateway stopped", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger, cfg config) error {
	tokens, err := auth.NewTokenService([]byte(cfg.jwtSecret), auth.TokenConfig{})
	if err != nil {
		return fmt.Errorf("JWT_SECRET: %w", err)
	}
	metrics := obs.NewMetrics()
	rdb := goredis.NewClient(&goredis.Options{Addr: cfg.redisAddr})
	defer rdb.Close()
	rdb.AddHook(metrics.RedisHook("gateway"))

	log := redis.New(rdb, redis.Config{})

	// Two pub/sub connections: SOS topics get their own, so a backlog of
	// location traffic never sits in front of an SOS.
	mainBus, sosBus := redis.NewBus(rdb, logger), redis.NewBus(rdb, logger)
	defer mainBus.Close()
	defer sosBus.Close()

	// The hub's first/last subscriber hooks drive the router's Redis
	// subscriptions, and the router publishes into the hub.
	var router *routing.Router
	hub := fanout.NewHub(fanout.Hooks{
		OnFirstSubscriber: func(t domain.Topic) error { return router.OnFirstSubscriber(t) },
		OnLastUnsubscribe: func(t domain.Topic) { router.OnLastUnsubscribe(t) },
	})
	router = routing.New(hub, log, mainBus, sosBus, logger, routing.Config{Metrics: metrics})

	trips := auth.NewCachedTrips(demoTrips(), auth.CacheConfig{})
	subs := service.NewSubscriptions(auth.NewRoleAuthorizer(trips), hub, log,
		service.SubscriptionsConfig{Metrics: metrics})
	ws := handler.NewWSHandler(tokens, subs, logger,
		handler.WSConfig{MaxConns: cfg.maxConns, Metrics: metrics, ConnMetrics: metrics, NodeID: cfg.nodeID})
	metrics.Gauge("livetrip_ws_connections", "Open websocket connections on this node.",
		func() float64 { return float64(ws.Conns()) })
	metrics.Gauge("livetrip_topics_subscribed", "Topics with at least one subscriber on this node.",
		func() float64 { return float64(hub.Topics()) })
	for name, bus := range map[string]*redis.Bus{"main": mainBus, "sos": sosBus} {
		metrics.LabeledGauge("livetrip_bus_backlog", "Messages waiting in a Redis subscription channel for the router.",
			map[string]string{"bus": name}, func() float64 { return float64(bus.Backlog()) })
	}

	mux := http.NewServeMux()
	ws.Register(mux)
	if cfg.devTokens {
		// Anyone who can reach this route can mint any token: dev only.
		handler.NewDevTokenHandler(tokens).Register(mux)
		logger.Warn("dev token endpoint enabled", "route", "POST /v1/dev/token")
	}
	mux.Handle("GET /healthz", handler.Health(logger, map[string]handler.HealthCheck{
		"redis": func(ctx context.Context) error { return rdb.Ping(ctx).Err() },
	}))
	mux.Handle("GET /metrics", metrics.Handler())
	handler.RegisterAssets(mux, assets()...)

	// Websocket handlers outlive Server.Shutdown (it skips hijacked
	// connections), so their request contexts hang off root and are
	// cancelled explicitly on shutdown.
	root, cancelRoot := context.WithCancel(context.Background())
	defer cancelRoot()
	srv := &http.Server{
		Addr:              cfg.addr,
		Handler:           handler.Recover(logger, metrics, mux),
		ReadHeaderTimeout: 2 * time.Second,
		IdleTimeout:       60 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return root },
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errc := make(chan error, 3)
	go func() { errc <- srv.ListenAndServe() }()
	for name, bus := range map[string]*redis.Bus{"main": mainBus, "sos": sosBus} {
		// Without its bus the node would hold clients that get no events.
		go func() { errc <- fmt.Errorf("%s bus: %w", name, bus.Run(root, router)) }()
	}
	logger.Info("gateway listening", "addr", cfg.addr, "node", cfg.nodeID)

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	logger.Info("gateway shutting down", "conns", ws.Conns())
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	// Clients get 1001 and reconnect through the load balancer elsewhere.
	cancelRoot()
	return ws.Wait(shutdownCtx)
}

// assets are the pages served next to /ws: the demo, the API reference and
// the OpenAPI file it renders.
func assets() []handler.Asset {
	return []handler.Asset{
		{Path: "/demo", ContentType: "text/html; charset=utf-8", Body: web.Demo},
		{Path: "/docs", ContentType: "text/html; charset=utf-8", Body: web.Docs},
		{Path: "/openapi.yaml", ContentType: "application/yaml", Body: api.OpenAPI},
	}
}

// demoTrips stands in for the trip service until one exists.
func demoTrips() *auth.StaticTrips {
	trips := auth.NewStaticTrips()
	trips.Put("acme", "42", auth.Trip{EmployeeID: "alice", OfficeID: "blr-1"})
	trips.Put("acme", "43", auth.Trip{EmployeeID: "bob", OfficeID: "blr-1"})
	trips.Put("acme", "44", auth.Trip{EmployeeID: "carol", OfficeID: "blr-2"})
	trips.Put("globex", "42", auth.Trip{EmployeeID: "alice", OfficeID: "hyd-1"})
	return trips
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return h
}

func envInt(key string, def int) int {
	n, err := strconv.Atoi(os.Getenv(key))
	if err != nil || n <= 0 {
		return def
	}
	return n
}
