package obs

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	goredis "github.com/redis/go-redis/v9"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"
)

// latencyBuckets span 1ms to ~4s: SOS p99 should sit in the first few.
var latencyBuckets = prometheus.ExponentialBuckets(0.001, 2, 13)

// Metrics is the Prometheus side of every metrics interface in the core
// packages (fanout.Metrics, service.SubscribeMetrics,
// service.PublishMetrics, routing.Metrics, handler.Metrics). Each binary
// creates one and serves it on GET /metrics; series a binary never touches
// stay empty.
//
// Labels are fixed small sets (kind, outcome, reason). Topic, tenant and
// user IDs are never labels: 10,000 clients would mean 10,000 series.
type Metrics struct {
	reg *prometheus.Registry

	e2eLatency     *prometheus.HistogramVec
	queueDepth     prometheus.Histogram
	conflated      prometheus.Counter
	wsCloses       *prometheus.CounterVec
	subscribes     *prometheus.CounterVec
	resumes        *prometheus.CounterVec
	publishes      *prometheus.CounterVec
	publishLatency *prometheus.HistogramVec
	sosRetries     prometheus.Counter
	gapRepairs     *prometheus.CounterVec
	resyncs        prometheus.Counter
	redisErrors    *prometheus.CounterVec
	panics         *prometheus.CounterVec
}

func NewMetrics() *Metrics {
	m := &Metrics{
		reg: prometheus.NewRegistry(),
		e2eLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "livetrip_delivery_latency_seconds",
			Help:    "Time from event createdAt to the frame being written to a client, live events only.",
			Buckets: latencyBuckets,
		}, []string{"kind"}),
		queueDepth: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "livetrip_reliable_queue_depth",
			Help:    "Depth of a connection's reliable lane after an enqueue; 64 means it is about to be closed.",
			Buckets: []float64{0, 1, 2, 4, 8, 16, 32, 48, 63},
		}),
		conflated: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "livetrip_locations_conflated_total",
			Help: "Location updates replaced by a newer one before the client was sent them.",
		}),
		wsCloses: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "livetrip_ws_closes_total",
			Help: "Websocket connections ended, by reason (slow_consumer, forbidden, resync, ...).",
		}, []string{"reason"}),
		subscribes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "livetrip_subscribes_total",
			Help: "Subscribe requests by outcome (ok, forbidden, lookup_failed, unavailable, closed).",
		}, []string{"outcome"}),
		resumes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "livetrip_resumes_total",
			Help: "Successful subscribes by catch-up mode (replay, snapshot, snapshot_trimmed).",
		}, []string{"mode"}),
		publishes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "livetrip_publishes_total",
			Help: "Cab events received, by kind and outcome (ok, duplicate, invalid, busy, unavailable).",
		}, []string{"kind", "outcome"}),
		publishLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "livetrip_publish_duration_seconds",
			Help:    "Time to store one cab event in the event log, retries included.",
			Buckets: latencyBuckets,
		}, []string{"kind"}),
		sosRetries: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "livetrip_sos_retries_total",
			Help: "SOS publishes retried after an event log error.",
		}),
		gapRepairs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "livetrip_gap_repairs_total",
			Help: "Seq gaps from the broker, by result (ok: replayed from the log, failed: subscribers closed).",
		}, []string{"result"}),
		resyncs: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "livetrip_bus_resyncs_total",
			Help: "Topics resynced from the log after the broker connection came back.",
		}),
		redisErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "livetrip_redis_errors_total",
			Help: "Failed Redis commands by client pool and command.",
		}, []string{"pool", "cmd"}),
		panics: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "livetrip_panics_total",
			Help: "Panics recovered, by where they happened.",
		}, []string{"where"}),
	}
	m.reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.e2eLatency, m.queueDepth, m.conflated, m.wsCloses, m.subscribes, m.resumes,
		m.publishes, m.publishLatency, m.sosRetries, m.gapRepairs, m.resyncs,
		m.redisErrors, m.panics,
	)
	return m
}

// Handler serves the metrics in the Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// Gauge registers a gauge that calls f at scrape time, for values the
// program already tracks (open connections, topics).
func (m *Metrics) Gauge(name, help string, f func() float64) {
	m.reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: name, Help: help}, f))
}

// fanout.Metrics

func (m *Metrics) Delivered(ev domain.Event) {
	if ev.CreatedAt.IsZero() {
		return
	}
	// createdAt may come from the cab's clock; a skewed clock must not
	// give a negative latency.
	m.e2eLatency.WithLabelValues(string(ev.Kind)).Observe(max(time.Since(ev.CreatedAt), 0).Seconds())
}

func (m *Metrics) ReliableQueued(depth int) { m.queueDepth.Observe(float64(depth)) }
func (m *Metrics) LocationConflated()       { m.conflated.Inc() }

// service.SubscribeMetrics and service.PublishMetrics

func (m *Metrics) Subscribed(outcome string) { m.subscribes.WithLabelValues(outcome).Inc() }
func (m *Metrics) Resumed(mode string)       { m.resumes.WithLabelValues(mode).Inc() }

func (m *Metrics) Published(kind domain.EventKind, outcome string, took time.Duration) {
	m.publishes.WithLabelValues(string(kind), outcome).Inc()
	m.publishLatency.WithLabelValues(string(kind)).Observe(took.Seconds())
}

func (m *Metrics) SOSRetried() { m.sosRetries.Inc() }

// routing.Metrics

func (m *Metrics) GapRepaired(ok bool) {
	result := "ok"
	if !ok {
		result = "failed"
	}
	m.gapRepairs.WithLabelValues(result).Inc()
}

func (m *Metrics) Resynced() { m.resyncs.Inc() }

// handler.Metrics

func (m *Metrics) WSClosed(reason string) { m.wsCloses.WithLabelValues(reason).Inc() }
func (m *Metrics) Panicked(where string)  { m.panics.WithLabelValues(where).Inc() }

// RedisHook returns a go-redis hook that counts failed commands under
// pool. Add it with client.AddHook.
func (m *Metrics) RedisHook(pool string) goredis.Hook {
	return redisHook{m: m, pool: pool}
}

type redisHook struct {
	m    *Metrics
	pool string
}

func (h redisHook) count(cmd string, err error) {
	if !isRedisFailure(err) {
		return
	}
	h.m.redisErrors.WithLabelValues(h.pool, cmd).Inc()
}

func (h redisHook) DialHook(next goredis.DialHook) goredis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		c, err := next(ctx, network, addr)
		h.count("dial", err)
		return c, err
	}
}

func (h redisHook) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		err := next(ctx, cmd)
		// CLIENT is only sent by go-redis's connection handshake, which
		// ignores its errors (older or managed Redis rejects some of it).
		if cmd.Name() != "client" {
			h.count(cmd.Name(), err)
		}
		return err
	}
}

func (h redisHook) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []goredis.Cmder) error {
		err := next(ctx, cmds)
		h.count("pipeline", err)
		return err
	}
}

// isRedisFailure reports whether err means Redis failed. A missing key
// (redis.Nil) is an answer, and NOSCRIPT is the expected first reply to
// EVALSHA before go-redis loads the script.
func isRedisFailure(err error) bool {
	return err != nil && !errors.Is(err, goredis.Nil) && !strings.HasPrefix(err.Error(), "NOSCRIPT")
}
