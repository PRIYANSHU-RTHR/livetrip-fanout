package obs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	goredis "github.com/redis/go-redis/v9"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"
)

func TestLoggerRedactsPersonalData(t *testing.T) {
	var buf bytes.Buffer
	log := NewLogger(&buf, "info")
	log.Info("trip update",
		"phone", "+91-9876543210",
		"Email", "a@example.com",
		"tripId", "42",
		"cab", map[string]any{"id": "cab-7"},
	)
	log.WithGroup("rider").Info("nested", "lat", 12.97, "lng", 77.59)

	out := buf.String()
	for _, leak := range []string{"9876543210", "a@example.com", "12.97", "77.59"} {
		if strings.Contains(out, leak) {
			t.Fatalf("log leaked %q:\n%s", leak, out)
		}
	}
	var first map[string]any
	if err := json.Unmarshal([]byte(strings.SplitN(out, "\n", 2)[0]), &first); err != nil {
		t.Fatal(err)
	}
	if first["phone"] != Redacted || first["tripId"] != "42" {
		t.Fatalf("first line = %v", first)
	}
}

func TestLoggerLevel(t *testing.T) {
	for level, wantDebug := range map[string]bool{"debug": true, "info": false, "": false, "nonsense": false} {
		var buf bytes.Buffer
		NewLogger(&buf, level).Debug("hidden?")
		if got := buf.Len() > 0; got != wantDebug {
			t.Errorf("level %q: debug written = %v, want %v", level, got, wantDebug)
		}
	}
}

func TestDeliveredLatencyByKind(t *testing.T) {
	m := NewMetrics()
	m.Delivered(domain.Event{Kind: domain.KindSOS, BaseEvent: domain.BaseEvent{CreatedAt: time.Now().Add(-20 * time.Millisecond)}})
	// A cab clock ahead of ours must not record a negative latency.
	m.Delivered(domain.Event{Kind: domain.KindSOS, BaseEvent: domain.BaseEvent{CreatedAt: time.Now().Add(time.Hour)}})
	m.Delivered(domain.Event{Kind: domain.KindStatus}) // no createdAt: skipped

	if n := testutil.CollectAndCount(m.e2eLatency); n != 1 {
		t.Fatalf("latency series = %d, want 1 (sos only)", n)
	}
	body := scrape(t, m)
	if !strings.Contains(body, `livetrip_delivery_latency_seconds_count{kind="sos"} 2`) {
		t.Fatalf("missing sos count:\n%s", grep(body, "delivery_latency_seconds_count"))
	}
	if !strings.Contains(body, `livetrip_delivery_latency_seconds_bucket{kind="sos",le="0.001"} 1`) {
		t.Fatalf("skewed clock not clamped to 0:\n%s", grep(body, `le="0.001"`))
	}
}

func TestCountersAndGauges(t *testing.T) {
	m := NewMetrics()
	m.LocationConflated()
	m.LocationConflated()
	m.ReliableQueued(3)
	m.WSClosed("slow_consumer")
	m.Subscribed("forbidden")
	m.Resumed("snapshot_trimmed")
	m.Published(domain.KindStatus, "duplicate", time.Millisecond)
	m.SOSRetried()
	m.GapRepaired(false)
	m.Resynced()
	m.Panicked("http")
	m.Gauge("livetrip_ws_connections", "open", func() float64 { return 7 })

	body := scrape(t, m)
	for _, want := range []string{
		"livetrip_locations_conflated_total 2",
		"livetrip_reliable_queue_depth_count 1",
		`livetrip_ws_closes_total{reason="slow_consumer"} 1`,
		`livetrip_subscribes_total{outcome="forbidden"} 1`,
		`livetrip_resumes_total{mode="snapshot_trimmed"} 1`,
		`livetrip_publishes_total{kind="status",outcome="duplicate"} 1`,
		"livetrip_sos_retries_total 1",
		`livetrip_gap_repairs_total{result="failed"} 1`,
		"livetrip_bus_resyncs_total 1",
		`livetrip_panics_total{where="http"} 1`,
		"livetrip_ws_connections 7",
		"go_goroutines",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("scrape is missing %q", want)
		}
	}
}

func TestRedisHookCountsOnlyFailures(t *testing.T) {
	m := NewMetrics()
	hook := m.RedisHook("sos")
	run := func(name string, err error) {
		cmd := goredis.NewCmd(context.Background(), name)
		_ = hook.ProcessHook(func(context.Context, goredis.Cmder) error { return err })(context.Background(), cmd)
	}
	run("get", nil)
	run("get", goredis.Nil)
	run("evalsha", errors.New("NOSCRIPT No matching script"))
	run("evalsha", errors.New("i/o timeout"))
	run("xrange", context.DeadlineExceeded)
	run("client", errors.New("ERR unknown subcommand")) // handshake noise

	if got := testutil.ToFloat64(m.redisErrors.WithLabelValues("sos", "evalsha")); got != 1 {
		t.Errorf("evalsha errors = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.redisErrors.WithLabelValues("sos", "xrange")); got != 1 {
		t.Errorf("xrange errors = %v, want 1", got)
	}
	if n := testutil.CollectAndCount(m.redisErrors); n != 2 {
		t.Errorf("series = %d, want 2 (nil and NOSCRIPT are not failures)", n)
	}
}

func scrape(t *testing.T, m *Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	b, _ := io.ReadAll(rec.Body)
	return string(b)
}

func grep(body, sub string) string {
	var out []string
	for _, line := range strings.Split(body, "\n") {
		if strings.Contains(line, sub) {
			out = append(out, line)
		}
	}
	return strings.Join(out, "\n")
}
