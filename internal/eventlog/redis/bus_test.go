package redis

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/eventlog/logtest"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/routing"
)

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// recordingHub is a routing.Hub that keeps reliable seqs per topic.
type recordingHub struct {
	mu     sync.Mutex
	seqs   map[domain.Topic][]uint64
	closed int
}

func newRecordingHub() *recordingHub { return &recordingHub{seqs: map[domain.Topic][]uint64{}} }

func (h *recordingHub) Publish(ev domain.Event) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	if ev.Kind.Reliable() {
		h.seqs[ev.Topic] = append(h.seqs[ev.Topic], ev.Seq)
	}
	return 1
}

func (h *recordingHub) CloseTopic(domain.Topic, error) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed++
	return 1
}

func (h *recordingHub) got(topic domain.Topic) []uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]uint64(nil), h.seqs[topic]...)
}

// countingHandler counts Resync calls on the way to the router.
type countingHandler struct {
	Handler
	resyncs atomic.Int32
}

func (h *countingHandler) Resync(t domain.Topic) {
	h.resyncs.Add(1)
	h.Handler.Resync(t)
}

func startBus(t *testing.T, bus *Bus, h Handler) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = bus.Run(ctx, h) }()
	t.Cleanup(func() { cancel(); bus.Close(); <-done })
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestBusDeliversAfterConfirmedSubscribe(t *testing.T) {
	ctx := context.Background()
	c := newClient(t)
	log := New(c, Config{})
	hub := newRecordingHub()
	main, sos := NewBus(c, discardLogger()), NewBus(c, discardLogger())
	router := routing.New(hub, log, main, sos, discardLogger(), routing.Config{})
	startBus(t, main, router)
	startBus(t, sos, router)

	trip := logtest.NewTopic(t)
	desk := domain.Topic{Tenant: "acme", Kind: domain.TopicSOS, ID: trip.ID}
	for _, topic := range []domain.Topic{trip, desk} {
		if err := router.OnFirstSubscriber(topic); err != nil {
			t.Fatal(err)
		}
	}

	// Subscribe returned only after Redis confirmed, so publishing right
	// away must not be lost.
	if _, _, err := log.Publish(ctx, logtest.NewEvent(trip, "s-1", "cab-1", domain.KindStatus)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := log.Publish(ctx, logtest.NewEvent(desk, "sos-1", "cab-1", domain.KindSOS)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "both events", func() bool { return len(hub.got(trip)) == 1 && len(hub.got(desk)) == 1 })

	router.OnLastUnsubscribe(trip)
	if _, _, err := log.Publish(ctx, logtest.NewEvent(trip, "s-2", "cab-1", domain.KindStatus)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if got := hub.got(trip); len(got) != 1 {
		t.Fatalf("got %v after unsubscribe, want only seq 1", got)
	}
}

// blockingHandler holds Run inside Deliver until release is closed.
type blockingHandler struct {
	Handler
	release chan struct{}
}

func (h blockingHandler) Deliver(ev domain.Event) {
	<-h.release
	h.Handler.Deliver(ev)
}

// While the router is stuck, messages pile up in the subscription channel
// and Backlog counts them; once it catches up the backlog is empty.
func TestBusBacklogCountsWaitingMessages(t *testing.T) {
	ctx := context.Background()
	c := newClient(t)
	log := New(c, Config{})
	hub := newRecordingHub()
	main, sos := NewBus(c, discardLogger()), NewBus(c, discardLogger())
	if n := main.Backlog(); n != 0 {
		t.Fatalf("backlog before Run = %d, want 0", n)
	}
	router := routing.New(hub, log, main, sos, discardLogger(), routing.Config{})
	release := make(chan struct{})
	startBus(t, main, blockingHandler{Handler: router, release: release})
	startBus(t, sos, router)

	topic := logtest.NewTopic(t)
	if err := router.OnFirstSubscriber(topic); err != nil {
		t.Fatal(err)
	}
	const total = 10
	for i := 1; i <= total; i++ {
		if _, _, err := log.Publish(ctx, logtest.NewEvent(topic, fmt.Sprintf("s-%d", i), "cab-1", domain.KindStatus)); err != nil {
			t.Fatal(err)
		}
	}
	// Run holds the first message; the rest wait in the channel.
	waitFor(t, "backlog", func() bool { return main.Backlog() == total-1 })
	close(release)
	waitFor(t, "drain", func() bool { return main.Backlog() == 0 && len(hub.got(topic)) == total })
}

// Failure injection: Redis drops the gateway's pub/sub connections while
// statuses are being published. Every status must still reach the hub
// exactly once and in order.
func TestBusReconnectLosesNothing(t *testing.T) {
	ctx := context.Background()
	c := newClient(t)
	log := New(c, Config{})
	hub := newRecordingHub()
	main, sos := NewBus(c, discardLogger()), NewBus(c, discardLogger())
	router := routing.New(hub, log, main, sos, discardLogger(), routing.Config{MaxRepair: 1000})
	h := &countingHandler{Handler: router}
	startBus(t, main, h)
	startBus(t, sos, h)

	topic := logtest.NewTopic(t)
	if err := router.OnFirstSubscriber(topic); err != nil {
		t.Fatal(err)
	}

	const total = 300
	for i := 1; i <= total; i++ {
		ev := logtest.NewEvent(topic, fmt.Sprintf("s-%d", i), "cab-1", domain.KindStatus)
		if _, _, err := log.Publish(ctx, ev); err != nil {
			t.Fatal(err)
		}
		if i == total/3 {
			if err := c.ClientKillByFilter(ctx, "TYPE", "pubsub").Err(); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(time.Millisecond)
	}

	waitFor(t, "all statuses", func() bool { return len(hub.got(topic)) >= total })
	got := hub.got(topic)
	if len(got) != total {
		t.Fatalf("got %d events, want %d", len(got), total)
	}
	for i, seq := range got {
		if seq != uint64(i+1) {
			t.Fatalf("event %d has seq %d: order broken or duplicate", i, seq)
		}
	}
	if h.resyncs.Load() == 0 {
		t.Fatal("no resync after the connection was killed")
	}
	if hub.closed != 0 {
		t.Fatalf("subscribers were closed %d times; the gap should have been repaired", hub.closed)
	}
}
