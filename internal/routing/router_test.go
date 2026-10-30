package routing

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/eventlog/memory"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/fanout"
)

var (
	trip = domain.Topic{Tenant: "acme", Kind: domain.TopicTrip, ID: "42"}
	desk = domain.Topic{Tenant: "acme", Kind: domain.TopicSOS, ID: "blr-1"}
)

// fakeHub records what the router delivers.
type fakeHub struct {
	mu     sync.Mutex
	got    []domain.Event
	closed []error
}

func (h *fakeHub) Publish(ev domain.Event) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.got = append(h.got, ev)
	return 1
}

func (h *fakeHub) CloseTopic(_ domain.Topic, cause error) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = append(h.closed, cause)
	return 1
}

// seqs returns the Seq of every reliable event delivered on topic.
func (h *fakeHub) seqs(topic domain.Topic) []uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []uint64
	for _, ev := range h.got {
		if ev.Topic == topic && ev.Kind.Reliable() {
			out = append(out, ev.Seq)
		}
	}
	return out
}

func (h *fakeHub) locations() []domain.Event {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []domain.Event
	for _, ev := range h.got {
		if !ev.Kind.Reliable() {
			out = append(out, ev)
		}
	}
	return out
}

func (h *fakeHub) closes() []error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.closed)
}

// fakeBus records subscriptions. subErr makes Subscribe fail.
type fakeBus struct {
	mu     sync.Mutex
	subs   map[domain.Topic]bool
	unsubs int
	subErr error
}

func newFakeBus() *fakeBus { return &fakeBus{subs: map[domain.Topic]bool{}} }

func (b *fakeBus) Subscribe(_ context.Context, t domain.Topic) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subs[t] = true
	return b.subErr
}

func (b *fakeBus) Unsubscribe(_ context.Context, t domain.Topic) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.subs, t)
	b.unsubs++
	return nil
}

func (b *fakeBus) has(t domain.Topic) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.subs[t]
}

// brokenLog fails the calls named in fail; the rest go to the memory log.
type brokenLog struct {
	*memory.Log
	mu   sync.Mutex
	fail map[string]bool
}

var errLogDown = errors.New("log down")

func (l *brokenLog) broken(call string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.fail[call]
}

func (l *brokenLog) set(call string, broken bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.fail[call] = broken
}

func (l *brokenLog) After(ctx context.Context, t domain.Topic, seq uint64) ([]domain.Event, error) {
	if l.broken("After") {
		return nil, errLogDown
	}
	return l.Log.After(ctx, t, seq)
}

func (l *brokenLog) LastSeq(ctx context.Context, t domain.Topic) (uint64, error) {
	if l.broken("LastSeq") {
		return 0, errLogDown
	}
	return l.Log.LastSeq(ctx, t)
}

type env struct {
	router    *Router
	hub       *fakeHub
	log       *brokenLog
	main, sos *fakeBus
	n         int
}

func newEnv(t *testing.T, cfg Config) *env {
	t.Helper()
	e := &env{
		hub:  &fakeHub{},
		log:  &brokenLog{Log: memory.New(memory.Config{MaxLen: 100}), fail: map[string]bool{}},
		main: newFakeBus(),
		sos:  newFakeBus(),
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	e.router = New(e.hub, e.log, e.main, e.sos, logger, cfg)
	return e
}

// publish stores an event in the log, as the publisher would, and returns
// it with its Seq or Ver set. It is not delivered to the router.
func (e *env) publish(t *testing.T, topic domain.Topic, kind domain.EventKind, cab string) domain.Event {
	t.Helper()
	e.n++
	ev := domain.Event{
		BaseEvent: domain.BaseEvent{ID: fmt.Sprintf("e-%d", e.n), Tenant: topic.Tenant, CabID: cab},
		Topic:     topic,
		Kind:      kind,
	}
	pos, _, err := e.log.Publish(context.Background(), ev)
	if err != nil {
		t.Fatal(err)
	}
	if kind.Reliable() {
		ev.Seq = pos
	} else {
		ev.Ver = pos
	}
	return ev
}

func (e *env) subscribe(t *testing.T, topic domain.Topic) {
	t.Helper()
	if err := e.router.OnFirstSubscriber(topic); err != nil {
		t.Fatal(err)
	}
}

func wantSeqs(t *testing.T, got []uint64, want ...uint64) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("delivered seqs %v, want %v", got, want)
	}
}

func TestForwardsInOrderAndDropsDuplicates(t *testing.T) {
	e := newEnv(t, Config{})
	e.subscribe(t, trip)

	s1 := e.publish(t, trip, domain.KindStatus, "cab-1")
	s2 := e.publish(t, trip, domain.KindStatus, "cab-1")
	loc := e.publish(t, trip, domain.KindLocation, "cab-1")
	e.router.Deliver(s1)
	e.router.Deliver(s1) // broker redelivery
	e.router.Deliver(s2)
	e.router.Deliver(loc)

	wantSeqs(t, e.hub.seqs(trip), 1, 2)
	if locs := e.hub.locations(); len(locs) != 1 {
		t.Fatalf("got %d locations, want 1", len(locs))
	}
}

func TestStartsAfterSeqAtSubscribe(t *testing.T) {
	e := newEnv(t, Config{})
	old := e.publish(t, trip, domain.KindStatus, "cab-1")
	e.subscribe(t, trip)

	// A late copy of an event the new subscriber reads in its catch-up is
	// not forwarded, and the next one is not seen as a gap.
	e.router.Deliver(old)
	e.router.Deliver(e.publish(t, trip, domain.KindStatus, "cab-1"))

	wantSeqs(t, e.hub.seqs(trip), 2)
	if len(e.hub.closes()) != 0 {
		t.Fatal("subscribers were closed without a gap")
	}
}

func TestGapIsRepairedFromLog(t *testing.T) {
	e := newEnv(t, Config{})
	e.subscribe(t, trip)

	s1 := e.publish(t, trip, domain.KindStatus, "cab-1")
	s2 := e.publish(t, trip, domain.KindStatus, "cab-1")
	s3 := e.publish(t, trip, domain.KindSOS, "cab-1")
	s4 := e.publish(t, trip, domain.KindStatus, "cab-1")

	e.router.Deliver(s1)
	e.router.Deliver(s4) // 2 and 3 were lost on the way
	e.router.Deliver(s2) // arrives late: already repaired
	e.router.Deliver(s3)

	wantSeqs(t, e.hub.seqs(trip), 1, 2, 3, 4)
}

func TestResyncAfterReconnect(t *testing.T) {
	e := newEnv(t, Config{})
	e.subscribe(t, trip)
	e.router.Deliver(e.publish(t, trip, domain.KindStatus, "cab-1"))

	// The bus was down while these were published.
	for range 3 {
		e.publish(t, trip, domain.KindStatus, "cab-1")
	}
	e.publish(t, trip, domain.KindLocation, "cab-1")
	newest := e.publish(t, trip, domain.KindLocation, "cab-1")

	e.router.Resync(trip)

	wantSeqs(t, e.hub.seqs(trip), 1, 2, 3, 4)
	locs := e.hub.locations()
	if len(locs) != 1 || locs[0].Ver != newest.Ver {
		t.Fatalf("locations after resync = %+v, want only ver %d", locs, newest.Ver)
	}
	e.router.Resync(trip) // nothing new: nothing repeated
	wantSeqs(t, e.hub.seqs(trip), 1, 2, 3, 4)
}

func TestUnrepairableGapClosesSubscribers(t *testing.T) {
	tests := []struct {
		name  string
		cfg   Config
		setup func(e *env)
	}{
		{"log down", Config{}, func(e *env) { e.log.set("After", true) }},
		{"gap too long", Config{MaxRepair: 2}, func(*env) {}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, tc.cfg)
			e.subscribe(t, trip)
			e.router.Deliver(e.publish(t, trip, domain.KindStatus, "cab-1"))
			for range 3 {
				e.publish(t, trip, domain.KindStatus, "cab-1")
			}
			tc.setup(e)

			e.router.Deliver(e.publish(t, trip, domain.KindStatus, "cab-1")) // seq 5

			closes := e.hub.closes()
			if len(closes) != 1 || !errors.Is(closes[0], fanout.ErrResync) {
				t.Fatalf("closes = %v, want one ErrResync", closes)
			}
			// The router carries on from the event that showed the gap.
			e.router.Deliver(e.publish(t, trip, domain.KindStatus, "cab-1"))
			wantSeqs(t, e.hub.seqs(trip), 1, 5, 6)
			if len(e.hub.closes()) != 1 {
				t.Fatal("subscribers closed again without a new gap")
			}
		})
	}
}

func TestTrimmedGapClosesSubscribers(t *testing.T) {
	e := newEnv(t, Config{MaxRepair: 1000})
	e.subscribe(t, trip)
	for range 150 { // the log keeps 100
		e.publish(t, trip, domain.KindStatus, "cab-1")
	}
	e.router.Resync(trip)

	if closes := e.hub.closes(); len(closes) != 1 || !errors.Is(closes[0], fanout.ErrResync) {
		t.Fatalf("closes = %v, want one ErrResync", closes)
	}
	// Everyone was closed and reads the log on resume, so the next event
	// starts afresh rather than closing the new subscribers again.
	e.router.Deliver(e.publish(t, trip, domain.KindStatus, "cab-1"))
	wantSeqs(t, e.hub.seqs(trip), 151)
	if len(e.hub.closes()) != 1 {
		t.Fatal("subscribers closed twice for one outage")
	}
}

func TestSOSTopicsUseTheirOwnBus(t *testing.T) {
	e := newEnv(t, Config{})
	e.subscribe(t, trip)
	e.subscribe(t, desk)

	if !e.main.has(trip) || e.main.has(desk) {
		t.Fatal("trip topic must be on the main bus only")
	}
	if !e.sos.has(desk) || e.sos.has(trip) {
		t.Fatal("sos topic must be on the sos bus only")
	}

	e.router.OnLastUnsubscribe(desk)
	if e.sos.has(desk) {
		t.Fatal("sos topic still subscribed after the last subscriber left")
	}
	e.router.Deliver(e.publish(t, desk, domain.KindSOS, "cab-1"))
	if got := e.hub.seqs(desk); len(got) != 0 {
		t.Fatalf("event of an unsubscribed topic delivered: %v", got)
	}
}

func TestFailedSubscribeLeavesNothingBehind(t *testing.T) {
	t.Run("bus", func(t *testing.T) {
		e := newEnv(t, Config{})
		e.main.subErr = errors.New("not confirmed")
		if err := e.router.OnFirstSubscriber(trip); err == nil {
			t.Fatal("OnFirstSubscriber succeeded with a failing bus")
		}
		if e.main.has(trip) {
			t.Fatal("topic left subscribed in the broker")
		}
		e.router.Deliver(e.publish(t, trip, domain.KindStatus, "cab-1"))
		if len(e.hub.seqs(trip)) != 0 {
			t.Fatal("event delivered for a topic whose subscribe failed")
		}
	})
	t.Run("log", func(t *testing.T) {
		e := newEnv(t, Config{})
		e.log.set("LastSeq", true)
		if err := e.router.OnFirstSubscriber(trip); !errors.Is(err, errLogDown) {
			t.Fatalf("OnFirstSubscriber = %v, want the log error", err)
		}
		if e.main.has(trip) {
			t.Fatal("topic left subscribed in the broker")
		}
	})
}

// Both bus loops deliver while subscribers come and go; -race checks the
// router's locking and each topic still arrives in order.
func TestConcurrentLanes(t *testing.T) {
	e := newEnv(t, Config{MaxRepair: 1000})
	e.subscribe(t, trip)
	e.subscribe(t, desk)

	const n = 100 // all still in the log, so every gap is repairable
	var tripEvs, deskEvs []domain.Event
	for range n {
		tripEvs = append(tripEvs, e.publish(t, trip, domain.KindStatus, "cab-1"))
		deskEvs = append(deskEvs, e.publish(t, desk, domain.KindSOS, "cab-1"))
	}

	var wg sync.WaitGroup
	for _, evs := range [][]domain.Event{tripEvs, deskEvs} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i, ev := range evs {
				if i%10 == 3 {
					continue // lost on the way
				}
				e.router.Deliver(ev)
			}
		}()
	}
	other := domain.Topic{Tenant: "acme", Kind: domain.TopicTrip, ID: "churn"}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 50 {
			_ = e.router.OnFirstSubscriber(other)
			e.router.OnLastUnsubscribe(other)
		}
	}()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("deliveries did not finish")
	}

	for _, topic := range []domain.Topic{trip, desk} {
		got := e.hub.seqs(topic)
		if len(got) != n {
			t.Fatalf("%s: got %d events, want %d", topic, len(got), n)
		}
		for i, seq := range got {
			if seq != uint64(i+1) {
				t.Fatalf("%s: event %d has seq %d", topic, i, seq)
			}
		}
	}
}

type recMetrics struct {
	mu      sync.Mutex
	repairs []bool
	resyncs int
}

func (m *recMetrics) GapRepaired(ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.repairs = append(m.repairs, ok)
}

func (m *recMetrics) Resynced() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.resyncs++
}

func TestRepairMetrics(t *testing.T) {
	m := &recMetrics{}
	e := newEnv(t, Config{Metrics: m})
	e.subscribe(t, trip)

	s1 := e.publish(t, trip, domain.KindStatus, "cab-1")
	e.publish(t, trip, domain.KindStatus, "cab-1")
	e.router.Deliver(s1)
	e.router.Deliver(e.publish(t, trip, domain.KindStatus, "cab-1")) // gap at 2: repaired
	e.router.Resync(trip)                                            // nothing missing: still a repair
	e.log.set("After", true)
	e.publish(t, trip, domain.KindStatus, "cab-1")
	e.router.Deliver(e.publish(t, trip, domain.KindStatus, "cab-1")) // gap at 4: log down

	if fmt.Sprint(m.repairs) != "[true true false]" || m.resyncs != 1 {
		t.Fatalf("repairs = %v, resyncs = %d; want [true true false], 1", m.repairs, m.resyncs)
	}
}
