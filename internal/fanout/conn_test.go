package fanout

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"
)

// fakeSink records writes. If gate is set, every Write first signals
// entered and then waits on gate.
type fakeSink struct {
	gate    chan struct{}
	entered chan struct{}
	writes  chan domain.Event

	mu       sync.Mutex
	closed   bool
	cause    error
	writeErr error
}

func newFakeSink() *fakeSink {
	return &fakeSink{writes: make(chan domain.Event, 4096)}
}

func (s *fakeSink) Write(ev domain.Event) error {
	if s.gate != nil {
		s.entered <- struct{}{}
		<-s.gate
	}
	s.mu.Lock()
	err := s.writeErr
	s.mu.Unlock()
	if err != nil {
		return err
	}
	s.writes <- ev
	return nil
}

func (s *fakeSink) Close(cause error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed, s.cause = true, cause
	return nil
}

func (s *fakeSink) closeCause() (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed, s.cause
}

// next waits for the next written event.
func (s *fakeSink) next(t *testing.T) domain.Event {
	t.Helper()
	select {
	case ev := <-s.writes:
		return ev
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a write")
		return domain.Event{}
	}
}

// expectNoMore fails if anything else is written within a short window.
func (s *fakeSink) expectNoMore(t *testing.T) {
	t.Helper()
	select {
	case ev := <-s.writes:
		t.Fatalf("unexpected extra write: %s %s ver=%d seq=%d", ev.Kind, ev.CabID, ev.Ver, ev.Seq)
	case <-time.After(50 * time.Millisecond):
	}
}

var trip = domain.Topic{Tenant: "acme", Kind: domain.TopicTrip, ID: "42"}

func loc(cab string, ver uint64) domain.Event {
	return domain.Event{
		BaseEvent: domain.BaseEvent{ID: fmt.Sprintf("%s-v%d", cab, ver), Tenant: "acme", CabID: cab},
		Topic:     trip, Kind: domain.KindLocation, Ver: ver,
	}
}

func reliable(kind domain.EventKind, seq uint64) domain.Event {
	return domain.Event{
		BaseEvent: domain.BaseEvent{ID: fmt.Sprintf("%s-%d", kind, seq), Tenant: "acme", CabID: "cab-1"},
		Topic:     trip, Kind: kind, Seq: seq,
	}
}

// start runs the write loop and returns a function that stops it and
// returns Run's result.
func start(t *testing.T, c *Conn) func() error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	res := make(chan error, 1)
	go func() { res <- c.Run(ctx) }()
	return func() error {
		cancel()
		select {
		case err := <-res:
			return err
		case <-time.After(2 * time.Second):
			t.Fatal("Run did not return")
			return nil
		}
	}
}

func mustEnqueue(t *testing.T, c *Conn, ev domain.Event) {
	t.Helper()
	if err := c.Enqueue(ev); err != nil {
		t.Fatalf("Enqueue(%s %s): %v", ev.Kind, ev.ID, err)
	}
}

func TestConflationKeepsNewestPerCab(t *testing.T) {
	sink := newFakeSink()
	c := NewConn(sink)

	for v := uint64(1); v <= 5; v++ {
		mustEnqueue(t, c, loc("cab-a", v))
	}
	for v := uint64(1); v <= 3; v++ {
		mustEnqueue(t, c, loc("cab-b", v))
	}
	mustEnqueue(t, c, loc("cab-a", 2)) // late, older than v5: ignored

	stop := start(t, c)
	defer stop()

	got := map[string]uint64{}
	for range 2 {
		ev := sink.next(t)
		got[ev.CabID] = ev.Ver
	}
	if got["cab-a"] != 5 || got["cab-b"] != 3 {
		t.Fatalf("got %v, want cab-a=5 cab-b=3", got)
	}
	sink.expectNoMore(t)

	// An older version after the newest was sent is still ignored.
	mustEnqueue(t, c, loc("cab-a", 4))
	sink.expectNoMore(t)
	mustEnqueue(t, c, loc("cab-a", 6))
	if ev := sink.next(t); ev.CabID != "cab-a" || ev.Ver != 6 {
		t.Fatalf("got %s ver=%d, want cab-a ver=6", ev.CabID, ev.Ver)
	}
}

// Ver is per topic: the same cab on two topics must not share one slot.
// Regression: keyed by cab alone, the office topic's ver 15 was dropped
// as older than the trip topic's ver 16.
func TestConflationIsPerTopicAndCab(t *testing.T) {
	sink := newFakeSink()
	c := NewConn(sink)
	stop := start(t, c)
	defer stop()

	office := domain.Topic{Tenant: "acme", Kind: domain.TopicOffice, ID: "blr-1"}
	onOffice := func(ev domain.Event) domain.Event { ev.Topic = office; return ev }

	mustEnqueue(t, c, loc("cab-7", 16))
	if ev := sink.next(t); ev.Topic != trip || ev.Ver != 16 {
		t.Fatalf("got %s ver=%d, want trip ver 16", ev.Topic, ev.Ver)
	}
	mustEnqueue(t, c, onOffice(loc("cab-7", 15)))
	if ev := sink.next(t); ev.Topic != office || ev.Ver != 15 {
		t.Fatalf("got %s ver=%d, want office ver 15", ev.Topic, ev.Ver)
	}
	mustEnqueue(t, c, loc("cab-7", 14)) // older than trip's 16: still dropped
	sink.expectNoMore(t)
}

func TestSOSWrittenBeforeQueuedLocations(t *testing.T) {
	sink := newFakeSink()
	c := NewConn(sink)

	for i := range 1000 {
		mustEnqueue(t, c, loc(fmt.Sprintf("cab-%d", i), 1))
	}
	mustEnqueue(t, c, reliable(domain.KindSOS, 1))

	stop := start(t, c)
	defer stop()

	if ev := sink.next(t); ev.Kind != domain.KindSOS {
		t.Fatalf("first write = %s, want sos", ev.Kind)
	}
	for range 1000 {
		if ev := sink.next(t); ev.Kind != domain.KindLocation {
			t.Fatalf("got %s, want location", ev.Kind)
		}
	}
	sink.expectNoMore(t)
}

func TestSOSPreemptsLocationsMidStream(t *testing.T) {
	sink := newFakeSink()
	sink.gate = make(chan struct{})
	sink.entered = make(chan struct{}, 4096)
	c := NewConn(sink)

	for i := range 1000 {
		mustEnqueue(t, c, loc(fmt.Sprintf("cab-%d", i), 1))
	}
	stop := start(t, c)
	defer stop()

	// Let a few locations through, then raise SOS while the writer is
	// blocked inside a location write.
	for range 3 {
		<-sink.entered
		sink.gate <- struct{}{}
		sink.next(t)
	}
	<-sink.entered
	mustEnqueue(t, c, reliable(domain.KindSOS, 1))
	close(sink.gate)

	if ev := sink.next(t); ev.Kind != domain.KindLocation {
		t.Fatalf("in-flight write = %s, want location", ev.Kind)
	}
	if ev := sink.next(t); ev.Kind != domain.KindSOS {
		t.Fatalf("write after SOS enqueue = %s, want sos", ev.Kind)
	}
}

func TestReliableInOrder(t *testing.T) {
	sink := newFakeSink()
	c := NewConn(sink)
	stop := start(t, c)
	defer stop()

	for seq := uint64(1); seq <= 500; seq++ {
		kind := domain.KindStatus
		if seq%50 == 0 {
			kind = domain.KindSOS
		}
		if c.Enqueue(reliable(kind, seq)) != nil {
			t.Fatalf("seq %d rejected", seq)
		}
		if seq%32 == 0 { // keep under the buffer so the test is not timing-dependent
			for want := seq - 31; want <= seq; want++ {
				if ev := sink.next(t); ev.Seq != want {
					t.Fatalf("seq = %d, want %d", ev.Seq, want)
				}
			}
		}
	}
}

func TestFullReliableLaneClosesNotDrops(t *testing.T) {
	sink := newFakeSink()
	c := NewConn(sink) // writer not started: the client is frozen

	for seq := uint64(1); seq <= ReliableBuffer; seq++ {
		mustEnqueue(t, c, reliable(domain.KindStatus, seq))
	}
	if c.Err() != nil {
		t.Fatalf("closed early: %v", c.Err())
	}

	err := c.Enqueue(reliable(domain.KindStatus, ReliableBuffer+1))
	if !errors.Is(err, ErrSlowConsumer) {
		t.Fatalf("overflow Enqueue = %v, want ErrSlowConsumer", err)
	}
	select {
	case <-c.Done():
	default:
		t.Fatal("conn not closed after overflow")
	}
	if !errors.Is(c.Err(), ErrSlowConsumer) {
		t.Fatalf("Err() = %v, want ErrSlowConsumer", c.Err())
	}
	if err := c.Enqueue(reliable(domain.KindStatus, 99)); !errors.Is(err, ErrClosed) {
		t.Fatalf("Enqueue after close = %v, want ErrClosed", err)
	}
	if err := c.Enqueue(loc("cab-a", 1)); !errors.Is(err, ErrClosed) {
		t.Fatalf("location Enqueue after close = %v, want ErrClosed", err)
	}

	// The writer sees the close and passes the cause to the sink.
	if err := c.Run(context.Background()); !errors.Is(err, ErrSlowConsumer) {
		t.Fatalf("Run = %v, want ErrSlowConsumer", err)
	}
	if closed, cause := sink.closeCause(); !closed || !errors.Is(cause, ErrSlowConsumer) {
		t.Fatalf("sink closed=%v cause=%v, want ErrSlowConsumer", closed, cause)
	}
}

func TestLocationsNeverCloseFrozenClient(t *testing.T) {
	c := NewConn(newFakeSink()) // writer not started

	for v := uint64(1); v <= 10_000; v++ {
		mustEnqueue(t, c, loc(fmt.Sprintf("cab-%d", v%20), v))
	}
	if c.Err() != nil {
		t.Fatalf("closed by locations: %v", c.Err())
	}
	c.mu.Lock()
	n := len(c.pending)
	c.mu.Unlock()
	if n != 20 {
		t.Fatalf("pending = %d, want 20 (one per cab)", n)
	}
}

func TestWriteErrorClosesConn(t *testing.T) {
	sink := newFakeSink()
	boom := errors.New("broken pipe")
	sink.writeErr = boom
	c := NewConn(sink)
	mustEnqueue(t, c, reliable(domain.KindStatus, 1))

	if err := c.Run(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("Run = %v, want write error", err)
	}
	if !errors.Is(c.Err(), boom) {
		t.Fatalf("Err() = %v, want write error", c.Err())
	}
	if closed, _ := sink.closeCause(); !closed {
		t.Fatal("sink not closed")
	}
}

func TestContextCancelStopsRun(t *testing.T) {
	sink := newFakeSink()
	c := NewConn(sink)
	stop := start(t, c)
	if err := stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if closed, _ := sink.closeCause(); !closed {
		t.Fatal("sink not closed")
	}
}

// Many publishers enqueue while the writer runs. Run with -race.
func TestConcurrentEnqueue(t *testing.T) {
	sink := newFakeSink()
	c := NewConn(sink)
	stop := start(t, c)
	defer stop()

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cab := fmt.Sprintf("cab-%d", g)
			for v := uint64(1); v <= 200; v++ {
				_ = c.Enqueue(loc(cab, v))
			}
		}()
	}
	for seq := uint64(1); seq <= 20; seq++ {
		mustEnqueue(t, c, reliable(domain.KindStatus, seq))
	}
	wg.Wait()

	var lastSeq uint64
	lastVer := map[string]uint64{}
	newest := 0
	deadline := time.After(2 * time.Second)
	for lastSeq < 20 || newest < 8 {
		select {
		case ev := <-sink.writes:
			if ev.Kind.Reliable() {
				if ev.Seq != lastSeq+1 {
					t.Fatalf("seq %d after %d", ev.Seq, lastSeq)
				}
				lastSeq = ev.Seq
				continue
			}
			if ev.Ver <= lastVer[ev.CabID] {
				t.Fatalf("%s ver %d after %d", ev.CabID, ev.Ver, lastVer[ev.CabID])
			}
			lastVer[ev.CabID] = ev.Ver
			if ev.Ver == 200 {
				newest++
			}
		case <-deadline:
			t.Fatalf("lastSeq=%d cabs at newest=%d", lastSeq, newest)
		}
	}
}

func reliableOn(topic domain.Topic, seq uint64) domain.Event {
	ev := reliable(domain.KindStatus, seq)
	ev.Topic = topic
	return ev
}

// Live events that arrive during a resume wait in the hold; the ones the
// catch-up already covers are sent once, the rest follow in order.
func TestResumeMergesHeldEventsWithoutDuplicates(t *testing.T) {
	sink := newFakeSink()
	c := NewConn(sink)
	stop := start(t, c)
	defer stop()

	c.Hold(trip)
	for seq := uint64(3); seq <= 6; seq++ {
		mustEnqueue(t, c, reliable(domain.KindStatus, seq))
	}
	sink.expectNoMore(t) // held, not written

	catchup := []domain.Event{reliable(domain.KindStatus, 1), reliable(domain.KindStatus, 2),
		reliable(domain.KindStatus, 3), reliable(domain.KindStatus, 4)}
	if err := c.Resume(context.Background(), trip, catchup, 4); err != nil {
		t.Fatal(err)
	}
	mustEnqueue(t, c, reliable(domain.KindStatus, 7)) // hold is over: direct
	for want := uint64(1); want <= 7; want++ {
		if ev := sink.next(t); ev.Seq != want {
			t.Fatalf("seq %d, want %d", ev.Seq, want)
		}
	}
	sink.expectNoMore(t)
}

// A snapshot's events are not contiguous and may be older than asOf; they
// are all sent, and held events up to asOf are dropped.
func TestResumeSnapshotDropsHeldUpToAsOf(t *testing.T) {
	sink := newFakeSink()
	c := NewConn(sink)
	stop := start(t, c)
	defer stop()

	c.Hold(trip)
	mustEnqueue(t, c, reliable(domain.KindStatus, 20))
	mustEnqueue(t, c, reliable(domain.KindStatus, 21))
	snap := []domain.Event{reliable(domain.KindStatus, 12), reliable(domain.KindSOS, 17)}
	if err := c.Resume(context.Background(), trip, snap, 20); err != nil {
		t.Fatal(err)
	}
	for _, want := range []uint64{12, 17, 21} {
		if ev := sink.next(t); ev.Seq != want {
			t.Fatalf("seq %d, want %d", ev.Seq, want)
		}
	}
	sink.expectNoMore(t)
}

func TestHoldOverflowClosesAsSlowConsumer(t *testing.T) {
	c := NewConn(newFakeSink())
	c.Hold(trip)
	for seq := uint64(1); seq <= ReliableBuffer; seq++ {
		mustEnqueue(t, c, reliable(domain.KindStatus, seq))
	}
	if err := c.Enqueue(reliable(domain.KindStatus, ReliableBuffer+1)); !errors.Is(err, ErrSlowConsumer) {
		t.Fatalf("err = %v, want ErrSlowConsumer", err)
	}
	if !errors.Is(c.Err(), ErrSlowConsumer) {
		t.Fatalf("conn err = %v", c.Err())
	}
}

// A replay much longer than the reliable lane must not make live events
// of other topics look like a slow consumer.
func TestLongReplayLeavesReliableLaneFree(t *testing.T) {
	sink := newFakeSink()
	sink.gate, sink.entered = make(chan struct{}), make(chan struct{}, 4096)
	c := NewConn(sink)
	stop := start(t, c)
	defer stop()

	catchup := make([]domain.Event, 500)
	for i := range catchup {
		catchup[i] = reliable(domain.KindStatus, uint64(i+1))
	}
	c.Hold(trip)
	resumed := make(chan error, 1)
	go func() { resumed <- c.Resume(context.Background(), trip, catchup, 0) }()

	<-sink.entered // Run is stuck writing the first catch-up event
	other := domain.Topic{Tenant: "acme", Kind: domain.TopicTrip, ID: "43"}
	for seq := uint64(1); seq <= ReliableBuffer; seq++ {
		mustEnqueue(t, c, reliableOn(other, seq))
	}
	close(sink.gate)
	if err := <-resumed; err != nil {
		t.Fatal(err)
	}

	got := map[domain.Topic]uint64{}
	for i := 0; i < 500+ReliableBuffer; i++ {
		ev := sink.next(t)
		if ev.Seq != got[ev.Topic]+1 {
			t.Fatalf("%s seq %d after %d", ev.Topic, ev.Seq, got[ev.Topic])
		}
		got[ev.Topic] = ev.Seq
	}
}

func TestResumeReturnsWhenConnCloses(t *testing.T) {
	c := NewConn(newFakeSink()) // Run never started: nobody takes catch-up
	c.Hold(trip)
	done := make(chan error, 1)
	go func() {
		done <- c.Resume(context.Background(), trip, []domain.Event{reliable(domain.KindStatus, 1)}, 0)
	}()
	c.Close(errors.New("client went away"))
	select {
	case err := <-done:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("err = %v, want ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Resume blocked after close")
	}
}
