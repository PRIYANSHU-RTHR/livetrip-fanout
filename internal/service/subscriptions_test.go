package service

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/auth"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/eventlog"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/eventlog/memory"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/fanout"
)

// recSink records what the client would receive.
type recSink struct{ writes chan domain.Event }

func newRecSink() *recSink                     { return &recSink{writes: make(chan domain.Event, 1024)} }
func (s *recSink) Write(ev domain.Event) error { s.writes <- ev; return nil }
func (s *recSink) Close(error) error           { return nil }

// next waits for the next written event.
func (s *recSink) next(t *testing.T) domain.Event {
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
func (s *recSink) expectNoMore(t *testing.T) {
	t.Helper()
	select {
	case ev := <-s.writes:
		t.Fatalf("unexpected extra write: %s seq=%d ver=%d", ev.Kind, ev.Seq, ev.Ver)
	case <-time.After(50 * time.Millisecond):
	}
}

var (
	alice   = domain.Principal{Tenant: "acme", UserID: "alice", Role: domain.RoleEmployee}
	trip42  = domain.Topic{Tenant: "acme", Kind: domain.TopicTrip, ID: "42"}
	nextID  = 0
	confirm = func(Resume) error { return nil }
)

type env struct {
	log  *memory.Log
	hub  *fanout.Hub
	subs *Subscriptions
}

func newEnv(t *testing.T, log eventlog.Log, mem *memory.Log) *env {
	t.Helper()
	trips := auth.NewStaticTrips()
	trips.Put("acme", "42", auth.Trip{EmployeeID: "alice", OfficeID: "blr-1"})
	trips.Put("globex", "42", auth.Trip{EmployeeID: "alice", OfficeID: "hyd-1"})
	hub := fanout.NewHub(fanout.Hooks{})
	return &env{log: mem, hub: hub, subs: NewSubscriptions(auth.NewRoleAuthorizer(trips), hub, log, SubscriptionsConfig{})}
}

func newMemEnv(t *testing.T, cfg memory.Config) *env {
	mem := memory.New(cfg)
	return newEnv(t, mem, mem)
}

// publish stores ev in the log and, like the gateway's Redis routing,
// fans the stored event out to the hub.
func (e *env) publish(t *testing.T, kind domain.EventKind, cab string) domain.Event {
	t.Helper()
	nextID++
	ev := domain.Event{
		BaseEvent: domain.BaseEvent{ID: fmt.Sprint("e-", nextID), Tenant: "acme", CabID: cab},
		Topic:     trip42, Kind: kind,
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
	e.hub.Publish(ev)
	return ev
}

// connect returns a running connection and its sink.
func connect(t *testing.T) (*fanout.Conn, *recSink) {
	t.Helper()
	sink := newRecSink()
	c := fanout.NewConn(sink)
	ctx, cancel := context.WithCancel(context.Background())
	go c.Run(ctx)
	t.Cleanup(cancel)
	return c, sink
}

func (e *env) subscribe(t *testing.T, c *fanout.Conn, lastSeq uint64) Resume {
	t.Helper()
	var got Resume
	if err := e.subs.Subscribe(context.Background(), alice, trip42, lastSeq, c,
		func(r Resume) error { got = r; return nil }); err != nil {
		t.Fatal(err)
	}
	return got
}

// The lift: alice's phone goes dark for 40s. Meanwhile the cab reports its
// location every second and the trip changes status three times. On
// reconnect she gets the three statuses in order and one location, the
// newest, then live events as normal.
func TestResumeAfterLift(t *testing.T) {
	e := newMemEnv(t, memory.Config{})
	c, sink := connect(t)
	e.subscribe(t, c, 0)
	e.publish(t, domain.KindStatus, "cab-7") // seq 1
	e.publish(t, domain.KindStatus, "cab-7") // seq 2
	for want := uint64(1); want <= 2; want++ {
		if ev := sink.next(t); ev.Seq != want {
			t.Fatalf("seq %d, want %d", ev.Seq, want)
		}
	}
	e.subs.Release(c)
	c.Close(errors.New("lift"))

	var newest domain.Event
	for sec := 1; sec <= 40; sec++ {
		newest = e.publish(t, domain.KindLocation, "cab-7")
		if sec%10 == 0 && sec < 40 {
			e.publish(t, domain.KindStatus, "cab-7") // seq 3, 4, 5
		}
	}

	c, sink = connect(t)
	if r := e.subscribe(t, c, 2); r != (Resume{Mode: ResumeReplay, AsOfSeq: 5}) {
		t.Fatalf("resume = %+v, want replay as of 5", r)
	}
	for want := uint64(3); want <= 5; want++ {
		if ev := sink.next(t); ev.Kind != domain.KindStatus || ev.Seq != want {
			t.Fatalf("got %s seq %d, want status %d", ev.Kind, ev.Seq, want)
		}
	}
	if ev := sink.next(t); ev.Kind != domain.KindLocation || ev.Ver != newest.Ver {
		t.Fatalf("got %s ver %d, want newest location ver %d", ev.Kind, ev.Ver, newest.Ver)
	}
	sink.expectNoMore(t)

	e.publish(t, domain.KindStatus, "cab-7")
	if ev := sink.next(t); ev.Seq != 6 {
		t.Fatalf("live seq %d, want 6", ev.Seq)
	}
}

// Away too long: what the client missed was trimmed from the log, so it
// gets a snapshot (newest per cab and kind) and is told to reset.
func TestResumeTooOldGetsSnapshot(t *testing.T) {
	e := newMemEnv(t, memory.Config{MaxLen: 5})
	for i := 0; i < 20; i++ {
		e.publish(t, domain.KindStatus, fmt.Sprint("cab-", i%2)) // seq 1..20; cab-0 ends at 19, cab-1 at 20
	}
	e.publish(t, domain.KindSOS, "cab-0") // seq 21

	c, sink := connect(t)
	if r := e.subscribe(t, c, 3); r != (Resume{Mode: ResumeSnapshot, AsOfSeq: 21}) {
		t.Fatalf("resume = %+v, want snapshot as of 21", r)
	}
	for _, want := range []struct {
		cab  string
		kind domain.EventKind
		seq  uint64
	}{{"cab-0", domain.KindStatus, 19}, {"cab-1", domain.KindStatus, 20}, {"cab-0", domain.KindSOS, 21}} {
		ev := sink.next(t)
		if ev.CabID != want.cab || ev.Kind != want.kind || ev.Seq != want.seq {
			t.Fatalf("got %s %s seq %d, want %+v", ev.CabID, ev.Kind, ev.Seq, want)
		}
	}
	sink.expectNoMore(t)
}

func TestNewSubscriberGetsSnapshot(t *testing.T) {
	e := newMemEnv(t, memory.Config{})
	c, sink := connect(t)
	if r := e.subscribe(t, c, 0); r != (Resume{Mode: ResumeSnapshot}) {
		t.Fatalf("resume on empty topic = %+v", r)
	}
	sink.expectNoMore(t)

	e.publish(t, domain.KindStatus, "cab-7")
	e.publish(t, domain.KindStatus, "cab-7")
	c2, sink2 := connect(t)
	if r := e.subscribe(t, c2, 0); r != (Resume{Mode: ResumeSnapshot, AsOfSeq: 2}) {
		t.Fatalf("resume = %+v, want snapshot as of 2", r)
	}
	if ev := sink2.next(t); ev.Seq != 2 {
		t.Fatalf("snapshot seq %d, want 2 (newest status only)", ev.Seq)
	}
	sink2.expectNoMore(t)
	if ev := sink.next(t); ev.Seq != 1 {
		t.Fatalf("first subscriber live seq %d, want 1", ev.Seq)
	}
}

// racyLog publishes extra events right after the connection was attached
// and before the catch-up is read, so they reach the client both through
// the log read and live.
type racyLog struct {
	*memory.Log
	during func()
}

func (l racyLog) After(ctx context.Context, topic domain.Topic, seq uint64) ([]domain.Event, error) {
	l.during()
	return l.Log.After(ctx, topic, seq)
}

func TestResumeOverlapIsDeliveredOnce(t *testing.T) {
	mem := memory.New(memory.Config{})
	var e *env
	e = newEnv(t, racyLog{Log: mem, during: func() {
		e.publish(t, domain.KindStatus, "cab-7") // seq 3: log and hold
		e.publish(t, domain.KindStatus, "cab-7") // seq 4: log and hold
	}}, mem)
	e.publish(t, domain.KindStatus, "cab-7")
	e.publish(t, domain.KindStatus, "cab-7")

	c, sink := connect(t)
	if r := e.subscribe(t, c, 1); r != (Resume{Mode: ResumeReplay, AsOfSeq: 4}) {
		t.Fatalf("resume = %+v", r)
	}
	for want := uint64(2); want <= 4; want++ {
		if ev := sink.next(t); ev.Seq != want {
			t.Fatalf("seq %d, want %d", ev.Seq, want)
		}
	}
	sink.expectNoMore(t)
}

func TestSubscribeDeniedLeavesHubUntouched(t *testing.T) {
	e := newMemEnv(t, memory.Config{})
	c, _ := connect(t)
	other := domain.Topic{Tenant: "globex", Kind: domain.TopicTrip, ID: "42"}
	called := false
	err := e.subs.Subscribe(context.Background(), alice, other, 0, c, func(Resume) error { called = true; return nil })
	if !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("err = %v, want ErrForbidden", err)
	}
	if called || e.hub.Topics() != 0 {
		t.Fatal("denied subscribe confirmed or attached")
	}
}

type brokenLog struct{ *memory.Log }

func (brokenLog) Snapshot(context.Context, domain.Topic) (eventlog.Snapshot, error) {
	return eventlog.Snapshot{}, errors.New("dial tcp 10.0.0.9:6379: i/o timeout")
}

func TestSubscribeLogDownDetaches(t *testing.T) {
	mem := memory.New(memory.Config{})
	e := newEnv(t, brokenLog{mem}, mem)
	c, _ := connect(t)
	err := e.subs.Subscribe(context.Background(), alice, trip42, 0, c, confirm)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}
	if e.hub.Topics() != 0 {
		t.Fatal("still attached after a failed catch-up")
	}
	// The hold is gone too: a later live event goes straight out.
	if err := c.Enqueue(domain.Event{Topic: trip42, Kind: domain.KindStatus, Seq: 1}); err != nil {
		t.Fatal(err)
	}
}

func TestSubscribeBrokerDownIsUnavailable(t *testing.T) {
	mem := memory.New(memory.Config{})
	e := newEnv(t, mem, mem)
	e.hub = fanout.NewHub(fanout.Hooks{
		OnFirstSubscriber: func(domain.Topic) error { return errors.New("subscribe not confirmed") },
	})
	e.subs = NewSubscriptions(e.subs.authz, e.hub, mem, SubscriptionsConfig{})
	c, _ := connect(t)

	called := false
	err := e.subs.Subscribe(context.Background(), alice, trip42, 0, c, func(Resume) error { called = true; return nil })
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}
	if called || e.hub.Topics() != 0 {
		t.Fatal("subscribe confirmed or attached although the broker subscribe failed")
	}
}

func TestSubscribeClosedConn(t *testing.T) {
	e := newMemEnv(t, memory.Config{})
	c := fanout.NewConn(newRecSink())
	c.Close(errors.New("client went away"))
	if err := e.subs.Subscribe(context.Background(), alice, trip42, 0, c, confirm); !errors.Is(err, fanout.ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed", err)
	}
}

type recMetrics struct {
	noMetrics
	subscribed, resumed []string
}

func (m *recMetrics) Subscribed(outcome string) { m.subscribed = append(m.subscribed, outcome) }
func (m *recMetrics) Resumed(mode string)       { m.resumed = append(m.resumed, mode) }

// An on-call engineer must tell "clients are being denied" from "clients
// were away so long they lost their history".
func TestSubscribeMetrics(t *testing.T) {
	e := newMemEnv(t, memory.Config{MaxLen: 2})
	m := &recMetrics{}
	e.subs = NewSubscriptions(e.subs.authz, e.hub, e.log, SubscriptionsConfig{Metrics: m})
	for range 5 {
		e.publish(t, domain.KindStatus, "cab-1")
	}

	c, _ := connect(t)
	e.subscribe(t, c, 0) // new client
	c2, _ := connect(t)
	e.subscribe(t, c2, 4) // replay of seq 5
	c3, _ := connect(t)
	e.subscribe(t, c3, 1) // seq 2 is trimmed
	other := domain.Topic{Tenant: "globex", Kind: domain.TopicTrip, ID: "42"}
	_ = e.subs.Subscribe(context.Background(), alice, other, 0, c, confirm)

	wantSub := []string{OutcomeOK, OutcomeOK, OutcomeOK, OutcomeForbidden}
	wantRes := []string{string(ResumeSnapshot), string(ResumeReplay), ResumeSnapshotTrimmed}
	if fmt.Sprint(m.subscribed) != fmt.Sprint(wantSub) || fmt.Sprint(m.resumed) != fmt.Sprint(wantRes) {
		t.Fatalf("subscribed = %v, resumed = %v; want %v, %v", m.subscribed, m.resumed, wantSub, wantRes)
	}
}
