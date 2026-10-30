package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/eventlog"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/eventlog/memory"
)

var tripTopic = domain.Topic{Tenant: "acme", Kind: domain.TopicTrip, ID: "42"}

func event(id string, kind domain.EventKind) domain.Event {
	return domain.Event{
		BaseEvent: domain.BaseEvent{ID: id, Tenant: "acme", CabID: "cab-7"},
		Topic:     tripTopic,
		Kind:      kind,
		Payload:   json.RawMessage(`{"status":"arrived"}`),
	}
}

// stubLog lets a test control Publish; the rest comes from a memory log.
type stubLog struct {
	eventlog.Log
	calls   atomic.Int32
	publish func(ctx context.Context, ev domain.Event) (uint64, bool, error)
}

func (s *stubLog) Publish(ctx context.Context, ev domain.Event) (uint64, bool, error) {
	s.calls.Add(1)
	return s.publish(ctx, ev)
}

func TestPublishAssignsSeqAndVer(t *testing.T) {
	log := memory.New(memory.Config{})
	p := NewPublisher(log, log, PublisherConfig{})
	ctx := context.Background()

	res, err := p.Publish(ctx, event("s1", domain.KindStatus))
	if err != nil || res.Seq != 1 || res.Ver != 0 || res.Duplicate {
		t.Fatalf("status = %+v, %v; want seq 1", res, err)
	}
	res, err = p.Publish(ctx, event("l1", domain.KindLocation))
	if err != nil || res.Ver != 1 || res.Seq != 0 {
		t.Fatalf("location = %+v, %v; want ver 1", res, err)
	}
}

func TestPublishDuplicateReturnsOriginalSeq(t *testing.T) {
	log := memory.New(memory.Config{})
	p := NewPublisher(log, log, PublisherConfig{})
	ctx := context.Background()

	first, err := p.Publish(ctx, event("s1", domain.KindStatus))
	if err != nil {
		t.Fatal(err)
	}
	again, err := p.Publish(ctx, event("s1", domain.KindStatus))
	if err != nil {
		t.Fatal(err)
	}
	if !again.Duplicate || again.Seq != first.Seq {
		t.Fatalf("retry = %+v, want duplicate of seq %d", again, first.Seq)
	}
}

func TestPublishSetsCreatedAt(t *testing.T) {
	now := time.Date(2026, 10, 30, 9, 0, 0, 0, time.UTC)
	var got time.Time
	log := &stubLog{publish: func(_ context.Context, ev domain.Event) (uint64, bool, error) {
		got = ev.CreatedAt
		return 1, false, nil
	}}
	p := NewPublisher(log, log, PublisherConfig{Now: func() time.Time { return now }})
	if _, err := p.Publish(context.Background(), event("s1", domain.KindStatus)); err != nil {
		t.Fatal(err)
	}
	if !got.Equal(now) {
		t.Fatalf("createdAt = %v, want %v", got, now)
	}
}

func TestPublishRejectsInvalid(t *testing.T) {
	log := &stubLog{publish: func(context.Context, domain.Event) (uint64, bool, error) {
		return 1, false, nil
	}}
	p := NewPublisher(log, log, PublisherConfig{})

	noID := event("", domain.KindStatus)
	locOnSOS := event("l1", domain.KindLocation)
	locOnSOS.Topic = domain.Topic{Tenant: "acme", Kind: domain.TopicSOS, ID: "blr-1"}

	for name, ev := range map[string]domain.Event{"missing eventId": noID, "location on sos topic": locOnSOS} {
		t.Run(name, func(t *testing.T) {
			if _, err := p.Publish(context.Background(), ev); !errors.Is(err, domain.ErrBadEvent) {
				t.Fatalf("err = %v, want ErrBadEvent", err)
			}
		})
	}
	if n := log.calls.Load(); n != 0 {
		t.Fatalf("log called %d times for invalid events", n)
	}
}

func TestSOSUsesItsOwnLane(t *testing.T) {
	events := memory.New(memory.Config{})
	sos := memory.New(memory.Config{})
	p := NewPublisher(events, sos, PublisherConfig{})
	ctx := context.Background()

	if _, err := p.Publish(ctx, event("sos-1", domain.KindSOS)); err != nil {
		t.Fatal(err)
	}
	if got, _ := sos.After(ctx, tripTopic, 0); len(got) != 1 {
		t.Fatalf("sos lane has %d events, want 1", len(got))
	}
	if got, _ := events.After(ctx, tripTopic, 0); len(got) != 0 {
		t.Fatalf("events lane has %d events, want 0", len(got))
	}
}

// With every location slot stuck on a slow log, new locations are shed at
// once and an SOS still goes through.
func TestSOSNotBlockedByLocationFlood(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	stuck := &stubLog{publish: func(ctx context.Context, _ domain.Event) (uint64, bool, error) {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return 0, false, ctx.Err()
	}}
	sos := memory.New(memory.Config{})
	p := NewPublisher(stuck, sos, PublisherConfig{MaxInFlight: 2, Timeout: 10 * time.Second})
	ctx := context.Background()

	for i := range 2 {
		go p.Publish(ctx, event("l-stuck-"+string(rune('a'+i)), domain.KindLocation))
	}
	waitFor(t, func() bool { return stuck.calls.Load() == 2 })

	start := time.Now()
	if _, err := p.Publish(ctx, event("l-new", domain.KindLocation)); !errors.Is(err, ErrBusy) {
		t.Fatalf("location err = %v, want ErrBusy", err)
	}
	res, err := p.Publish(ctx, event("sos-1", domain.KindSOS))
	if err != nil || res.Seq != 1 {
		t.Fatalf("sos = %+v, %v; want seq 1", res, err)
	}
	if d := time.Since(start); d > 100*time.Millisecond {
		t.Fatalf("shedding and sos took %v", d)
	}
}

func TestSOSRetriesOnce(t *testing.T) {
	sos := &stubLog{}
	sos.publish = func(context.Context, domain.Event) (uint64, bool, error) {
		if sos.calls.Load() == 1 {
			return 0, false, errors.New("connection reset")
		}
		return 7, false, nil
	}
	p := NewPublisher(nil, sos, PublisherConfig{})

	res, err := p.Publish(context.Background(), event("sos-1", domain.KindSOS))
	if err != nil || res.Seq != 7 {
		t.Fatalf("sos = %+v, %v; want seq 7 after retry", res, err)
	}
	if n := sos.calls.Load(); n != 2 {
		t.Fatalf("calls = %d, want 2", n)
	}
}

func TestLogErrorIsUnavailable(t *testing.T) {
	down := &stubLog{publish: func(context.Context, domain.Event) (uint64, bool, error) {
		return 0, false, errors.New("dial tcp 10.0.0.5:6379: connection refused")
	}}
	p := NewPublisher(down, down, PublisherConfig{})
	ctx := context.Background()

	if _, err := p.Publish(ctx, event("s1", domain.KindStatus)); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("status err = %v, want ErrUnavailable", err)
	}
	down.calls.Store(0)
	if _, err := p.Publish(ctx, event("sos-1", domain.KindSOS)); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("sos err = %v, want ErrUnavailable", err)
	}
	if n := down.calls.Load(); n != 2 {
		t.Fatalf("sos attempts = %d, want 2", n)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(time.Millisecond)
	}
}

type pubMetrics struct {
	noMetrics
	outcomes []string
	retries  int
}

func (m *pubMetrics) Published(kind domain.EventKind, outcome string, _ time.Duration) {
	m.outcomes = append(m.outcomes, string(kind)+":"+outcome)
}
func (m *pubMetrics) SOSRetried() { m.retries++ }

func TestPublishMetrics(t *testing.T) {
	log := memory.New(memory.Config{})
	flaky := &stubLog{}
	flaky.publish = func(context.Context, domain.Event) (uint64, bool, error) {
		if flaky.calls.Load() == 1 {
			return 0, false, errors.New("connection reset")
		}
		return 1, false, nil
	}
	m := &pubMetrics{}
	p := NewPublisher(log, flaky, PublisherConfig{Metrics: m})
	ctx := context.Background()

	_, _ = p.Publish(ctx, event("s1", domain.KindStatus))
	_, _ = p.Publish(ctx, event("s1", domain.KindStatus))
	_, _ = p.Publish(ctx, event("sos-1", domain.KindSOS))
	_, _ = p.Publish(ctx, event("x1", "teleport")) // client-chosen kind

	want := "[status:ok status:duplicate sos:ok unknown:invalid]"
	if got := fmt.Sprint(m.outcomes); got != want || m.retries != 1 {
		t.Fatalf("outcomes = %s, retries = %d; want %s, 1", got, m.retries, want)
	}
}
