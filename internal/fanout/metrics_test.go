package fanout

import (
	"context"
	"sync"
	"testing"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"
)

type fakeMetrics struct {
	mu        sync.Mutex
	delivered []uint64 // Seq, or Ver for locations
	depths    []int
	conflated int
}

func (m *fakeMetrics) Delivered(ev domain.Event) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.delivered = append(m.delivered, ev.Seq+ev.Ver)
}

func (m *fakeMetrics) ReliableQueued(depth int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.depths = append(m.depths, depth)
}

func (m *fakeMetrics) LocationConflated() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.conflated++
}

func TestMetricsQueueDepthAndConflation(t *testing.T) {
	m := &fakeMetrics{}
	c := NewConnWithMetrics(newFakeSink(), m) // writer not started

	mustEnqueue(t, c, reliable(domain.KindStatus, 1))
	mustEnqueue(t, c, reliable(domain.KindStatus, 2))
	mustEnqueue(t, c, loc("cab-1", 1))
	mustEnqueue(t, c, loc("cab-1", 2)) // replaces ver 1
	mustEnqueue(t, c, loc("cab-1", 2)) // duplicate: not a conflation
	mustEnqueue(t, c, loc("cab-2", 1))

	if len(m.depths) != 2 || m.depths[0] != 1 || m.depths[1] != 2 {
		t.Fatalf("depths = %v, want [1 2]", m.depths)
	}
	if m.conflated != 1 {
		t.Fatalf("conflated = %d, want 1", m.conflated)
	}
}

// Only live events count towards delivery latency: a catch-up event is
// old on purpose and would make the latency look like the client's
// time away.
func TestMetricsDeliveredOnlyForLiveEvents(t *testing.T) {
	m := &fakeMetrics{}
	sink := newFakeSink()
	c := NewConnWithMetrics(sink, m)
	stop := start(t, c)

	c.Hold(trip)
	mustEnqueue(t, c, reliable(domain.KindStatus, 3)) // held: sent by Resume
	if err := c.Resume(context.Background(), trip,
		[]domain.Event{reliable(domain.KindStatus, 1), reliable(domain.KindStatus, 2)}, 0); err != nil {
		t.Fatal(err)
	}
	mustEnqueue(t, c, reliable(domain.KindSOS, 4))
	mustEnqueue(t, c, loc("cab-1", 9))
	for range 5 {
		sink.next(t)
	}
	// Delivered runs just after the write returns; stopping Run waits
	// for it.
	stop()

	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.delivered) != 2 || m.delivered[0] != 4 || m.delivered[1] != 9 {
		t.Fatalf("delivered = %v, want [4 9]", m.delivered)
	}
}
