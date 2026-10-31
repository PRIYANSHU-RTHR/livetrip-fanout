package fanout

import (
	"fmt"
	"testing"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"
)

// drain empties c's reliable lane, standing in for Run.
func drain(c *Conn) {
	for {
		select {
		case <-c.reliable:
		default:
			return
		}
	}
}

// Enqueue of a status plus taking it off the lane, without the sink write.
func BenchmarkConnEnqueueReliable(b *testing.B) {
	c := NewConn(newFakeSink())
	ev := reliable(domain.KindStatus, 1)
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		if err := c.Enqueue(ev); err != nil {
			b.Fatal(err)
		}
		if i%ReliableBuffer == ReliableBuffer-1 {
			drain(c)
		}
	}
}

// 100 cabs on one topic, each location replacing an unsent one.
func BenchmarkConnConflate(b *testing.B) {
	const cabs = 100
	c := NewConn(newFakeSink())
	evs := make([]domain.Event, cabs)
	for i := range evs {
		evs[i] = loc(fmt.Sprintf("cab-%d", i), 0)
	}
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		ev := evs[i%cabs]
		ev.Ver = uint64(i + 1)
		if err := c.Enqueue(ev); err != nil {
			b.Fatal(err)
		}
	}
}

// One event fanned out to every subscriber of a topic. Nobody runs the
// connections' writers, so this is the cost of Publish alone.
func BenchmarkHubPublish(b *testing.B) {
	for _, kind := range []domain.EventKind{domain.KindStatus, domain.KindLocation} {
		for _, subs := range []int{1, 100, 10_000} {
			b.Run(fmt.Sprintf("%s/subs=%d", kind, subs), func(b *testing.B) {
				h := NewHub(Hooks{})
				conns := make([]*Conn, subs)
				for i := range conns {
					conns[i] = NewConn(newFakeSink())
					if err := h.Attach(trip, conns[i]); err != nil {
						b.Fatal(err)
					}
				}
				ev := loc("cab-1", 0)
				if kind.Reliable() {
					ev = reliable(kind, 1)
				}
				// The first location creates each connection's slot; that
				// is subscribe cost, not publish cost.
				h.Publish(ev)
				for _, c := range conns {
					drain(c)
				}
				b.ReportAllocs()
				n := 0
				for i := 0; b.Loop(); i++ {
					ev.Ver = uint64(i + 1)
					if got := h.Publish(ev); got != subs {
						b.Fatalf("delivered to %d, want %d", got, subs)
					}
					n += subs
					if kind.Reliable() && i%ReliableBuffer == ReliableBuffer-1 {
						b.StopTimer()
						for _, c := range conns {
							drain(c)
						}
						b.StartTimer()
					}
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(n), "ns/delivery")
			})
		}
	}
}
