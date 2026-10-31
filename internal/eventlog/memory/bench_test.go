package memory

import (
	"context"
	"strconv"
	"testing"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/eventlog/logtest"
)

var benchTopic = domain.Topic{Tenant: "acme", Kind: domain.TopicTrip, ID: "42"}

// One publish per iteration, each with a new eventId, so the dedup window
// grows as it would under real traffic. Building the id is one allocation.
func BenchmarkPublish(b *testing.B) {
	for _, kind := range []domain.EventKind{domain.KindStatus, domain.KindLocation} {
		b.Run(string(kind), func(b *testing.B) {
			ctx := context.Background()
			l := New(Config{})
			ev := logtest.NewEvent(benchTopic, "", "cab-1", kind)
			b.ReportAllocs()
			for i := 0; b.Loop(); i++ {
				ev.ID = "e-" + strconv.Itoa(i)
				if _, _, err := l.Publish(ctx, ev); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// A replay of 1,000 events: a client resuming from the start of a full log.
func BenchmarkAfter(b *testing.B) {
	const events = 1000
	ctx := context.Background()
	l := New(Config{MaxLen: events})
	for i := range events {
		ev := logtest.NewEvent(benchTopic, "e-"+strconv.Itoa(i), "cab-1", domain.KindStatus)
		if _, _, err := l.Publish(ctx, ev); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	for b.Loop() {
		evs, err := l.After(ctx, benchTopic, 0)
		if err != nil || len(evs) != events {
			b.Fatalf("After = %d events, %v", len(evs), err)
		}
	}
}
