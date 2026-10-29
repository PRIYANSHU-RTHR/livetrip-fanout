package memory

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/eventlog"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/eventlog/logtest"
)

func TestContract(t *testing.T) {
	logtest.Run(t, func(t *testing.T, maxLen int) eventlog.Log {
		return New(Config{MaxLen: maxLen})
	})
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func TestTrimmingIsExact(t *testing.T) {
	ctx := context.Background()
	l := New(Config{MaxLen: 10})
	topic := logtest.NewTopic(t)

	for i := 1; i <= 35; i++ {
		if _, _, err := l.Publish(ctx, logtest.NewEvent(topic, fmt.Sprintf("e-%d", i), "cab-1", domain.KindStatus)); err != nil {
			t.Fatal(err)
		}
		first, _ := l.FirstSeq(ctx, topic)
		if want := uint64(max(1, i-9)); first != want {
			t.Fatalf("after %d publishes FirstSeq = %d, want %d", i, first, want)
		}
	}
	evs, err := l.After(ctx, topic, 25)
	if err != nil || len(evs) != 10 || evs[0].Seq != 26 || evs[9].Seq != 35 {
		t.Fatalf("After(25) = %d events, err %v; want seq 26..35", len(evs), err)
	}
	if _, err := l.After(ctx, topic, 24); !errors.Is(err, eventlog.ErrTrimmed) {
		t.Fatalf("After(24) err = %v, want ErrTrimmed", err)
	}
}

func TestDedupWindowExpires(t *testing.T) {
	ctx := context.Background()
	clock := &fakeClock{now: time.Unix(1700000000, 0)}
	l := New(Config{DedupTTL: time.Minute, Now: clock.Now})
	topic := logtest.NewTopic(t)
	ev := logtest.NewEvent(topic, "e-1", "cab-1", domain.KindStatus)

	if seq, dup, _ := l.Publish(ctx, ev); seq != 1 || dup {
		t.Fatalf("first = (%d, %v), want (1, false)", seq, dup)
	}
	clock.Advance(59 * time.Second)
	if seq, dup, _ := l.Publish(ctx, ev); seq != 1 || !dup {
		t.Fatalf("inside window = (%d, %v), want (1, true)", seq, dup)
	}
	clock.Advance(time.Second)
	if seq, dup, _ := l.Publish(ctx, ev); seq != 2 || dup {
		t.Fatalf("after window = (%d, %v), want (2, false)", seq, dup)
	}

	tl := l.topic(topic, false)
	if len(tl.seen) != 1 || len(tl.expiry) != 1 {
		t.Fatalf("dedup state not swept: seen=%d expiry=%d", len(tl.seen), len(tl.expiry))
	}
}

func TestStoredEventIsACopy(t *testing.T) {
	ctx := context.Background()
	l := New(Config{})
	topic := logtest.NewTopic(t)
	ev := logtest.NewEvent(topic, "e-1", "cab-1", domain.KindStatus)

	if _, _, err := l.Publish(ctx, ev); err != nil {
		t.Fatal(err)
	}
	ev.Payload[0] = 'X'
	evs, _ := l.After(ctx, topic, 0)
	if evs[0].Payload[0] != '{' {
		t.Fatalf("caller's payload change leaked into the log: %s", evs[0].Payload)
	}
}

func TestCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	l := New(Config{})
	ev := logtest.NewEvent(logtest.NewTopic(t), "e-1", "cab-1", domain.KindStatus)
	if _, _, err := l.Publish(ctx, ev); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}
