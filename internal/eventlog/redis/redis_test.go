package redis

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/eventlog"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/eventlog/logtest"
)

// newClient connects to REDIS_ADDR or skips the test when it is unset.
func newClient(t *testing.T) *goredis.Client {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set; start Redis with `make redis` and export REDIS_ADDR=localhost:6379")
	}
	c := goredis.NewClient(&goredis.Options{Addr: addr})
	t.Cleanup(func() { c.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping %s: %v", addr, err)
	}
	return c
}

func TestContract(t *testing.T) {
	c := newClient(t)
	logtest.Run(t, func(t *testing.T, maxLen int) eventlog.Log {
		return New(c, Config{MaxLen: maxLen})
	})
}

func TestKeysShareHashSlot(t *testing.T) {
	topic := domain.Topic{Tenant: "acme", Kind: domain.TopicTrip, ID: "42"}
	k := keysFor(topic)
	for _, key := range []string{k.seq, k.ver, k.log, k.state, k.latest, k.dedup("e-1")} {
		if !strings.HasPrefix(key, "{t:acme:trip:42}:") {
			t.Fatalf("key %q does not start with the topic hash tag", key)
		}
	}
}

func TestPublishBroadcastsSequencedEvent(t *testing.T) {
	ctx := context.Background()
	c := newClient(t)
	l := New(c, Config{})
	topic := logtest.NewTopic(t)

	sub := c.Subscribe(ctx, Channel(topic))
	defer sub.Close()
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatal(err)
	}

	if _, _, err := l.Publish(ctx, logtest.NewEvent(topic, "s-1", "cab-1", domain.KindStatus)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := l.Publish(ctx, logtest.NewEvent(topic, "loc-1", "cab-1", domain.KindLocation)); err != nil {
		t.Fatal(err)
	}
	// A duplicate must not be broadcast again.
	if _, _, err := l.Publish(ctx, logtest.NewEvent(topic, "s-1", "cab-1", domain.KindStatus)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := l.Publish(ctx, logtest.NewEvent(topic, "s-2", "cab-1", domain.KindSOS)); err != nil {
		t.Fatal(err)
	}

	want := []struct {
		id       string
		seq, ver uint64
	}{{"s-1", 1, 0}, {"loc-1", 0, 1}, {"s-2", 2, 0}}
	for _, w := range want {
		rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		msg, err := sub.ReceiveMessage(rctx)
		cancel()
		if err != nil {
			t.Fatalf("waiting for %s: %v", w.id, err)
		}
		ev, err := Decode([]byte(msg.Payload))
		if err != nil {
			t.Fatal(err)
		}
		if ev.ID != w.id || ev.Seq != w.seq || ev.Ver != w.ver || ev.Topic != topic {
			t.Fatalf("got %s seq=%d ver=%d topic=%s, want %s seq=%d ver=%d",
				ev.ID, ev.Seq, ev.Ver, ev.Topic, w.id, w.seq, w.ver)
		}
	}
}

func TestPayloadStoredExactly(t *testing.T) {
	ctx := context.Background()
	l := New(newClient(t), Config{})
	topic := logtest.NewTopic(t)

	// Many digits: a decode/encode round trip in Lua would round these.
	payload := `{"lat":12.971598765432101,"lng":77.594562345678901,"note":"a\"b"}`
	ev := logtest.NewEvent(topic, "e-1", "cab-1", domain.KindStatus)
	ev.Payload = json.RawMessage(payload)
	if _, _, err := l.Publish(ctx, ev); err != nil {
		t.Fatal(err)
	}
	evs, err := l.After(ctx, topic, 0)
	if err != nil || len(evs) != 1 {
		t.Fatalf("After = (%d events, %v)", len(evs), err)
	}
	if got := string(evs[0].Payload); got != payload {
		t.Fatalf("payload = %s, want %s", got, payload)
	}
}

func TestDedupWindowExpires(t *testing.T) {
	ctx := context.Background()
	l := New(newClient(t), Config{DedupTTL: 200 * time.Millisecond})
	topic := logtest.NewTopic(t)
	ev := logtest.NewEvent(topic, "e-1", "cab-1", domain.KindStatus)

	if seq, dup, err := l.Publish(ctx, ev); err != nil || seq != 1 || dup {
		t.Fatalf("first = (%d, %v, %v), want (1, false, nil)", seq, dup, err)
	}
	if seq, dup, err := l.Publish(ctx, ev); err != nil || seq != 1 || !dup {
		t.Fatalf("inside window = (%d, %v, %v), want (1, true, nil)", seq, dup, err)
	}
	time.Sleep(300 * time.Millisecond)
	if seq, dup, err := l.Publish(ctx, ev); err != nil || seq != 2 || dup {
		t.Fatalf("after window = (%d, %v, %v), want (2, false, nil)", seq, dup, err)
	}
}

func TestRedisDownReturnsError(t *testing.T) {
	c := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1", MaxRetries: -1, DialTimeout: 200 * time.Millisecond})
	defer c.Close()
	l := New(c, Config{})
	topic := domain.Topic{Tenant: "acme", Kind: domain.TopicTrip, ID: "42"}

	if _, _, err := l.Publish(context.Background(), logtest.NewEvent(topic, "e-1", "cab-1", domain.KindStatus)); err == nil {
		t.Fatal("Publish with Redis down returned nil error")
	}
	if _, err := l.After(context.Background(), topic, 0); err == nil {
		t.Fatal("After with Redis down returned nil error")
	}
}
