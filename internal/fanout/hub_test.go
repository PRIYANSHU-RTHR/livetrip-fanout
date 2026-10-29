package fanout

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"
)

// hookRecorder checks that hooks alternate first/last per topic.
type hookRecorder struct {
	t *testing.T

	mu      sync.Mutex
	active  map[domain.Topic]bool
	firsts  int
	lasts   int
	ordered []string
}

func newHookRecorder(t *testing.T) *hookRecorder {
	return &hookRecorder{t: t, active: map[domain.Topic]bool{}}
}

func (r *hookRecorder) hooks() Hooks {
	return Hooks{
		OnFirstSubscriber: func(tp domain.Topic) { r.record(tp, true) },
		OnLastUnsubscribe: func(tp domain.Topic) { r.record(tp, false) },
	}
}

func (r *hookRecorder) record(tp domain.Topic, first bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.active[tp] == first {
		r.t.Errorf("%s: first=%v while active=%v", tp, first, r.active[tp])
	}
	r.active[tp] = first
	if first {
		r.firsts++
		r.ordered = append(r.ordered, "first "+tp.ID)
	} else {
		r.lasts++
		r.ordered = append(r.ordered, "last "+tp.ID)
	}
}

func (r *hookRecorder) activeTopics() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, a := range r.active {
		if a {
			n++
		}
	}
	return n
}

func topicN(i int) domain.Topic {
	return domain.Topic{Tenant: "acme", Kind: domain.TopicTrip, ID: fmt.Sprint(i)}
}

func onTopic(ev domain.Event, tp domain.Topic) domain.Event {
	ev.Topic = tp
	return ev
}

func mustAttach(t *testing.T, h *Hub, tp domain.Topic, c *Conn) {
	t.Helper()
	if err := h.Attach(tp, c); err != nil {
		t.Fatalf("Attach(%s): %v", tp, err)
	}
}

func TestPublishOnlyToSubscribers(t *testing.T) {
	h := NewHub(Hooks{})
	a, b := newFakeSink(), newFakeSink()
	ca, cb := NewConn(a), NewConn(b)
	defer start(t, ca)()
	defer start(t, cb)()

	mustAttach(t, h, topicN(1), ca)
	mustAttach(t, h, topicN(1), cb)
	mustAttach(t, h, topicN(1), cb) // twice: still one delivery
	mustAttach(t, h, topicN(2), cb)

	if n := h.Publish(onTopic(reliable(domain.KindStatus, 1), topicN(1))); n != 2 {
		t.Fatalf("Publish to topic 1 reached %d, want 2", n)
	}
	if n := h.Publish(onTopic(reliable(domain.KindStatus, 1), topicN(2))); n != 1 {
		t.Fatalf("Publish to topic 2 reached %d, want 1", n)
	}
	if n := h.Publish(onTopic(reliable(domain.KindStatus, 1), topicN(3))); n != 0 {
		t.Fatalf("Publish to topic 3 reached %d, want 0", n)
	}

	if ev := a.next(t); ev.Topic != topicN(1) {
		t.Fatalf("a got %s", ev.Topic)
	}
	a.expectNoMore(t)
	got := map[domain.Topic]int{}
	got[b.next(t).Topic]++
	got[b.next(t).Topic]++
	if got[topicN(1)] != 1 || got[topicN(2)] != 1 {
		t.Fatalf("b got %v, want one event on each of topics 1 and 2", got)
	}
	b.expectNoMore(t)
}

func TestHooksFireOnFirstAndLast(t *testing.T) {
	rec := newHookRecorder(t)
	h := NewHub(rec.hooks())
	c1, c2 := NewConn(newFakeSink()), NewConn(newFakeSink())
	tp := topicN(1)

	mustAttach(t, h, tp, c1)
	mustAttach(t, h, tp, c2)
	h.Detach(tp, c1)
	h.Detach(tp, c1) // not attached any more: no-op
	if rec.lasts != 0 {
		t.Fatalf("last hook fired with a subscriber left")
	}
	h.Detach(tp, c2)

	want := []string{"first 1", "last 1"}
	if fmt.Sprint(rec.ordered) != fmt.Sprint(want) {
		t.Fatalf("hooks = %v, want %v", rec.ordered, want)
	}
	if h.Topics() != 0 {
		t.Fatalf("Topics() = %d, want 0", h.Topics())
	}
}

func TestDetachAll(t *testing.T) {
	rec := newHookRecorder(t)
	h := NewHub(rec.hooks())
	c, other := NewConn(newFakeSink()), NewConn(newFakeSink())

	for i := range 3 {
		mustAttach(t, h, topicN(i), c)
	}
	mustAttach(t, h, topicN(0), other)

	h.DetachAll(c)

	if h.Subscribers(topicN(0)) != 1 || h.Topics() != 1 {
		t.Fatalf("subs(0)=%d topics=%d, want 1 and 1", h.Subscribers(topicN(0)), h.Topics())
	}
	if rec.firsts != 3 || rec.lasts != 2 {
		t.Fatalf("firsts=%d lasts=%d, want 3 and 2", rec.firsts, rec.lasts)
	}
	h.DetachAll(c) // again: no-op
	if rec.lasts != 2 {
		t.Fatalf("lasts=%d after second DetachAll, want 2", rec.lasts)
	}
}

func TestAttachClosedConn(t *testing.T) {
	h := NewHub(Hooks{})
	c := NewConn(newFakeSink())
	c.Close(errors.New("gone"))
	if err := h.Attach(topicN(1), c); !errors.Is(err, ErrClosed) {
		t.Fatalf("Attach = %v, want ErrClosed", err)
	}
	if h.Topics() != 0 {
		t.Fatal("closed conn was attached")
	}
}

// One client is stuck inside a socket write. Publishing must not wait for
// it, and a healthy client on the same topic must get everything quickly.
func TestFrozenConnDoesNotDelayOthers(t *testing.T) {
	h := NewHub(Hooks{})
	tp := topicN(1)

	frozen := newFakeSink()
	frozen.gate = make(chan struct{}) // never opened
	frozen.entered = make(chan struct{}, 4096)
	cf := NewConn(frozen)
	stopFrozen := start(t, cf)
	mustAttach(t, h, tp, cf)

	healthy := newFakeSink()
	ch := NewConn(healthy)
	defer start(t, ch)()
	mustAttach(t, h, tp, ch)

	// Get the frozen writer stuck in Write.
	h.Publish(onTopic(reliable(domain.KindStatus, 1), tp))
	select {
	case <-frozen.entered:
	case <-time.After(time.Second):
		t.Fatal("frozen writer never started a write")
	}
	healthy.next(t)

	const n = 2000
	began := time.Now()
	for seq := uint64(2); seq <= n; seq++ {
		h.Publish(onTopic(reliable(domain.KindStatus, seq), tp))
		h.Publish(onTopic(loc("cab-1", seq), tp))
		if seq%32 == 0 { // keep the healthy client within its buffer
			for want := seq - 31; want <= seq; want++ {
				if want == 1 {
					continue
				}
				for {
					ev := healthy.next(t)
					if !ev.Kind.Reliable() {
						continue
					}
					if ev.Seq != want {
						t.Fatalf("healthy seq = %d, want %d", ev.Seq, want)
					}
					break
				}
			}
		}
	}
	if d := time.Since(began); d > 2*time.Second {
		t.Fatalf("publishing %d events took %v with a frozen client", n, d)
	}

	// The frozen client overflowed and was closed, not silently skipped.
	if !errors.Is(cf.Err(), ErrSlowConsumer) {
		t.Fatalf("frozen conn Err() = %v, want ErrSlowConsumer", cf.Err())
	}
	if ch.Err() != nil {
		t.Fatalf("healthy conn closed: %v", ch.Err())
	}

	close(frozen.gate)
	if err := stopFrozen(); !errors.Is(err, ErrSlowConsumer) {
		t.Fatalf("frozen Run = %v, want ErrSlowConsumer", err)
	}
	h.DetachAll(cf)
	if h.Subscribers(tp) != 1 {
		t.Fatalf("subscribers = %d, want 1", h.Subscribers(tp))
	}
}

// Many goroutines attach, detach and publish at once. Run with -race.
func TestConcurrentAttachDetach(t *testing.T) {
	rec := newHookRecorder(t)
	h := NewHub(rec.hooks())
	const topics, workers, rounds = 5, 16, 300

	conns := make([]*Conn, workers)
	for i := range conns {
		conns[i] = NewConn(newFakeSink())
		defer start(t, conns[i])()
	}

	var wg sync.WaitGroup
	stopPub := make(chan struct{})
	pubDone := make(chan struct{})
	go func() {
		defer close(pubDone)
		for v := uint64(1); ; v++ {
			select {
			case <-stopPub:
				return
			default:
			}
			h.Publish(onTopic(loc("cab-1", v), topicN(int(v)%topics)))
		}
	}()

	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := conns[w]
			for r := range rounds {
				tp := topicN((w + r) % topics)
				if err := h.Attach(tp, c); err != nil {
					t.Errorf("Attach: %v", err)
					return
				}
				if r%3 == 0 {
					h.DetachAll(c)
				} else {
					h.Detach(tp, c)
				}
			}
			// Leave odd workers subscribed to one topic.
			if w%2 == 1 {
				_ = h.Attach(topicN(w%topics), c)
			}
		}()
	}
	wg.Wait()
	close(stopPub)
	<-pubDone

	if got, want := rec.activeTopics(), h.Topics(); got != want {
		t.Fatalf("hooks say %d active topics, hub has %d", got, want)
	}
	total := 0
	for i := range topics {
		total += h.Subscribers(topicN(i))
	}
	if total != workers/2 {
		t.Fatalf("subscriptions left = %d, want %d", total, workers/2)
	}
	for _, c := range conns {
		h.DetachAll(c)
	}
	if h.Topics() != 0 || rec.activeTopics() != 0 {
		t.Fatalf("after DetachAll: hub topics=%d, hook active=%d", h.Topics(), rec.activeTopics())
	}
}
