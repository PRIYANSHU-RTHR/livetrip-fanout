// Package logtest holds the contract tests every eventlog.Log must pass.
// The memory and Redis implementations both call Run.
package logtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/eventlog"
)

// Factory returns a fresh log keeping at least maxLen reliable events per
// topic. Implementations with approximate trimming may keep more.
type Factory func(t *testing.T, maxLen int) eventlog.Log

// Run runs the contract tests against logs made by newLog.
func Run(t *testing.T, newLog Factory) {
	tests := []struct {
		name string
		fn   func(*testing.T, Factory)
	}{
		{"GapFreeConcurrent", testGapFreeConcurrent},
		{"DuplicateEventID", testDuplicateEventID},
		{"ConcurrentDuplicates", testConcurrentDuplicates},
		{"TrimmingMovesFirstSeq", testTrimming},
		{"LocationsConflate", testLocations},
		{"Snapshot", testSnapshot},
		{"TopicsIndependent", testTopicsIndependent},
		{"EmptyTopic", testEmptyTopic},
		{"RejectsInvalid", testRejectsInvalid},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) { tc.fn(t, newLog) })
	}
}

var topicCounter atomic.Uint64

// NewTopic returns a topic no other test uses, so implementations backed by
// a shared store do not need a flush between tests.
func NewTopic(t *testing.T) domain.Topic {
	t.Helper()
	name := strings.NewReplacer("/", "-", " ", "_").Replace(t.Name())
	id := fmt.Sprintf("%s-%d-%d", name, time.Now().UnixNano(), topicCounter.Add(1))
	if len(id) > 128 {
		id = id[len(id)-128:]
	}
	return domain.Topic{Tenant: "acme", Kind: domain.TopicTrip, ID: id}
}

// NewEvent builds a valid event on topic.
func NewEvent(topic domain.Topic, id, cabID string, kind domain.EventKind) domain.Event {
	return domain.Event{
		BaseEvent: domain.BaseEvent{ID: id, Tenant: topic.Tenant, CabID: cabID, CreatedAt: time.Now().UTC()},
		Topic:     topic,
		Kind:      kind,
		Payload:   json.RawMessage(`{"id":"` + id + `"}`),
	}
}

func mustPublish(t *testing.T, l eventlog.Log, ev domain.Event) uint64 {
	t.Helper()
	pos, dup, err := l.Publish(context.Background(), ev)
	if err != nil {
		t.Fatalf("Publish(%s): %v", ev.ID, err)
	}
	if dup {
		t.Fatalf("Publish(%s): unexpected dup", ev.ID)
	}
	return pos
}

func mustAfter(t *testing.T, l eventlog.Log, topic domain.Topic, seq uint64) []domain.Event {
	t.Helper()
	evs, err := l.After(context.Background(), topic, seq)
	if err != nil {
		t.Fatalf("After(%d): %v", seq, err)
	}
	return evs
}

// checkContiguous asserts evs carry Seq from..to with no gaps.
func checkContiguous(t *testing.T, evs []domain.Event, from, to uint64) {
	t.Helper()
	if want := int(to - from + 1); len(evs) != want {
		t.Fatalf("got %d events, want %d (seq %d..%d)", len(evs), want, from, to)
	}
	for i, ev := range evs {
		if want := from + uint64(i); ev.Seq != want {
			t.Fatalf("event %d has seq %d, want %d", i, ev.Seq, want)
		}
	}
}

func testGapFreeConcurrent(t *testing.T, newLog Factory) {
	const workers, perWorker = 50, 20
	const total = workers * perWorker
	l := newLog(t, 2*total)
	topic := NewTopic(t)

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		seqs []uint64
		errs = make(chan error, total)
	)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				ev := NewEvent(topic, fmt.Sprintf("w%d-e%d", w, i), fmt.Sprintf("cab-%d", w), domain.KindStatus)
				seq, dup, err := l.Publish(context.Background(), ev)
				if err != nil || dup {
					errs <- fmt.Errorf("publish %s: dup=%v err=%v", ev.ID, dup, err)
					return
				}
				mu.Lock()
				seqs = append(seqs, seq)
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	for i, s := range seqs {
		if s != uint64(i+1) {
			t.Fatalf("sorted seq[%d] = %d, want %d (gap or duplicate)", i, s, i+1)
		}
	}
	checkContiguous(t, mustAfter(t, l, topic, 0), 1, total)
}

func testDuplicateEventID(t *testing.T, newLog Factory) {
	ctx := context.Background()
	l := newLog(t, 100)
	topic := NewTopic(t)

	first := mustPublish(t, l, NewEvent(topic, "e-1", "cab-1", domain.KindStatus))
	again := NewEvent(topic, "e-1", "cab-1", domain.KindStatus)
	again.Payload = json.RawMessage(`{"retry":true}`)
	pos, dup, err := l.Publish(ctx, again)
	if err != nil || !dup || pos != first {
		t.Fatalf("retry = (%d, %v, %v), want (%d, true, nil)", pos, dup, err, first)
	}
	if next := mustPublish(t, l, NewEvent(topic, "e-2", "cab-1", domain.KindStatus)); next != first+1 {
		t.Fatalf("next seq = %d, want %d (duplicate consumed a seq)", next, first+1)
	}
	evs := mustAfter(t, l, topic, 0)
	checkContiguous(t, evs, 1, 2)
	if string(evs[0].Payload) != `{"id":"e-1"}` {
		t.Fatalf("stored payload = %s, want the first publish", evs[0].Payload)
	}

	// Locations are deduplicated too and keep their Ver.
	ver := mustPublish(t, l, NewEvent(topic, "loc-1", "cab-1", domain.KindLocation))
	pos, dup, err = l.Publish(ctx, NewEvent(topic, "loc-1", "cab-1", domain.KindLocation))
	if err != nil || !dup || pos != ver {
		t.Fatalf("location retry = (%d, %v, %v), want (%d, true, nil)", pos, dup, err, ver)
	}
}

func testConcurrentDuplicates(t *testing.T, newLog Factory) {
	const n = 20
	l := newLog(t, 100)
	topic := NewTopic(t)

	type result struct {
		pos uint64
		dup bool
		err error
	}
	results := make(chan result, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pos, dup, err := l.Publish(context.Background(), NewEvent(topic, "same", "cab-1", domain.KindSOS))
			results <- result{pos, dup, err}
		}()
	}
	wg.Wait()
	close(results)

	fresh := 0
	for r := range results {
		if r.err != nil {
			t.Fatal(r.err)
		}
		if r.pos != 1 {
			t.Fatalf("pos = %d, want 1 for every retry", r.pos)
		}
		if !r.dup {
			fresh++
		}
	}
	if fresh != 1 {
		t.Fatalf("%d publishes reported new, want exactly 1", fresh)
	}
	checkContiguous(t, mustAfter(t, l, topic, 0), 1, 1)
}

func testTrimming(t *testing.T, newLog Factory) {
	const maxLen, total = 10, 200
	ctx := context.Background()
	l := newLog(t, maxLen)
	topic := NewTopic(t)

	for i := 1; i <= total; i++ {
		mustPublish(t, l, NewEvent(topic, fmt.Sprintf("e-%d", i), "cab-1", domain.KindStatus))
	}

	first, err := l.FirstSeq(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if first <= 1 || first > total-maxLen+1 {
		t.Fatalf("FirstSeq = %d, want in (1, %d]", first, total-maxLen+1)
	}
	if _, err := l.After(ctx, topic, 0); !errors.Is(err, eventlog.ErrTrimmed) {
		t.Fatalf("After(0) err = %v, want ErrTrimmed", err)
	}
	if _, err := l.After(ctx, topic, first-2); first >= 2 && !errors.Is(err, eventlog.ErrTrimmed) {
		t.Fatalf("After(first-2) err = %v, want ErrTrimmed", err)
	}
	checkContiguous(t, mustAfter(t, l, topic, first-1), first, total)
	if last, err := l.LastSeq(ctx, topic); err != nil || last != total {
		t.Fatalf("LastSeq = (%d, %v), want (%d, nil)", last, err, total)
	}
	if evs := mustAfter(t, l, topic, total); len(evs) != 0 {
		t.Fatalf("After(last) returned %d events, want 0", len(evs))
	}
}

func testLocations(t *testing.T, newLog Factory) {
	ctx := context.Background()
	l := newLog(t, 100)
	topic := NewTopic(t)

	var lastVer uint64
	for i := 1; i <= 5; i++ {
		for _, cab := range []string{"cab-a", "cab-b"} {
			ver := mustPublish(t, l, NewEvent(topic, fmt.Sprintf("%s-%d", cab, i), cab, domain.KindLocation))
			if ver <= lastVer {
				t.Fatalf("ver %d not above previous %d", ver, lastVer)
			}
			lastVer = ver
		}
	}

	if first, _ := l.FirstSeq(ctx, topic); first != 0 {
		t.Fatalf("FirstSeq = %d after only locations, want 0", first)
	}
	if evs := mustAfter(t, l, topic, 0); len(evs) != 0 {
		t.Fatalf("After returned %d locations, want 0", len(evs))
	}

	latest, err := l.LatestLocation(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, ev := range latest {
		if ev.Seq != 0 || ev.Ver == 0 {
			t.Fatalf("location %s has seq=%d ver=%d", ev.ID, ev.Seq, ev.Ver)
		}
		got[ev.CabID] = ev.ID
	}
	want := map[string]string{"cab-a": "cab-a-5", "cab-b": "cab-b-5"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("latest = %v, want %v", got, want)
	}

	// A status after locations still starts at seq 1.
	if seq := mustPublish(t, l, NewEvent(topic, "s-1", "cab-a", domain.KindStatus)); seq != 1 {
		t.Fatalf("first status seq = %d, want 1", seq)
	}
}

func testSnapshot(t *testing.T, newLog Factory) {
	l := newLog(t, 100)
	topic := NewTopic(t)

	mustPublish(t, l, NewEvent(topic, "a-1", "cab-a", domain.KindStatus))
	mustPublish(t, l, NewEvent(topic, "b-1", "cab-b", domain.KindStatus))
	mustPublish(t, l, NewEvent(topic, "a-sos", "cab-a", domain.KindSOS))
	mustPublish(t, l, NewEvent(topic, "a-2", "cab-a", domain.KindStatus))
	mustPublish(t, l, NewEvent(topic, "loc", "cab-a", domain.KindLocation))

	snap, err := l.Snapshot(context.Background(), topic)
	if err != nil {
		t.Fatal(err)
	}
	if snap.AsOfSeq != 4 {
		t.Fatalf("AsOfSeq = %d, want 4", snap.AsOfSeq)
	}
	var ids []string
	for _, ev := range snap.Events {
		ids = append(ids, ev.ID)
	}
	if got, want := strings.Join(ids, ","), "b-1,a-sos,a-2"; got != want {
		t.Fatalf("snapshot = %s, want %s (newest per cab and kind, by seq)", got, want)
	}
}

func testTopicsIndependent(t *testing.T, newLog Factory) {
	l := newLog(t, 100)
	a, b := NewTopic(t), NewTopic(t)

	mustPublish(t, l, NewEvent(a, "e-1", "cab-1", domain.KindStatus))
	mustPublish(t, l, NewEvent(a, "e-2", "cab-1", domain.KindStatus))
	// Same eventId on another topic is a different event there.
	if seq := mustPublish(t, l, NewEvent(b, "e-1", "cab-1", domain.KindStatus)); seq != 1 {
		t.Fatalf("topic b first seq = %d, want 1", seq)
	}
	checkContiguous(t, mustAfter(t, l, a, 0), 1, 2)
	checkContiguous(t, mustAfter(t, l, b, 0), 1, 1)
}

func testEmptyTopic(t *testing.T, newLog Factory) {
	ctx := context.Background()
	l := newLog(t, 100)
	topic := NewTopic(t)

	if first, err := l.FirstSeq(ctx, topic); err != nil || first != 0 {
		t.Fatalf("FirstSeq = (%d, %v), want (0, nil)", first, err)
	}
	if last, err := l.LastSeq(ctx, topic); err != nil || last != 0 {
		t.Fatalf("LastSeq = (%d, %v), want (0, nil)", last, err)
	}
	if evs := mustAfter(t, l, topic, 0); len(evs) != 0 {
		t.Fatalf("After = %d events, want 0", len(evs))
	}
	if locs, err := l.LatestLocation(ctx, topic); err != nil || len(locs) != 0 {
		t.Fatalf("LatestLocation = (%v, %v), want empty", locs, err)
	}
	if snap, err := l.Snapshot(ctx, topic); err != nil || snap.AsOfSeq != 0 || len(snap.Events) != 0 {
		t.Fatalf("Snapshot = (%+v, %v), want empty", snap, err)
	}
}

func testRejectsInvalid(t *testing.T, newLog Factory) {
	l := newLog(t, 100)
	ev := NewEvent(NewTopic(t), "", "cab-1", domain.KindStatus)
	if _, _, err := l.Publish(context.Background(), ev); !errors.Is(err, domain.ErrBadEvent) {
		t.Fatalf("err = %v, want ErrBadEvent", err)
	}
}
