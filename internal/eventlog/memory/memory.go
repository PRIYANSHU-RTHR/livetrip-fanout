// Package memory is an in-process eventlog.Log for tests and single-node
// runs. It follows the same rules as the Redis implementation: per-topic
// counters, a capped log and an eventId dedup window.
package memory

import (
	"bytes"
	"context"
	"sort"
	"sync"
	"time"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/eventlog"
)

const (
	DefaultMaxLen   = 1000
	DefaultDedupTTL = 10 * time.Minute
)

// Config tunes a Log. Zero values take the defaults.
type Config struct {
	// MaxLen is how many reliable events are kept per topic (Redis MAXLEN).
	MaxLen int
	// DedupTTL is how long an eventId is remembered per topic.
	DedupTTL time.Duration
	// Now is the clock, replaceable in tests.
	Now func() time.Time
}

// Log implements eventlog.Log. Each topic has its own lock, so publishes to
// different topics do not contend.
type Log struct {
	maxLen   int
	dedupTTL time.Duration
	now      func() time.Time

	mu     sync.RWMutex
	topics map[domain.Topic]*topicLog
}

var _ eventlog.Log = (*Log)(nil)

func New(cfg Config) *Log {
	if cfg.MaxLen <= 0 {
		cfg.MaxLen = DefaultMaxLen
	}
	if cfg.DedupTTL <= 0 {
		cfg.DedupTTL = DefaultDedupTTL
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Log{
		maxLen:   cfg.MaxLen,
		dedupTTL: cfg.DedupTTL,
		now:      cfg.Now,
		topics:   make(map[domain.Topic]*topicLog),
	}
}

type stateKey struct {
	cabID string
	kind  domain.EventKind
}

type dedupEntry struct {
	id string
	at time.Time // expiry
}

type topicLog struct {
	mu      sync.Mutex
	lastSeq uint64
	lastVer uint64

	// ring holds the last count reliable events; the oldest is at start.
	ring  []domain.Event
	start int
	count int

	latest map[string]domain.Event   // cabID -> newest location
	state  map[stateKey]domain.Event // newest reliable per (cab, kind)

	// seen maps eventId -> assigned position. The TTL is fixed, so entries
	// expire in insertion order and a FIFO is enough to sweep them.
	seen   map[string]uint64
	expiry []dedupEntry
}

func (l *Log) Publish(ctx context.Context, ev domain.Event) (uint64, bool, error) {
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	if err := ev.Validate(); err != nil {
		return 0, false, err
	}

	tl := l.topic(ev.Topic, true)
	tl.mu.Lock()
	defer tl.mu.Unlock()

	now := l.now()
	tl.sweep(now)
	if pos, ok := tl.seen[ev.ID]; ok {
		return pos, true, nil
	}

	ev.Payload = bytes.Clone(ev.Payload)
	var pos uint64
	if ev.Kind.Reliable() {
		tl.lastSeq++
		ev.Seq, ev.Ver = tl.lastSeq, 0
		tl.append(ev, l.maxLen)
		tl.state[stateKey{ev.CabID, ev.Kind}] = ev
		pos = ev.Seq
	} else {
		tl.lastVer++
		ev.Seq, ev.Ver = 0, tl.lastVer
		tl.latest[ev.CabID] = ev
		pos = ev.Ver
	}

	tl.seen[ev.ID] = pos
	tl.expiry = append(tl.expiry, dedupEntry{id: ev.ID, at: now.Add(l.dedupTTL)})
	return pos, false, nil
}

func (l *Log) After(ctx context.Context, topic domain.Topic, seq uint64) ([]domain.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	tl := l.topic(topic, false)
	if tl == nil {
		return nil, nil
	}
	tl.mu.Lock()
	defer tl.mu.Unlock()

	if seq >= tl.lastSeq {
		return nil, nil
	}
	first := tl.firstSeq()
	if seq+1 < first {
		return nil, eventlog.ErrTrimmed
	}
	skip := int(seq + 1 - first)
	out := make([]domain.Event, 0, tl.count-skip)
	for i := skip; i < tl.count; i++ {
		out = append(out, tl.ring[(tl.start+i)%len(tl.ring)])
	}
	return out, nil
}

func (l *Log) FirstSeq(ctx context.Context, topic domain.Topic) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	tl := l.topic(topic, false)
	if tl == nil {
		return 0, nil
	}
	tl.mu.Lock()
	defer tl.mu.Unlock()
	return tl.firstSeq(), nil
}

func (l *Log) LastSeq(ctx context.Context, topic domain.Topic) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	tl := l.topic(topic, false)
	if tl == nil {
		return 0, nil
	}
	tl.mu.Lock()
	defer tl.mu.Unlock()
	return tl.lastSeq, nil
}

func (l *Log) LatestLocation(ctx context.Context, topic domain.Topic) ([]domain.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	tl := l.topic(topic, false)
	if tl == nil {
		return nil, nil
	}
	tl.mu.Lock()
	out := make([]domain.Event, 0, len(tl.latest))
	for _, ev := range tl.latest {
		out = append(out, ev)
	}
	tl.mu.Unlock()

	sort.Slice(out, func(i, j int) bool { return out[i].Ver < out[j].Ver })
	return out, nil
}

func (l *Log) Snapshot(ctx context.Context, topic domain.Topic) (eventlog.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return eventlog.Snapshot{}, err
	}
	tl := l.topic(topic, false)
	if tl == nil {
		return eventlog.Snapshot{}, nil
	}
	tl.mu.Lock()
	snap := eventlog.Snapshot{
		AsOfSeq: tl.lastSeq,
		Events:  make([]domain.Event, 0, len(tl.state)),
	}
	for _, ev := range tl.state {
		snap.Events = append(snap.Events, ev)
	}
	tl.mu.Unlock()

	sort.Slice(snap.Events, func(i, j int) bool { return snap.Events[i].Seq < snap.Events[j].Seq })
	return snap, nil
}

// topic returns the log for t, creating it if create is set. Reads of an
// unknown topic return nil instead of allocating.
func (l *Log) topic(t domain.Topic, create bool) *topicLog {
	l.mu.RLock()
	tl := l.topics[t]
	l.mu.RUnlock()
	if tl != nil || !create {
		return tl
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if tl = l.topics[t]; tl == nil {
		tl = &topicLog{
			ring:   make([]domain.Event, l.maxLen),
			latest: make(map[string]domain.Event),
			state:  make(map[stateKey]domain.Event),
			seen:   make(map[string]uint64),
		}
		l.topics[t] = tl
	}
	return tl
}

// append adds ev to the ring, overwriting the oldest entry when full.
func (tl *topicLog) append(ev domain.Event, maxLen int) {
	if tl.count < maxLen {
		tl.ring[(tl.start+tl.count)%maxLen] = ev
		tl.count++
		return
	}
	tl.ring[tl.start] = ev
	tl.start = (tl.start + 1) % maxLen
}

func (tl *topicLog) firstSeq() uint64 {
	if tl.count == 0 {
		return 0
	}
	return tl.lastSeq - uint64(tl.count) + 1
}

// sweep forgets eventIds whose dedup window has passed.
func (tl *topicLog) sweep(now time.Time) {
	n := 0
	for n < len(tl.expiry) && !now.Before(tl.expiry[n].at) {
		delete(tl.seen, tl.expiry[n].id)
		n++
	}
	if n > 0 {
		tl.expiry = append(tl.expiry[:0], tl.expiry[n:]...)
	}
}
