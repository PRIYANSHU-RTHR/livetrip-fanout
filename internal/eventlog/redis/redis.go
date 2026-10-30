// Package redis is the eventlog.Log used in production. A Lua script does
// dedup, sequencing, storage and PUBLISH in one atomic step, so every
// gateway node sees the same gap-free Seq per topic.
//
// Keys per topic, all hash-tagged with the topic so they live in one
// cluster slot and one script can touch them all:
//
//	{t:acme:trip:42}:seq          reliable counter
//	{t:acme:trip:42}:ver          location counter
//	{t:acme:trip:42}:log          stream, entry ID "<seq>-0", MAXLEN ~
//	{t:acme:trip:42}:state        hash "cabId|kind" -> newest reliable event
//	{t:acme:trip:42}:latest       hash cabId -> newest location
//	{t:acme:trip:42}:dedup:<id>   eventId -> pos, expires after DedupTTL
package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/eventlog"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/scripts"
)

const (
	DefaultMaxLen   = 1000
	DefaultDedupTTL = 10 * time.Minute
)

// Config tunes a Log. Zero values take the defaults.
type Config struct {
	// MaxLen is the approximate number of reliable events kept per topic.
	MaxLen int
	// DedupTTL is how long an eventId is remembered per topic.
	DedupTTL time.Duration
}

// Log implements eventlog.Log on Redis.
type Log struct {
	client   goredis.UniversalClient
	maxLen   int
	dedupTTL time.Duration
	publish  *goredis.Script
}

var _ eventlog.Log = (*Log)(nil)

func New(client goredis.UniversalClient, cfg Config) *Log {
	if cfg.MaxLen <= 0 {
		cfg.MaxLen = DefaultMaxLen
	}
	if cfg.DedupTTL <= 0 {
		cfg.DedupTTL = DefaultDedupTTL
	}
	return &Log{
		client:   client,
		maxLen:   cfg.MaxLen,
		dedupTTL: cfg.DedupTTL,
		publish:  goredis.NewScript(scripts.Publish),
	}
}

// topicKeys names a topic's keys in Redis.
type topicKeys struct {
	seq, ver, log, state, latest string
	tag                          string
}

func keysFor(t domain.Topic) topicKeys {
	tag := "{" + t.String() + "}"
	return topicKeys{
		seq:    tag + ":seq",
		ver:    tag + ":ver",
		log:    tag + ":log",
		state:  tag + ":state",
		latest: tag + ":latest",
		tag:    tag,
	}
}

func (k topicKeys) dedup(eventID string) string { return k.tag + ":dedup:" + eventID }

// Channel is the pub/sub channel events of topic are published on.
func Channel(t domain.Topic) string { return t.String() }

func (l *Log) Publish(ctx context.Context, ev domain.Event) (uint64, bool, error) {
	if err := ev.Validate(); err != nil {
		return 0, false, err
	}
	// The script adds seq or ver, so the body is sent without them.
	ev.Seq, ev.Ver = 0, 0
	body, err := json.Marshal(ev)
	if err != nil {
		return 0, false, fmt.Errorf("%w: %w", domain.ErrBadEvent, err)
	}

	k := keysFor(ev.Topic)
	reliable, field := "0", ev.CabID
	if ev.Kind.Reliable() {
		reliable, field = "1", stateField(ev.CabID, ev.Kind)
	}
	res, err := l.publish.Run(ctx, l.client,
		[]string{k.seq, k.ver, k.log, k.state, k.latest, k.dedup(ev.ID)},
		reliable, body, l.maxLen, l.dedupTTL.Milliseconds(), field, Channel(ev.Topic),
	).Int64Slice()
	if err != nil {
		return 0, false, fmt.Errorf("redis eventlog: publish: %w", err)
	}
	if len(res) != 2 || res[0] <= 0 {
		return 0, false, fmt.Errorf("redis eventlog: publish: unexpected script result %v", res)
	}
	return uint64(res[0]), res[1] == 1, nil
}

func (l *Log) After(ctx context.Context, topic domain.Topic, seq uint64) ([]domain.Event, error) {
	k := keysFor(topic)
	var (
		last  *goredis.StringCmd
		head  *goredis.XMessageSliceCmd
		after *goredis.XMessageSliceCmd
	)
	// MULTI/EXEC so the counter, oldest entry and range are read together.
	_, err := l.client.TxPipelined(ctx, func(p goredis.Pipeliner) error {
		last = p.Get(ctx, k.seq)
		head = p.XRangeN(ctx, k.log, "-", "+", 1)
		after = p.XRange(ctx, k.log, strconv.FormatUint(seq+1, 10), "+")
		return nil
	})
	if err != nil && !errors.Is(err, goredis.Nil) {
		return nil, fmt.Errorf("redis eventlog: after: %w", err)
	}

	lastSeq, err := uint64OrZero(last)
	if err != nil {
		return nil, fmt.Errorf("redis eventlog: after: %w", err)
	}
	if seq >= lastSeq {
		return nil, nil
	}
	first, err := firstID(head.Val())
	if err != nil {
		return nil, fmt.Errorf("redis eventlog: after: %w", err)
	}
	if first == 0 || seq+1 < first {
		return nil, eventlog.ErrTrimmed
	}

	msgs := after.Val()
	out := make([]domain.Event, 0, len(msgs))
	for _, m := range msgs {
		ev, err := decodeEntry(m)
		if err != nil {
			return nil, fmt.Errorf("redis eventlog: after: %w", err)
		}
		out = append(out, ev)
	}
	return out, nil
}

func (l *Log) FirstSeq(ctx context.Context, topic domain.Topic) (uint64, error) {
	msgs, err := l.client.XRangeN(ctx, keysFor(topic).log, "-", "+", 1).Result()
	if err != nil {
		return 0, fmt.Errorf("redis eventlog: first seq: %w", err)
	}
	first, err := firstID(msgs)
	if err != nil {
		return 0, fmt.Errorf("redis eventlog: first seq: %w", err)
	}
	return first, nil
}

func (l *Log) LastSeq(ctx context.Context, topic domain.Topic) (uint64, error) {
	last, err := uint64OrZero(l.client.Get(ctx, keysFor(topic).seq))
	if err != nil {
		return 0, fmt.Errorf("redis eventlog: last seq: %w", err)
	}
	return last, nil
}

func (l *Log) LatestLocation(ctx context.Context, topic domain.Topic) ([]domain.Event, error) {
	vals, err := l.client.HVals(ctx, keysFor(topic).latest).Result()
	if err != nil {
		return nil, fmt.Errorf("redis eventlog: latest location: %w", err)
	}
	out, err := decodeAll(vals)
	if err != nil {
		return nil, fmt.Errorf("redis eventlog: latest location: %w", err)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Ver < out[j].Ver })
	return out, nil
}

func (l *Log) Snapshot(ctx context.Context, topic domain.Topic) (eventlog.Snapshot, error) {
	k := keysFor(topic)
	var (
		last  *goredis.StringCmd
		state *goredis.StringSliceCmd
	)
	// MULTI/EXEC so AsOfSeq matches the state that was read.
	_, err := l.client.TxPipelined(ctx, func(p goredis.Pipeliner) error {
		last = p.Get(ctx, k.seq)
		state = p.HVals(ctx, k.state)
		return nil
	})
	if err != nil && !errors.Is(err, goredis.Nil) {
		return eventlog.Snapshot{}, fmt.Errorf("redis eventlog: snapshot: %w", err)
	}

	asOf, err := uint64OrZero(last)
	if err != nil {
		return eventlog.Snapshot{}, fmt.Errorf("redis eventlog: snapshot: %w", err)
	}
	evs, err := decodeAll(state.Val())
	if err != nil {
		return eventlog.Snapshot{}, fmt.Errorf("redis eventlog: snapshot: %w", err)
	}
	sort.Slice(evs, func(i, j int) bool { return evs[i].Seq < evs[j].Seq })
	return eventlog.Snapshot{AsOfSeq: asOf, Events: evs}, nil
}

func stateField(cabID string, kind domain.EventKind) string {
	return cabID + "|" + string(kind)
}

// uint64OrZero reads a counter; a missing key means nothing was published.
func uint64OrZero(cmd *goredis.StringCmd) (uint64, error) {
	v, err := cmd.Uint64()
	if errors.Is(err, goredis.Nil) {
		return 0, nil
	}
	return v, err
}

// firstID returns the Seq of the first stream entry, or 0 if there is none.
func firstID(msgs []goredis.XMessage) (uint64, error) {
	if len(msgs) == 0 {
		return 0, nil
	}
	return parseID(msgs[0].ID)
}

// parseID turns a stream ID "<seq>-0" into seq.
func parseID(id string) (uint64, error) {
	ms, _, ok := strings.Cut(id, "-")
	if !ok {
		return 0, fmt.Errorf("bad stream id %q", id)
	}
	return strconv.ParseUint(ms, 10, 64)
}

func decodeEntry(m goredis.XMessage) (domain.Event, error) {
	raw, ok := m.Values["ev"].(string)
	if !ok {
		return domain.Event{}, fmt.Errorf("stream entry %s has no event", m.ID)
	}
	ev, err := Decode([]byte(raw))
	if err != nil {
		return domain.Event{}, err
	}
	seq, err := parseID(m.ID)
	if err != nil {
		return domain.Event{}, err
	}
	if ev.Seq != seq {
		return domain.Event{}, fmt.Errorf("stream entry %s holds seq %d", m.ID, ev.Seq)
	}
	return ev, nil
}

func decodeAll(vals []string) ([]domain.Event, error) {
	out := make([]domain.Event, 0, len(vals))
	for _, v := range vals {
		ev, err := Decode([]byte(v))
		if err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	return out, nil
}

// Decode parses an event as stored in Redis or received on a channel.
func Decode(b []byte) (domain.Event, error) {
	var ev domain.Event
	if err := json.Unmarshal(b, &ev); err != nil {
		return domain.Event{}, fmt.Errorf("decode event: %w", err)
	}
	return ev, nil
}
