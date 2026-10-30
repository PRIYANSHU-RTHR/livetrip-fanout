// Package routing brings events from the broker to this gateway node.
//
// The node subscribes in the broker only to topics someone on it watches
// (driven by the hub's first/last subscriber hooks). For each topic it
// remembers the last Seq it forwarded, so an event lost on the way (a
// dropped message, a broker reconnect) is noticed and fetched from the log
// instead of silently missing.
package routing

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/fanout"
)

const DefaultTimeout = 2 * time.Second

// DefaultMaxRepair is the longest gap replayed through the hub. A longer
// one would overflow every subscriber's reliable lane anyway, so they are
// told to resume instead.
const DefaultMaxRepair = fanout.ReliableBuffer

var errGapTooLong = errors.New("gap longer than max repair")

// Hub is where the router delivers. *fanout.Hub implements it.
type Hub interface {
	Publish(ev domain.Event) int
	CloseTopic(topic domain.Topic, cause error) int
}

// Bus is one pub/sub connection to the broker.
type Bus interface {
	// Subscribe returns once the broker confirmed the subscription, so
	// every event published after it returns will arrive.
	Subscribe(ctx context.Context, topic domain.Topic) error
	Unsubscribe(ctx context.Context, topic domain.Topic) error
}

// Log is the part of eventlog.Log the router reads.
type Log interface {
	After(ctx context.Context, topic domain.Topic, seq uint64) ([]domain.Event, error)
	LastSeq(ctx context.Context, topic domain.Topic) (uint64, error)
	LatestLocation(ctx context.Context, topic domain.Topic) ([]domain.Event, error)
}

// Config tunes a Router. Zero values take the defaults.
type Config struct {
	// Timeout bounds one broker or log call.
	Timeout time.Duration
	// MaxRepair is the longest gap fetched from the log and replayed.
	MaxRepair int
}

// Router forwards broker events to the hub in Seq order per topic.
//
// SOS topics use their own Bus, so a backlog of location traffic on the
// main connection never sits in front of an SOS.
type Router struct {
	hub       Hub
	log       Log
	main, sos Bus
	logger    *slog.Logger
	timeout   time.Duration
	maxRepair int

	mu     sync.Mutex
	topics map[domain.Topic]*topicState
}

// topicState is one subscribed topic. mu serializes forwarding, so the
// two bus loops and a repair never interleave events of one topic.
type topicState struct {
	mu   sync.Mutex
	last uint64 // highest Seq forwarded or already in the log at subscribe
	// reset is set when a failed resync closed every subscriber. All of
	// them, and anyone attaching later, read the log after that, so the
	// next event starts afresh instead of closing them again.
	reset bool
}

func New(hub Hub, log Log, main, sos Bus, logger *slog.Logger, cfg Config) *Router {
	if cfg.Timeout <= 0 {
		cfg.Timeout = DefaultTimeout
	}
	if cfg.MaxRepair <= 0 {
		cfg.MaxRepair = DefaultMaxRepair
	}
	return &Router{
		hub:       hub,
		log:       log,
		main:      main,
		sos:       sos,
		logger:    logger,
		timeout:   cfg.Timeout,
		maxRepair: cfg.MaxRepair,
		topics:    make(map[domain.Topic]*topicState),
	}
}

func (r *Router) bus(topic domain.Topic) Bus {
	if topic.Kind == domain.TopicSOS {
		return r.sos
	}
	return r.main
}

// OnFirstSubscriber subscribes topic in the broker, waits for the
// confirmation and records the topic's current Seq as the starting point.
// It is the hub's OnFirstSubscriber hook.
//
// Events up to that Seq are not forwarded: the connection being attached
// reads them from the log during its own catch-up, which starts after this
// returns.
func (r *Router) OnFirstSubscriber(topic domain.Topic) error {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()

	bus := r.bus(topic)
	if err := bus.Subscribe(ctx, topic); err != nil {
		r.unsubscribe(bus, topic)
		return fmt.Errorf("routing: subscribe %s: %w", topic, err)
	}
	last, err := r.log.LastSeq(ctx, topic)
	if err != nil {
		r.unsubscribe(bus, topic)
		return fmt.Errorf("routing: last seq of %s: %w", topic, err)
	}

	r.mu.Lock()
	r.topics[topic] = &topicState{last: last}
	r.mu.Unlock()
	return nil
}

// OnLastUnsubscribe forgets topic and unsubscribes it in the broker. It is
// the hub's OnLastUnsubscribe hook.
func (r *Router) OnLastUnsubscribe(topic domain.Topic) {
	r.mu.Lock()
	delete(r.topics, topic)
	r.mu.Unlock()
	r.unsubscribe(r.bus(topic), topic)
}

func (r *Router) unsubscribe(bus Bus, topic domain.Topic) {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()
	if err := bus.Unsubscribe(ctx, topic); err != nil {
		// The broker keeps sending; Deliver drops events of unknown topics.
		r.logger.Warn("broker unsubscribe failed", "topic", topic.String(), "err", err)
	}
}

func (r *Router) state(topic domain.Topic) *topicState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.topics[topic]
}

// Deliver handles one event from the broker. Locations go straight to the
// hub (conflation orders them by Ver). A reliable event already forwarded
// is dropped; one that skips ahead first triggers a repair from the log.
func (r *Router) Deliver(ev domain.Event) {
	st := r.state(ev.Topic)
	if st == nil {
		return // nobody here watches it any more
	}
	if !ev.Kind.Reliable() {
		r.hub.Publish(ev)
		return
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	if ev.Seq <= st.last {
		return // already forwarded, e.g. by a repair
	}
	if ev.Seq > st.last+1 && !st.reset {
		r.repair(ev.Topic, st)
	}
	st.reset = false
	if ev.Seq > st.last {
		r.forward(st, ev)
	}
}

// Resync is called when topic's subscription was restored after the bus
// reconnected: events published while it was down are fetched from the
// log, and the newest location per cab is sent again.
func (r *Router) Resync(topic domain.Topic) {
	st := r.state(topic)
	if st == nil {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if !r.repair(topic, st) {
		st.reset = true
	}

	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()
	locs, err := r.log.LatestLocation(ctx, topic)
	if err != nil {
		// Locations are state, not history: the next one fixes it.
		r.logger.Warn("location refresh failed", "topic", topic.String(), "err", err)
		return
	}
	for _, ev := range locs {
		r.hub.Publish(ev)
	}
}

// repair forwards the events after st.last from the log. If it cannot (log
// down, trimmed, or too many), it closes the topic's subscribers with
// fanout.ErrResync so each resumes from its own last seq, and returns
// false. st.mu must be held.
//
// Closing is safe because the missing events were published before the
// close: a connection attaching afterwards reads them in its catch-up.
func (r *Router) repair(topic domain.Topic, st *topicState) bool {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()

	evs, err := r.log.After(ctx, topic, st.last)
	if err == nil && len(evs) > r.maxRepair {
		err = errGapTooLong
	}
	if err != nil {
		n := r.hub.CloseTopic(topic, fanout.ErrResync)
		r.logger.Warn("gap not repaired, subscribers must resume",
			"topic", topic.String(), "after_seq", st.last, "closed", n, "err", err)
		return false
	}
	for _, ev := range evs {
		if ev.Seq > st.last {
			r.forward(st, ev)
		}
	}
	return true
}

func (r *Router) forward(st *topicState, ev domain.Event) {
	r.hub.Publish(ev)
	st.last = ev.Seq
}
