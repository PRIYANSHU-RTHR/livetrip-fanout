package fanout

import (
	"context"
	"errors"
	"sync"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"
)

// ReliableBuffer is how many status/SOS events may wait for one client.
// A client that falls this far behind is closed and must resume by seq.
const ReliableBuffer = 64

var (
	// ErrSlowConsumer means the reliable lane was full. The event was not
	// dropped silently: the connection is closed so the client resumes.
	ErrSlowConsumer = errors.New("slow consumer")
	// ErrClosed means the connection is already closed.
	ErrClosed = errors.New("connection closed")
	// ErrResync means this node missed events of a topic and could not
	// recover them. The client reconnects and resumes from its last seq.
	ErrResync = errors.New("resync required")
)

// Sink is where a connection writes. The websocket implementation maps the
// close cause to a close code (ErrSlowConsumer => 4008).
type Sink interface {
	Write(ev domain.Event) error
	Close(cause error) error
}

// Conn is one client with two lanes:
//   - reliable: status and SOS, FIFO, never dropped;
//   - location: conflated, only the newest event per (topic, cab) is kept.
//
// A third, unbuffered catch-up lane carries resume events (see Hold and
// Resume), so a long replay never fills the reliable lane. While a topic is
// held, its locations are conflated as usual but not signalled to Run, so
// none is written before the topic's "subscribed" frame and catch-up.
//
// Enqueue never blocks and never does I/O; Run is the only goroutine that
// touches the Sink.
type Conn struct {
	sink     Sink
	metrics  Metrics
	reliable chan domain.Event
	catchup  chan domain.Event
	locReady chan struct{}

	mu      sync.Mutex
	latest  map[locKey]domain.Event // newest by Ver
	pending []locKey                // keys with an unsent location, FIFO
	dirty   map[locKey]bool         // keys currently in pending or a hold
	// holds has an entry per topic being resumed; its events wait there
	// until Resume has sent the catch-up.
	holds map[domain.Topic]*hold

	done      chan struct{}
	closeOnce sync.Once
	cause     error // set once before done is closed
}

// hold is what waits for one topic during a resume.
type hold struct {
	events []domain.Event // reliable, in arrival order
	locs   []locKey       // keys with an unsent location, moved to pending by Resume
}

func NewConn(sink Sink) *Conn {
	return NewConnWithMetrics(sink, nil)
}

// NewConnWithMetrics is NewConn reporting to m; nil means no metrics.
func NewConnWithMetrics(sink Sink, m Metrics) *Conn {
	if m == nil {
		m = noMetrics{}
	}
	return &Conn{
		sink:     sink,
		metrics:  m,
		reliable: make(chan domain.Event, ReliableBuffer),
		catchup:  make(chan domain.Event),
		locReady: make(chan struct{}, 1),
		latest:   make(map[locKey]domain.Event),
		dirty:    make(map[locKey]bool),
		done:     make(chan struct{}),
	}
}

// Enqueue queues ev for this client without blocking. It returns
// ErrSlowConsumer if the reliable lane (or the topic's hold) was full (the
// connection is then closed) and ErrClosed if the connection was already
// closed.
func (c *Conn) Enqueue(ev domain.Event) error {
	select {
	case <-c.done:
		return ErrClosed
	default:
	}

	if !ev.Kind.Reliable() {
		c.conflate(ev)
		return nil
	}

	c.mu.Lock()
	if h, ok := c.holds[ev.Topic]; ok {
		if len(h.events) >= ReliableBuffer {
			c.mu.Unlock()
			c.Close(ErrSlowConsumer)
			return ErrSlowConsumer
		}
		h.events = append(h.events, ev)
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	select {
	case c.reliable <- ev:
		c.metrics.ReliableQueued(len(c.reliable))
		return nil
	default:
		c.Close(ErrSlowConsumer)
		return ErrSlowConsumer
	}
}

// Hold starts a resume of topic: from now on its reliable events are kept
// aside (at most ReliableBuffer) instead of queued, and its locations are
// kept but not written. Call it before the connection is attached to the
// topic, then read the catch-up from the log and call Resume, or CancelHold
// if the subscribe fails.
func (c *Conn) Hold(topic domain.Topic) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.holds == nil {
		c.holds = make(map[domain.Topic]*hold)
	}
	if _, ok := c.holds[topic]; ok {
		return
	}
	h := &hold{}
	// A location still queued from an earlier subscription waits too.
	kept := c.pending[:0]
	for _, k := range c.pending {
		if k.topic == topic {
			h.locs = append(h.locs, k)
		} else {
			kept = append(kept, k)
		}
	}
	clear(c.pending[len(kept):])
	c.pending = kept
	c.holds[topic] = h
}

// CancelHold ends a hold and discards what it kept.
func (c *Conn) CancelHold(topic domain.Topic) {
	c.mu.Lock()
	if h, ok := c.holds[topic]; ok {
		for _, k := range h.locs {
			delete(c.dirty, k)
		}
		delete(c.holds, topic)
	}
	c.mu.Unlock()
}

// Resume sends catchup (reliable events from the log, in order), then the
// held live events with Seq > asOf and not already sent, then ends the
// hold so later events take the reliable lane again and the held locations
// are written. asOf is the topic Seq the catch-up brings the client to.
//
// Unlike Enqueue it blocks until Run has taken each event, so it must not
// be called from Run. It returns ErrClosed if the connection closes and
// ctx.Err() if ctx ends first.
func (c *Conn) Resume(ctx context.Context, topic domain.Topic, catchup []domain.Event, asOf uint64) error {
	last := asOf
	for _, ev := range catchup {
		if err := c.push(ctx, ev); err != nil {
			return err
		}
		last = max(last, ev.Seq)
	}
	for {
		c.mu.Lock()
		h := c.holds[topic]
		if h == nil || len(h.events) == 0 {
			// The catch-up lane is unbuffered, so everything sent so far
			// is already in Run's hands: nothing queued after this point,
			// held locations included, can overtake it.
			delete(c.holds, topic)
			var locs bool
			if h != nil && len(h.locs) > 0 {
				c.pending = append(c.pending, h.locs...)
				locs = true
			}
			c.mu.Unlock()
			if locs {
				c.signalLocation()
			}
			return nil
		}
		held := h.events
		h.events = nil
		c.mu.Unlock()

		for _, ev := range held {
			if ev.Seq <= last {
				continue // also in the catch-up
			}
			if err := c.push(ctx, ev); err != nil {
				return err
			}
			last = ev.Seq
		}
	}
}

// push hands ev to Run through the catch-up lane.
func (c *Conn) push(ctx context.Context, ev domain.Event) error {
	select {
	case c.catchup <- ev:
		return nil
	case <-c.done:
		return ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

// locKey names one conflation slot. Ver is a per-topic counter, so a cab
// seen on two topics has two slots whose versions are not comparable.
type locKey struct {
	topic domain.Topic
	cabID string
}

func (c *Conn) conflate(ev domain.Event) {
	k := locKey{ev.Topic, ev.CabID}
	c.mu.Lock()
	if cur, ok := c.latest[k]; ok && ev.Ver <= cur.Ver {
		c.mu.Unlock()
		return // stale or duplicate
	}
	c.latest[k] = ev
	replaced := c.dirty[k]
	h, held := c.holds[ev.Topic]
	if !replaced {
		c.dirty[k] = true
		if held {
			h.locs = append(h.locs, k)
		} else {
			c.pending = append(c.pending, k)
		}
	}
	c.mu.Unlock()
	if replaced {
		c.metrics.LocationConflated()
	}
	if !held {
		c.signalLocation() // Resume signals held ones
	}
}

func (c *Conn) signalLocation() {
	select {
	case c.locReady <- struct{}{}:
	default: // a signal is already waiting
	}
}

// nextLocation pops the oldest pending (topic, cab) and returns its newest
// location. more reports whether others are still pending.
func (c *Conn) nextLocation() (ev domain.Event, ok, more bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.pending) == 0 {
		return domain.Event{}, false, false
	}
	k := c.pending[0]
	c.pending[0] = locKey{}
	c.pending = c.pending[1:]
	delete(c.dirty, k)
	return c.latest[k], true, len(c.pending) > 0
}

// Run writes queued events to the sink until the connection closes, ctx
// ends or a write fails. Reliable events always go before catch-up and
// locations, and at most one catch-up or location event is written between
// checks of the reliable lane.
// It closes the sink on return and returns the close cause.
func (c *Conn) Run(ctx context.Context) error {
	defer func() { _ = c.sink.Close(c.cause) }()

	for {
		// Step 1: drain the reliable lane first.
		select {
		case <-c.done:
			return c.cause
		case ev := <-c.reliable:
			if err := c.writeLive(ev); err != nil {
				return err
			}
			continue
		default:
		}

		// Step 2: nothing reliable is waiting, so wait for anything.
		select {
		case <-ctx.Done():
			c.Close(ctx.Err())
			return c.cause
		case <-c.done:
			return c.cause
		case ev := <-c.reliable:
			if err := c.writeLive(ev); err != nil {
				return err
			}
		case ev := <-c.catchup:
			if err := c.write(ev); err != nil {
				return err
			}
		case <-c.locReady:
			ev, ok, more := c.nextLocation()
			if more {
				c.signalLocation()
			}
			if ok {
				if err := c.writeLive(ev); err != nil {
					return err
				}
			}
		}
	}
}

// writeLive writes an event that came from the live lanes and reports it.
func (c *Conn) writeLive(ev domain.Event) error {
	if err := c.write(ev); err != nil {
		return err
	}
	c.metrics.Delivered(ev)
	return nil
}

func (c *Conn) write(ev domain.Event) error {
	if err := c.sink.Write(ev); err != nil {
		c.Close(err)
		return c.cause
	}
	return nil
}

// Close marks the connection closed with cause. Only the first call counts.
// It does no I/O; Run closes the sink.
func (c *Conn) Close(cause error) {
	c.closeOnce.Do(func() {
		c.cause = cause
		close(c.done)
	})
}

// Done is closed when the connection is closed.
func (c *Conn) Done() <-chan struct{} { return c.done }

// Err returns the close cause, or nil while the connection is open.
func (c *Conn) Err() error {
	select {
	case <-c.done:
		return c.cause
	default:
		return nil
	}
}
