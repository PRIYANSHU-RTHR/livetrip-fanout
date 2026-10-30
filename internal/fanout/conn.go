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
//   - location: conflated, only the newest event per cab is kept.
//
// A third, unbuffered catch-up lane carries resume events (see Hold and
// Resume), so a long replay never fills the reliable lane.
//
// Enqueue never blocks and never does I/O; Run is the only goroutine that
// touches the Sink.
type Conn struct {
	sink     Sink
	reliable chan domain.Event
	catchup  chan domain.Event
	locReady chan struct{}

	mu      sync.Mutex
	latest  map[string]domain.Event // cabID -> newest by Ver
	pending []string                // cabIDs with an unsent location, FIFO
	dirty   map[string]bool         // cabIDs currently in pending
	// holds has an entry per topic being resumed; its reliable events
	// wait there until Resume has sent the catch-up.
	holds map[domain.Topic][]domain.Event

	done      chan struct{}
	closeOnce sync.Once
	cause     error // set once before done is closed
}

func NewConn(sink Sink) *Conn {
	return &Conn{
		sink:     sink,
		reliable: make(chan domain.Event, ReliableBuffer),
		catchup:  make(chan domain.Event),
		locReady: make(chan struct{}, 1),
		latest:   make(map[string]domain.Event),
		dirty:    make(map[string]bool),
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
	if held, ok := c.holds[ev.Topic]; ok {
		if len(held) >= ReliableBuffer {
			c.mu.Unlock()
			c.Close(ErrSlowConsumer)
			return ErrSlowConsumer
		}
		c.holds[ev.Topic] = append(held, ev)
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	select {
	case c.reliable <- ev:
		return nil
	default:
		c.Close(ErrSlowConsumer)
		return ErrSlowConsumer
	}
}

// Hold starts a resume of topic: from now on its reliable events are kept
// aside (at most ReliableBuffer) instead of queued. Call it before the
// connection is attached to the topic, then read the catch-up from the log
// and call Resume, or CancelHold if the subscribe fails.
func (c *Conn) Hold(topic domain.Topic) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.holds == nil {
		c.holds = make(map[domain.Topic][]domain.Event)
	}
	if _, ok := c.holds[topic]; !ok {
		c.holds[topic] = nil
	}
}

// CancelHold ends a hold and discards what it kept.
func (c *Conn) CancelHold(topic domain.Topic) {
	c.mu.Lock()
	delete(c.holds, topic)
	c.mu.Unlock()
}

// Resume sends catchup (reliable events from the log, in order), then the
// held live events with Seq > asOf and not already sent, then ends the
// hold so later events take the reliable lane again. asOf is the topic Seq
// the catch-up brings the client to.
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
		held := c.holds[topic]
		if len(held) == 0 {
			// The catch-up lane is unbuffered, so everything sent so far
			// is already in Run's hands: nothing queued after this point
			// can overtake it.
			delete(c.holds, topic)
			c.mu.Unlock()
			return nil
		}
		c.holds[topic] = nil
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

func (c *Conn) conflate(ev domain.Event) {
	c.mu.Lock()
	if cur, ok := c.latest[ev.CabID]; ok && ev.Ver <= cur.Ver {
		c.mu.Unlock()
		return // stale or duplicate
	}
	c.latest[ev.CabID] = ev
	if !c.dirty[ev.CabID] {
		c.dirty[ev.CabID] = true
		c.pending = append(c.pending, ev.CabID)
	}
	c.mu.Unlock()
	c.signalLocation()
}

func (c *Conn) signalLocation() {
	select {
	case c.locReady <- struct{}{}:
	default: // a signal is already waiting
	}
}

// nextLocation pops the oldest pending cab and returns its newest location.
// more reports whether other cabs are still pending.
func (c *Conn) nextLocation() (ev domain.Event, ok, more bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.pending) == 0 {
		return domain.Event{}, false, false
	}
	cab := c.pending[0]
	c.pending[0] = ""
	c.pending = c.pending[1:]
	delete(c.dirty, cab)
	return c.latest[cab], true, len(c.pending) > 0
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
			if err := c.write(ev); err != nil {
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
			if err := c.write(ev); err != nil {
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
				if err := c.write(ev); err != nil {
					return err
				}
			}
		}
	}
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
