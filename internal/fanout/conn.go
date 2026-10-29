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
// Enqueue never blocks and never does I/O; Run is the only goroutine that
// touches the Sink.
type Conn struct {
	sink     Sink
	reliable chan domain.Event
	locReady chan struct{}

	mu      sync.Mutex
	latest  map[string]domain.Event // cabID -> newest by Ver
	pending []string                // cabIDs with an unsent location, FIFO
	dirty   map[string]bool         // cabIDs currently in pending

	done      chan struct{}
	closeOnce sync.Once
	cause     error // set once before done is closed
}

func NewConn(sink Sink) *Conn {
	return &Conn{
		sink:     sink,
		reliable: make(chan domain.Event, ReliableBuffer),
		locReady: make(chan struct{}, 1),
		latest:   make(map[string]domain.Event),
		dirty:    make(map[string]bool),
		done:     make(chan struct{}),
	}
}

// Enqueue queues ev for this client without blocking. It returns
// ErrSlowConsumer if the reliable lane was full (the connection is then
// closed) and ErrClosed if the connection was already closed.
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

	select {
	case c.reliable <- ev:
		return nil
	default:
		c.Close(ErrSlowConsumer)
		return ErrSlowConsumer
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
// ends or a write fails. Reliable events always go before locations, and
// at most one location is written between checks of the reliable lane.
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
