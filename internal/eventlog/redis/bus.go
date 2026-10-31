package redis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	goredis "github.com/redis/go-redis/v9"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"
)

// BusChannelSize is how many messages may wait between the Redis reader
// and Run. If Run falls behind for longer than go-redis waits, messages are
// dropped; the handler sees the Seq gap and repairs it from the log.
const BusChannelSize = 1024

// ErrBusClosed means the bus was closed and Run stopped.
var ErrBusClosed = errors.New("redis bus closed")

// Handler receives what a Bus reads.
type Handler interface {
	// Deliver is called for every event, in the order Redis sent them.
	Deliver(ev domain.Event)
	// Resync is called when topic was subscribed again after the
	// connection dropped; events published meanwhile were not delivered.
	Resync(topic domain.Topic)
}

// Bus is one Redis pub/sub connection carrying topic channels to a
// Handler. go-redis reconnects and re-subscribes on its own; Bus turns the
// re-subscribe confirmations into Resync calls.
type Bus struct {
	ps     *goredis.PubSub
	logger *slog.Logger

	mu sync.Mutex
	// waiting has the Subscribe calls still waiting for Redis to confirm.
	waiting map[string][]chan struct{}
	// ch is the go-redis message channel, set once Run starts.
	ch <-chan any
}

func NewBus(client goredis.UniversalClient, logger *slog.Logger) *Bus {
	return &Bus{
		ps:      client.Subscribe(context.Background()),
		logger:  logger,
		waiting: make(map[string][]chan struct{}),
	}
}

// Subscribe subscribes to topic and returns once Redis confirmed it. Run
// must be running, since it reads the confirmation.
func (b *Bus) Subscribe(ctx context.Context, topic domain.Topic) error {
	name := Channel(topic)
	ok := make(chan struct{})
	b.mu.Lock()
	b.waiting[name] = append(b.waiting[name], ok)
	b.mu.Unlock()

	if err := b.ps.Subscribe(ctx, name); err != nil {
		b.stopWaiting(name, ok)
		return fmt.Errorf("redis bus: subscribe: %w", err)
	}
	select {
	case <-ok:
		return nil
	case <-ctx.Done():
		b.stopWaiting(name, ok)
		return fmt.Errorf("redis bus: subscribe not confirmed: %w", ctx.Err())
	}
}

func (b *Bus) Unsubscribe(ctx context.Context, topic domain.Topic) error {
	if err := b.ps.Unsubscribe(ctx, Channel(topic)); err != nil {
		return fmt.Errorf("redis bus: unsubscribe: %w", err)
	}
	return nil
}

// Run reads the connection and calls h until ctx ends or the bus is
// closed.
func (b *Bus) Run(ctx context.Context, h Handler) error {
	ch := b.ps.ChannelWithSubscriptions(goredis.WithChannelSize(BusChannelSize))
	b.mu.Lock()
	b.ch = ch
	b.mu.Unlock()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-ch:
			if !ok {
				return ErrBusClosed
			}
			b.handle(msg, h)
		}
	}
}

func (b *Bus) handle(msg any, h Handler) {
	switch m := msg.(type) {
	case *goredis.Subscription:
		// A confirmation nobody waits for is go-redis re-subscribing after
		// a reconnect.
		if m.Kind != "subscribe" || b.confirm(m.Channel) {
			return
		}
		topic, err := domain.ParseTopic(m.Channel)
		if err != nil {
			return
		}
		h.Resync(topic)
	case *goredis.Message:
		ev, err := Decode([]byte(m.Payload))
		if err != nil || ev.Topic.String() != m.Channel {
			b.logger.Warn("redis bus: bad message dropped", "channel", m.Channel)
			return
		}
		h.Deliver(ev)
	}
}

// confirm wakes the Subscribe calls waiting for name and reports whether
// there were any.
func (b *Bus) confirm(name string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	ws := b.waiting[name]
	delete(b.waiting, name)
	for _, ok := range ws {
		close(ok)
	}
	return len(ws) > 0
}

func (b *Bus) stopWaiting(name string, ok chan struct{}) {
	b.mu.Lock()
	defer b.mu.Unlock()
	ws := b.waiting[name]
	for i, w := range ws {
		if w == ok {
			ws = append(ws[:i], ws[i+1:]...)
			break
		}
	}
	if len(ws) == 0 {
		delete(b.waiting, name)
	} else {
		b.waiting[name] = ws
	}
}

// Backlog returns how many messages wait between the Redis reader and Run
// (at most BusChannelSize); 0 before Run starts. A backlog near the cap
// means messages are about to be dropped and repaired from the log.
func (b *Bus) Backlog() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.ch)
}

// Close closes the connection; Run then returns ErrBusClosed.
func (b *Bus) Close() error {
	return b.ps.Close()
}
