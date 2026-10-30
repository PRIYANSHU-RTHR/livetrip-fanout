package fanout

import (
	"sync"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"
)

// Hooks are called when a topic gains its first subscriber or loses its
// last one on this node. The gateway uses them to SUBSCRIBE/UNSUBSCRIBE in
// Redis, so a node only receives topics someone here is watching.
//
// Hooks run one at a time and in the order the changes happened, but never
// while Publish is blocked. They must not call back into the Hub.
//
// If OnFirstSubscriber fails, Attach undoes the attach and returns its
// error; OnLastUnsubscribe is then not called for that topic.
type Hooks struct {
	OnFirstSubscriber func(domain.Topic) error
	OnLastUnsubscribe func(domain.Topic)
}

// Hub maps topics to the connections subscribed to them on this node.
//
// Publish only enqueues: it never does I/O and never waits for a client,
// so one frozen connection cannot delay delivery to anyone else.
type Hub struct {
	hooks Hooks

	// subMu serializes Attach/Detach so hooks fire in order. It is held
	// while a hook runs; mu is not, so Publish keeps going.
	subMu sync.Mutex

	mu     sync.RWMutex
	topics map[domain.Topic]map[*Conn]struct{}
	conns  map[*Conn]map[domain.Topic]struct{}
}

func NewHub(hooks Hooks) *Hub {
	return &Hub{
		hooks:  hooks,
		topics: make(map[domain.Topic]map[*Conn]struct{}),
		conns:  make(map[*Conn]map[domain.Topic]struct{}),
	}
}

// Attach subscribes c to topic. Attaching twice is a no-op. It returns
// ErrClosed if c is already closed, or the OnFirstSubscriber error.
func (h *Hub) Attach(topic domain.Topic, c *Conn) error {
	if c.Err() != nil {
		return ErrClosed
	}

	h.subMu.Lock()
	defer h.subMu.Unlock()

	h.mu.Lock()
	subs, ok := h.topics[topic]
	first := !ok
	if first {
		subs = make(map[*Conn]struct{})
		h.topics[topic] = subs
	}
	subs[c] = struct{}{}
	if h.conns[c] == nil {
		h.conns[c] = make(map[domain.Topic]struct{})
	}
	h.conns[c][topic] = struct{}{}
	h.mu.Unlock()

	if first && h.hooks.OnFirstSubscriber != nil {
		if err := h.hooks.OnFirstSubscriber(topic); err != nil {
			h.mu.Lock()
			h.remove(topic, c)
			h.mu.Unlock()
			return err
		}
	}
	return nil
}

// Detach unsubscribes c from topic. Detaching something not attached is a
// no-op.
func (h *Hub) Detach(topic domain.Topic, c *Conn) {
	h.subMu.Lock()
	defer h.subMu.Unlock()

	h.mu.Lock()
	last := h.remove(topic, c)
	h.mu.Unlock()

	if last && h.hooks.OnLastUnsubscribe != nil {
		h.hooks.OnLastUnsubscribe(topic)
	}
}

// DetachAll unsubscribes c from every topic. Call it when the connection
// ends.
func (h *Hub) DetachAll(c *Conn) {
	h.subMu.Lock()
	defer h.subMu.Unlock()

	h.mu.Lock()
	var emptied []domain.Topic
	for topic := range h.conns[c] {
		if h.remove(topic, c) {
			emptied = append(emptied, topic)
		}
	}
	h.mu.Unlock()

	if h.hooks.OnLastUnsubscribe != nil {
		for _, topic := range emptied {
			h.hooks.OnLastUnsubscribe(topic)
		}
	}
}

// remove deletes one subscription and reports whether the topic is now
// empty. h.mu must be held for writing.
func (h *Hub) remove(topic domain.Topic, c *Conn) (last bool) {
	subs, ok := h.topics[topic]
	if !ok {
		return false
	}
	if _, ok := subs[c]; !ok {
		return false
	}
	delete(subs, c)
	if ts := h.conns[c]; ts != nil {
		delete(ts, topic)
		if len(ts) == 0 {
			delete(h.conns, c)
		}
	}
	if len(subs) == 0 {
		delete(h.topics, topic)
		return true
	}
	return false
}

// Publish enqueues ev on every connection subscribed to ev.Topic and
// returns how many accepted it. A connection whose reliable lane is full
// is closed by Enqueue; its owner detaches it when Run returns, so Publish
// never changes subscriptions or fires hooks.
func (h *Hub) Publish(ev domain.Event) int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	n := 0
	for c := range h.topics[ev.Topic] {
		if c.Enqueue(ev) == nil {
			n++
		}
	}
	return n
}

// CloseTopic closes every connection subscribed to topic with cause and
// returns how many it closed. Like Publish it does no I/O and leaves
// detaching to each connection's owner.
func (h *Hub) CloseTopic(topic domain.Topic, cause error) int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for c := range h.topics[topic] {
		c.Close(cause)
	}
	return len(h.topics[topic])
}

// Subscribers returns how many connections are subscribed to topic.
func (h *Hub) Subscribers(topic domain.Topic) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.topics[topic])
}

// Topics returns how many topics have at least one subscriber.
func (h *Hub) Topics() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.topics)
}
