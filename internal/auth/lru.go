package auth

import (
	"container/list"
	"sync"
	"time"
)

// ttlLRU is a fixed-size cache. The least recently used entry is evicted
// when full, and an entry older than ttl is treated as missing, so a value
// is never served more than ttl after it was loaded.
type ttlLRU[K comparable, V any] struct {
	mu    sync.Mutex
	cap   int
	ttl   time.Duration
	now   func() time.Time
	order *list.List // front = most recently used
	items map[K]*list.Element
}

type lruEntry[K comparable, V any] struct {
	key     K
	val     V
	expires time.Time
}

func newTTLLRU[K comparable, V any](capacity int, ttl time.Duration, now func() time.Time) *ttlLRU[K, V] {
	return &ttlLRU[K, V]{
		cap:   capacity,
		ttl:   ttl,
		now:   now,
		order: list.New(),
		items: make(map[K]*list.Element, capacity),
	}
}

func (c *ttlLRU[K, V]) get(k K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[k]
	if !ok {
		var zero V
		return zero, false
	}
	e := el.Value.(*lruEntry[K, V])
	if !c.now().Before(e.expires) {
		c.order.Remove(el)
		delete(c.items, k)
		var zero V
		return zero, false
	}
	c.order.MoveToFront(el)
	return e.val, true
}

func (c *ttlLRU[K, V]) put(k K, v V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	expires := c.now().Add(c.ttl)
	if el, ok := c.items[k]; ok {
		e := el.Value.(*lruEntry[K, V])
		e.val, e.expires = v, expires
		c.order.MoveToFront(el)
		return
	}
	c.items[k] = c.order.PushFront(&lruEntry[K, V]{key: k, val: v, expires: expires})
	if c.order.Len() > c.cap {
		oldest := c.order.Back()
		c.order.Remove(oldest)
		delete(c.items, oldest.Value.(*lruEntry[K, V]).key)
	}
}

func (c *ttlLRU[K, V]) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}
