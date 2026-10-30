package main

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// frame is any server message; fields are filled by type.
type frame struct {
	Type      string    `json:"type"`
	Topic     string    `json:"topic"`
	Resume    string    `json:"resume"`
	AsOfSeq   uint64    `json:"asOfSeq"`
	Code      string    `json:"code"`
	EventID   string    `json:"eventId"`
	CabID     string    `json:"cabId"`
	Kind      string    `json:"kind"`
	Seq       uint64    `json:"seq"`
	Ver       uint64    `json:"ver"`
	CreatedAt time.Time `json:"createdAt"`
}

// clientStats is shared by every client.
type clientStats struct {
	latency map[string]*latencies // by kind, createdAt → received
	sosDesk latencies             // SOS: send → each desk receives it
	sent    *sync.Map             // SOS eventId → send time

	frames      atomic.Int64
	connected   atomic.Int64 // currently subscribed
	dialFailed  atomic.Int64
	disconnects atomic.Int64
	reconnects  atomic.Int64
	reconnect   latencies // disconnect → subscribed again

	mu      sync.Mutex
	closes  map[string]int // why connections ended
	errored map[string]int // error replies from the server
}

func newClientStats(sent *sync.Map) *clientStats {
	return &clientStats{
		latency: map[string]*latencies{"location": {}, "status": {}, "sos": {}},
		sent:    sent,
		closes:  map[string]int{},
		errored: map[string]int{},
	}
}

func (s *clientStats) inc(m map[string]int, key string) {
	s.mu.Lock()
	m[key]++
	s.mu.Unlock()
}

// client is one screen: it watches one topic, reconnects through the load
// balancer when its connection drops and resumes from its last seq.
type client struct {
	url   string
	token string
	topic string
	desk  bool
	stats *clientStats
	seq   *seqTracker

	// done is closed after the client's last connection ended.
	done chan struct{}
	// last mirrors seq.last for the verifier, which runs on another goroutine.
	last atomic.Uint64
	up   atomic.Bool
}

var dialer = websocket.Dialer{
	HandshakeTimeout: 5 * time.Second,
	ReadBufferSize:   2048,
	WriteBufferSize:  512,
}

// run keeps the client subscribed until ctx ends. ready is called once,
// after the first subscribe (or the first failure to get there).
func (c *client) run(ctx context.Context, ready func(ok bool)) {
	defer close(c.done)
	first := true
	backoff := 100 * time.Millisecond
	var downAt time.Time
	for ctx.Err() == nil {
		ws, err := c.connect(ctx)
		if err != nil {
			c.stats.dialFailed.Add(1)
			if first {
				ready(false)
				first = false
			}
			if !sleep(ctx, backoff+rand.N(backoff)) {
				return
			}
			backoff = min(backoff*2, 3*time.Second)
			continue
		}
		backoff = 100 * time.Millisecond
		reason := c.read(ctx, ws, func() {
			c.up.Store(true)
			c.stats.connected.Add(1)
			if first {
				ready(true)
				first = false
			} else if !downAt.IsZero() {
				c.stats.reconnects.Add(1)
				c.stats.reconnect.add(time.Since(downAt))
			}
		})
		if c.up.Swap(false) {
			c.stats.connected.Add(-1)
		}
		ws.Close()
		if ctx.Err() != nil {
			return
		}
		c.stats.disconnects.Add(1)
		c.stats.inc(c.stats.closes, reason)
		downAt = time.Now()
		// Jitter so a dead node's clients don't all come back at once.
		if !sleep(ctx, rand.N(500*time.Millisecond)) {
			return
		}
	}
}

func (c *client) connect(ctx context.Context) (*websocket.Conn, error) {
	h := http.Header{"Authorization": {"Bearer " + c.token}}
	ws, resp, err := dialer.DialContext(ctx, c.url, h)
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
	if err != nil {
		return nil, err
	}
	sub := map[string]any{"op": "subscribe", "topic": c.topic, "lastSeq": c.seq.last}
	if err := ws.WriteJSON(sub); err != nil {
		ws.Close()
		return nil, err
	}
	return ws, nil
}

// read handles frames until the connection fails; it returns why.
func (c *client) read(ctx context.Context, ws *websocket.Conn, subscribed func()) string {
	stop := context.AfterFunc(ctx, func() {
		_ = ws.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
		ws.Close()
	})
	defer stop()
	for {
		_, data, err := ws.ReadMessage()
		if err != nil {
			return closeReason(err)
		}
		now := time.Now()
		var f frame
		if json.Unmarshal(data, &f) != nil {
			c.stats.inc(c.stats.errored, "bad_frame")
			continue
		}
		switch f.Type {
		case "subscribed":
			c.seq.subscribed(f.Resume, f.AsOfSeq)
			c.last.Store(c.seq.last)
			subscribed()
		case "event":
			c.event(f, now)
		case "error":
			c.stats.inc(c.stats.errored, f.Code)
		}
	}
}

func (c *client) event(f frame, now time.Time) {
	c.stats.frames.Add(1)
	fresh := false
	if f.Kind == "location" {
		fresh = c.seq.location(f.CabID, f.Ver)
	} else {
		fresh = c.seq.reliable(f.Seq)
		c.last.Store(c.seq.last)
	}
	if !fresh {
		return // duplicate or catch-up already counted
	}
	if l := c.stats.latency[f.Kind]; l != nil {
		l.add(now.Sub(f.CreatedAt))
	}
	if f.Kind == "sos" && c.desk {
		if v, ok := c.stats.sent.Load(f.EventID); ok {
			c.stats.sosDesk.add(now.Sub(v.(time.Time)))
		}
	}
}

func closeReason(err error) string {
	// 1006 means no close frame: the node or the network went away.
	var ce *websocket.CloseError
	if errors.As(err, &ce) && ce.Code != websocket.CloseAbnormalClosure {
		return strconv.Itoa(ce.Code)
	}
	return "dropped"
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
