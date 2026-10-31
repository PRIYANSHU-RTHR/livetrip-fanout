package handler

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"
)

// heapAndStack is the memory the Go runtime holds for live objects and
// goroutine stacks, after a full collection.
func heapAndStack() (heap, stack uint64) {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapInuse, ms.StackInuse
}

// Opens real websocket connections to a gateway, each subscribed to its
// own topic, and measures what one costs. Clients live in this process
// too, so the number is an upper bound on the server's share.
func TestMemoryPerConnection(t *testing.T) {
	if testing.Short() {
		t.Skip("opens 2,000 sockets")
	}
	const n = 2000
	b := newBroker()
	nd := newNode(t, b, nil, "node-a")
	ops := domain.Principal{Tenant: "acme", UserID: "ops", Role: domain.RoleOperator}
	tok, _, err := nd.tokens.Issue(ops, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	// Small client buffers keep the client side out of the way.
	dialer := websocket.Dialer{ReadBufferSize: 256, WriteBufferSize: 256}

	heap0, stack0 := heapAndStack()
	conns := make([]*websocket.Conn, 0, n)
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()
	for i := range n {
		c, _, err := dialer.Dial(nd.url()+"?token="+tok, nil)
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		conns = append(conns, c)
		if f := read(t, c); f.Type != "hello" {
			t.Fatalf("first frame = %+v, want hello", f)
		}
		subscribeFrom(t, c, fmt.Sprintf("t:acme:cab:c%d", i), 0)
	}
	if got := nd.hub.Topics(); got != n {
		t.Fatalf("hub has %d topics, want %d", got, n)
	}
	heap1, stack1 := heapAndStack()

	heap, stack := float64(heap1-heap0)/n, float64(stack1-stack0)/n
	perConn := heap + stack
	t.Logf("memory per connection: %.1f KB (heap %.1f KB + stack %.1f KB, %d connections, client side included)",
		perConn/1024, heap/1024, stack/1024, n)
	if raceEnabled {
		t.Log("race detector on: the 64 KB limit is checked only without -race")
		return
	}
	if perConn > 64<<10 {
		t.Fatalf("%.1f KB per connection, want under 64 KB", perConn/1024)
	}
}
