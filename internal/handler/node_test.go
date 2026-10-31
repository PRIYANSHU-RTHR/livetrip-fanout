package handler

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/auth"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/eventlog"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/eventlog/memory"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/fanout"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/routing"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/service"
)

// broker stands in for Redis: one event log shared by every node, and,
// like the publish script, each stored event is sent to the nodes
// subscribed to its topic. mu keeps that delivery in Seq order.
type broker struct {
	*memory.Log
	mu   sync.Mutex
	subs map[domain.Topic]map[*brokerBus]struct{}
}

func newBroker() *broker {
	return &broker{Log: memory.New(memory.Config{}), subs: map[domain.Topic]map[*brokerBus]struct{}{}}
}

func (b *broker) Publish(ctx context.Context, ev domain.Event) (uint64, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	pos, dup, err := b.Log.Publish(ctx, ev)
	if err != nil || dup {
		return pos, dup, err
	}
	if ev.Kind.Reliable() {
		ev.Seq = pos
	} else {
		ev.Ver = pos
	}
	for bus := range b.subs[ev.Topic] {
		if !bus.dead.Load() {
			bus.router.Deliver(ev)
		}
	}
	return pos, dup, nil
}

// brokerBus is one node's routing.Bus on the broker.
type brokerBus struct {
	b      *broker
	router *routing.Router
	dead   atomic.Bool
}

func (bb *brokerBus) Subscribe(_ context.Context, t domain.Topic) error {
	bb.b.mu.Lock()
	defer bb.b.mu.Unlock()
	if bb.b.subs[t] == nil {
		bb.b.subs[t] = map[*brokerBus]struct{}{}
	}
	bb.b.subs[t][bb] = struct{}{}
	return nil
}

func (bb *brokerBus) Unsubscribe(_ context.Context, t domain.Topic) error {
	bb.b.mu.Lock()
	defer bb.b.mu.Unlock()
	delete(bb.b.subs[t], bb)
	return nil
}

// trackingListener remembers accepted sockets so a node can be killed
// without close frames, as if its process died.
type trackingListener struct {
	net.Listener
	mu    sync.Mutex
	conns []net.Conn
}

func (l *trackingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		l.mu.Lock()
		l.conns = append(l.conns, c)
		l.mu.Unlock()
	}
	return c, err
}

func (l *trackingListener) kill() {
	_ = l.Listener.Close()
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, c := range l.conns {
		_ = c.Close()
	}
}

// node is one gateway: hub, router, subscriptions and websocket handler
// wired as in cmd/gateway, on its own httptest server.
type node struct {
	srv    *httptest.Server
	lis    *trackingListener
	bus    *brokerBus
	hub    *fanout.Hub
	ws     *WSHandler
	tokens *auth.TokenService
}

// newNode starts a gateway on b. log is what it reads catch-ups from;
// nil means the broker's log.
func newNode(t *testing.T, b *broker, log eventlog.Log, id string) *node {
	t.Helper()
	if log == nil {
		log = b
	}
	tokens, err := auth.NewTokenService(wsSecret, auth.TokenConfig{})
	if err != nil {
		t.Fatal(err)
	}
	bus := &brokerBus{b: b}
	var router *routing.Router
	hub := fanout.NewHub(fanout.Hooks{
		OnFirstSubscriber: func(t domain.Topic) error { return router.OnFirstSubscriber(t) },
		OnLastUnsubscribe: func(t domain.Topic) { router.OnLastUnsubscribe(t) },
	})
	router = routing.New(hub, log, bus, bus, quietLog, routing.Config{})
	bus.router = router

	trips := auth.NewStaticTrips()
	trips.Put("acme", "42", auth.Trip{EmployeeID: "alice", OfficeID: "blr-1"})
	subs := service.NewSubscriptions(auth.NewRoleAuthorizer(trips), hub, log, service.SubscriptionsConfig{})
	ws := NewWSHandler(tokens, subs, quietLog, WSConfig{NodeID: id})
	mux := http.NewServeMux()
	ws.Register(mux)

	srv := httptest.NewUnstartedServer(mux)
	lis := &trackingListener{Listener: srv.Listener}
	srv.Listener = lis
	srv.Start()
	t.Cleanup(srv.Close)
	return &node{srv: srv, lis: lis, bus: bus, hub: hub, ws: ws, tokens: tokens}
}

// crash kills the node: its sockets drop without a close frame and it
// stops receiving from the broker.
func (n *node) crash() {
	n.bus.dead.Store(true)
	n.lis.kill()
}

func (n *node) url() string { return "ws" + strings.TrimPrefix(n.srv.URL, "http") + "/ws" }

func (n *node) dialAs(t *testing.T, p domain.Principal) *websocket.Conn {
	t.Helper()
	tok, _, err := n.tokens.Issue(p, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	c, _, err := websocket.DefaultDialer.Dial(n.url()+"?token="+tok, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	if f := read(t, c); f.Type != "hello" {
		t.Fatalf("first frame = %+v, want hello", f)
	}
	return c
}

// publisher stores events in the broker the way the publish API does.
type publisher struct {
	p *service.Publisher
	n int
}

func (p *publisher) publish(t *testing.T, topic domain.Topic, kind domain.EventKind) uint64 {
	t.Helper()
	p.n++
	ev := domain.Event{
		BaseEvent: domain.BaseEvent{ID: fmt.Sprintf("e-%d", p.n), Tenant: topic.Tenant, CabID: "cab-7"},
		Topic:     topic, Kind: kind,
	}
	res, err := p.p.Publish(context.Background(), ev)
	if err != nil {
		t.Fatal(err)
	}
	return res.Seq + res.Ver
}

func readEvent(t *testing.T, c *websocket.Conn, kind domain.EventKind, seq uint64) {
	t.Helper()
	if f := read(t, c); f.Type != "event" || f.Kind != string(kind) || f.Seq != seq {
		t.Fatalf("frame = %+v, want %s seq %d", f, kind, seq)
	}
}

// Node A dies mid-trip. The client reconnects to node B with its lastSeq
// and gets exactly what it missed, the SOS included, then live events.
func TestNodeCrashResumeOnOtherNode(t *testing.T) {
	b := newBroker()
	a, bNode := newNode(t, b, nil, "node-a"), newNode(t, b, nil, "node-b")
	pub := &publisher{p: service.NewPublisher(b, b, service.PublisherConfig{})}
	topic := domain.Topic{Tenant: "acme", Kind: domain.TopicTrip, ID: "42"}

	c := a.dialAs(t, alice)
	subscribeFrom(t, c, topic.String(), 0)
	for seq := uint64(1); seq <= 5; seq++ {
		pub.publish(t, topic, domain.KindStatus)
		readEvent(t, c, domain.KindStatus, seq)
	}

	a.crash()
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err := c.ReadMessage()
	var ce *websocket.CloseError
	if err == nil || (errors.As(err, &ce) && ce.Code != websocket.CloseAbnormalClosure) {
		t.Fatalf("read after crash = %v, want the socket to drop without a close frame", err)
	}

	for range 5 {
		pub.publish(t, topic, domain.KindStatus) // seq 6..10
	}
	sos := pub.publish(t, topic, domain.KindSOS) // seq 11

	c = bNode.dialAs(t, alice)
	if f := subscribeFrom(t, c, topic.String(), 5); f.Resume != "replay" || f.AsOfSeq != sos {
		t.Fatalf("subscribed = %+v, want replay as of %d", f, sos)
	}
	for seq := uint64(6); seq <= 10; seq++ {
		readEvent(t, c, domain.KindStatus, seq)
	}
	readEvent(t, c, domain.KindSOS, sos)

	// The next frame is the next live event: nothing was sent twice.
	live := pub.publish(t, topic, domain.KindStatus)
	readEvent(t, c, domain.KindStatus, live)
}

// racingLog publishes during the subscribe's catch-up read, after the
// connection is attached to the topic and before "subscribed" is sent.
type racingLog struct {
	eventlog.Log
	during func()
}

func (l racingLog) Snapshot(ctx context.Context, topic domain.Topic) (eventlog.Snapshot, error) {
	l.during()
	return l.Log.Snapshot(ctx, topic)
}

// Locations skip the reliable hold, but must still not reach the client
// before the topic's "subscribed" frame.
func TestLocationNotBeforeSubscribed(t *testing.T) {
	b := newBroker()
	pub := &publisher{p: service.NewPublisher(b, b, service.PublisherConfig{})}
	topic := domain.Topic{Tenant: "acme", Kind: domain.TopicTrip, ID: "42"}
	var once sync.Once
	log := racingLog{Log: b, during: func() {
		once.Do(func() {
			// Runs on the server's reader goroutine, so no t.Fatal here.
			ev := domain.Event{
				BaseEvent: domain.BaseEvent{ID: "loc-1", Tenant: topic.Tenant, CabID: "cab-7"},
				Topic:     topic, Kind: domain.KindLocation,
			}
			if _, err := pub.p.Publish(context.Background(), ev); err != nil {
				t.Error(err)
			}
			// Time for the writer to send it, were it not held.
			time.Sleep(50 * time.Millisecond)
		})
	}}
	n := newNode(t, b, log, "node-a")

	c := n.dialAs(t, alice)
	subscribeFrom(t, c, topic.String(), 0) // fails if the location came first
	if f := read(t, c); f.Type != "event" || f.Kind != "location" {
		t.Fatalf("frame after subscribed = %+v, want the location", f)
	}
}
