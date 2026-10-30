package handler

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/auth"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/fanout"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/service"
)

var wsSecret = []byte("0123456789abcdef0123456789abcdef")

type wsEnv struct {
	srv    *httptest.Server
	hub    *fanout.Hub
	tokens *auth.TokenService
	ws     *WSHandler
}

func newWSEnv(t *testing.T, trips auth.TripDirectory, cfg WSConfig) *wsEnv {
	t.Helper()
	tokens, err := auth.NewTokenService(wsSecret, auth.TokenConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if trips == nil {
		st := auth.NewStaticTrips()
		st.Put("acme", "42", auth.Trip{EmployeeID: "alice", OfficeID: "blr-1"})
		st.Put("globex", "42", auth.Trip{EmployeeID: "alice", OfficeID: "blr-1"})
		trips = st
	}
	hub := fanout.NewHub(fanout.Hooks{})
	h := NewWSHandler(tokens, service.NewSubscriptions(auth.NewRoleAuthorizer(trips), hub), quietLog, cfg)
	mux := http.NewServeMux()
	h.Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return &wsEnv{srv: srv, hub: hub, tokens: tokens, ws: h}
}

func (e *wsEnv) token(t *testing.T, p domain.Principal) string {
	t.Helper()
	tok, _, err := e.tokens.Issue(p, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func (e *wsEnv) dial(t *testing.T, query string, header http.Header) *websocket.Conn {
	t.Helper()
	url := "ws" + strings.TrimPrefix(e.srv.URL, "http") + "/ws" + query
	c, _, err := websocket.DefaultDialer.Dial(url, header)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func (e *wsEnv) dialAs(t *testing.T, p domain.Principal) *websocket.Conn {
	return e.dial(t, "?token="+e.token(t, p), nil)
}

var alice = domain.Principal{Tenant: "acme", UserID: "alice", Role: domain.RoleEmployee}

type frame struct {
	Type    string `json:"type"`
	Topic   string `json:"topic"`
	Code    string `json:"code"`
	Kind    string `json:"kind"`
	Seq     uint64 `json:"seq"`
	EventID string `json:"eventId"`
}

func read(t *testing.T, c *websocket.Conn) frame {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	var f frame
	if err := c.ReadJSON(&f); err != nil {
		t.Fatalf("read: %v", err)
	}
	return f
}

// readClose reads until the server closes and returns the close code.
func readClose(t *testing.T, c *websocket.Conn) int {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		_, _, err := c.ReadMessage()
		if err == nil {
			continue
		}
		var ce *websocket.CloseError
		if !errors.As(err, &ce) {
			t.Fatalf("want close frame, got %v", err)
		}
		return ce.Code
	}
}

func send(t *testing.T, c *websocket.Conn, op, topic string) {
	t.Helper()
	if err := c.WriteJSON(map[string]any{"op": op, "topic": topic}); err != nil {
		t.Fatal(err)
	}
}

func statusEvent(topic string, seq uint64) domain.Event {
	tp, _ := domain.ParseTopic(topic)
	return domain.Event{
		BaseEvent: domain.BaseEvent{ID: fmt.Sprint("e-", seq), Tenant: tp.Tenant, CabID: "cab-7"},
		Topic:     tp, Kind: domain.KindStatus, Seq: seq,
	}
}

// waitFor polls cond; hub changes happen on the server's goroutines.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestWSSubscribeThenReceiveInOrder(t *testing.T) {
	env := newWSEnv(t, nil, WSConfig{})
	c := env.dialAs(t, alice)

	send(t, c, "subscribe", "t:acme:trip:42")
	if f := read(t, c); f.Type != "subscribed" || f.Topic != "t:acme:trip:42" {
		t.Fatalf("got %+v, want subscribed", f)
	}
	waitFor(t, "attach", func() bool { return env.hub.Subscribers(statusEvent("t:acme:trip:42", 1).Topic) == 1 })

	for seq := uint64(1); seq <= 3; seq++ {
		env.hub.Publish(statusEvent("t:acme:trip:42", seq))
	}
	for want := uint64(1); want <= 3; want++ {
		f := read(t, c)
		if f.Type != "event" || f.Seq != want || f.Kind != "status" || f.Topic != "t:acme:trip:42" {
			t.Fatalf("frame = %+v, want status event seq %d", f, want)
		}
	}
}

func TestWSTokenInAuthorizationHeader(t *testing.T) {
	env := newWSEnv(t, nil, WSConfig{})
	c := env.dial(t, "", http.Header{"Authorization": {"Bearer " + env.token(t, alice)}})
	send(t, c, "subscribe", "t:acme:trip:42")
	if f := read(t, c); f.Type != "subscribed" {
		t.Fatalf("got %+v, want subscribed", f)
	}
}

func TestWSMissingOrBadTokenCloses4001(t *testing.T) {
	env := newWSEnv(t, nil, WSConfig{})
	for name, q := range map[string]string{"missing": "", "garbage": "?token=not.a.token"} {
		t.Run(name, func(t *testing.T) {
			if code := readClose(t, env.dial(t, q, nil)); code != CloseUnauthenticated {
				t.Fatalf("code = %d, want %d", code, CloseUnauthenticated)
			}
		})
	}
}

// Tenant isolation over the wire: an acme employee asks for globex's trip
// 42, which exists and is ridden by a user also called alice.
func TestWSCrossTenantSubscribeCloses4003(t *testing.T) {
	env := newWSEnv(t, nil, WSConfig{})
	c := env.dialAs(t, alice)
	send(t, c, "subscribe", "t:globex:trip:42")
	if code := readClose(t, c); code != CloseForbidden {
		t.Fatalf("code = %d, want %d", code, CloseForbidden)
	}
	if n := env.hub.Topics(); n != 0 {
		t.Fatalf("hub has %d topics after a denied subscribe", n)
	}
}

type downTrips struct{}

func (downTrips) Trip(context.Context, string, string) (auth.Trip, error) {
	return auth.Trip{}, errors.New("dial tcp 10.0.0.5:5432: connection refused")
}

func TestWSLookupFailureCloses1013(t *testing.T) {
	env := newWSEnv(t, downTrips{}, WSConfig{})
	c := env.dialAs(t, alice)
	send(t, c, "subscribe", "t:acme:trip:42")
	if code := readClose(t, c); code != CloseTryAgainLater {
		t.Fatalf("code = %d, want %d", code, CloseTryAgainLater)
	}
}

func TestWSConnectionLimitCloses1013(t *testing.T) {
	env := newWSEnv(t, nil, WSConfig{MaxConns: 1})
	first := env.dialAs(t, alice)
	send(t, first, "subscribe", "t:acme:trip:42")
	read(t, first) // the first connection is fully set up

	if code := readClose(t, env.dialAs(t, alice)); code != CloseTryAgainLater {
		t.Fatalf("code = %d, want %d", code, CloseTryAgainLater)
	}
}

func TestWSBadRequestKeepsConnection(t *testing.T) {
	env := newWSEnv(t, nil, WSConfig{})
	c := env.dialAs(t, alice)

	bad := []string{`not json`, `{"op":"subscribe","topic":"trip-42"}`, `{"op":"teleport","topic":"t:acme:trip:42"}`}
	for _, msg := range bad {
		if err := c.WriteMessage(websocket.TextMessage, []byte(msg)); err != nil {
			t.Fatal(err)
		}
		if f := read(t, c); f.Type != "error" || f.Code != "bad_request" {
			t.Fatalf("%s: got %+v, want bad_request", msg, f)
		}
	}
	send(t, c, "subscribe", "t:acme:trip:42")
	if f := read(t, c); f.Type != "subscribed" {
		t.Fatalf("got %+v, want subscribed", f)
	}
}

func TestWSTooManyTopics(t *testing.T) {
	env := newWSEnv(t, nil, WSConfig{MaxTopicsPerConn: 1})
	ops := domain.Principal{Tenant: "acme", UserID: "ops", Role: domain.RoleOperator}
	c := env.dialAs(t, ops)

	send(t, c, "subscribe", "t:acme:cab:c1")
	read(t, c)
	send(t, c, "subscribe", "t:acme:cab:c2")
	if f := read(t, c); f.Type != "error" || f.Code != "too_many_topics" {
		t.Fatalf("got %+v, want too_many_topics", f)
	}
	send(t, c, "subscribe", "t:acme:cab:c1") // already held: still fine
	if f := read(t, c); f.Type != "subscribed" {
		t.Fatalf("got %+v, want subscribed", f)
	}
}

func TestWSUnsubscribeStopsDelivery(t *testing.T) {
	env := newWSEnv(t, nil, WSConfig{})
	c := env.dialAs(t, alice)
	topic := statusEvent("t:acme:trip:42", 0).Topic

	send(t, c, "subscribe", topic.String())
	read(t, c)
	send(t, c, "unsubscribe", topic.String())
	if f := read(t, c); f.Type != "unsubscribed" {
		t.Fatalf("got %+v, want unsubscribed", f)
	}
	if n := env.hub.Subscribers(topic); n != 0 {
		t.Fatalf("subscribers = %d after unsubscribe", n)
	}
	// ack is accepted silently; the next frame is the reply to this subscribe.
	send(t, c, "ack", topic.String())
	send(t, c, "subscribe", topic.String())
	if f := read(t, c); f.Type != "subscribed" {
		t.Fatalf("got %+v, want subscribed", f)
	}
}

func TestWSDisconnectReleasesSubscriptions(t *testing.T) {
	env := newWSEnv(t, nil, WSConfig{})
	c := env.dialAs(t, alice)
	send(t, c, "subscribe", "t:acme:trip:42")
	read(t, c)

	c.Close()
	waitFor(t, "release", func() bool { return env.hub.Topics() == 0 && env.ws.Conns() == 0 })
}

// A client that never answers pings is dropped after PongTimeout.
func TestWSDeadPeerIsDropped(t *testing.T) {
	env := newWSEnv(t, nil, WSConfig{PingInterval: 20 * time.Millisecond, PongTimeout: 100 * time.Millisecond})
	c := env.dialAs(t, alice)
	send(t, c, "subscribe", "t:acme:trip:42")
	read(t, c)
	// Not reading means gorilla never sees the pings, so no pongs go back.
	waitFor(t, "dead peer drop", func() bool { return env.ws.Conns() == 0 && env.hub.Topics() == 0 })
}

// One frozen desk must not hold back another: the frozen one is cut off as
// a slow consumer while the healthy one gets every event.
func TestWSFrozenClientDoesNotDelayOthers(t *testing.T) {
	env := newWSEnv(t, nil, WSConfig{WriteTimeout: 200 * time.Millisecond})
	desk := domain.Principal{Tenant: "acme", UserID: "desk", Role: domain.RoleSecurity, OfficeID: "blr-1"}
	const topic = "t:acme:sos:blr-1"

	frozen := env.dialAs(t, desk)
	send(t, frozen, "subscribe", topic)
	read(t, frozen) // then never read again
	healthy := env.dialAs(t, desk)
	send(t, healthy, "subscribe", topic)
	read(t, healthy)
	waitFor(t, "both attached", func() bool { return env.hub.Subscribers(statusEvent(topic, 0).Topic) == 2 })

	// Big payloads fill the frozen socket's kernel buffers quickly. Each
	// SOS is published only after the healthy desk read the previous one,
	// so any stall it sees can only come from the frozen desk.
	payload := []byte(`{"note":"` + strings.Repeat("x", 64<<10) + `"}`)
	for seq := uint64(1); seq <= 300; seq++ {
		ev := statusEvent(topic, seq)
		ev.Kind, ev.Payload = domain.KindSOS, payload
		start := time.Now()
		env.hub.Publish(ev)
		if f := read(t, healthy); f.Seq != seq {
			t.Fatalf("healthy desk got seq %d, want %d", f.Seq, seq)
		}
		if d := time.Since(start); d > time.Second {
			t.Fatalf("SOS %d took %v to reach the healthy desk", seq, d)
		}
	}
	waitFor(t, "frozen client dropped", func() bool { return env.hub.Subscribers(statusEvent(topic, 0).Topic) == 1 })
}

func TestCloseFrameCodes(t *testing.T) {
	tests := []struct {
		cause error
		code  int
	}{
		{nil, websocket.CloseNormalClosure},
		{errClientGone, websocket.CloseNormalClosure},
		{fmt.Errorf("%w: bad sig", domain.ErrUnauthenticated), CloseUnauthenticated},
		{fmt.Errorf("%w: other tenant", domain.ErrForbidden), CloseForbidden},
		{fanout.ErrSlowConsumer, CloseSlowConsumer},
		{fmt.Errorf("%w: timeout", auth.ErrLookupFailed), CloseTryAgainLater},
		{errOverloaded, CloseTryAgainLater},
		{context.Canceled, websocket.CloseGoingAway},
		{errors.New("write tcp: broken pipe"), websocket.CloseInternalServerErr},
	}
	for _, tc := range tests {
		code, reason := closeFrame(tc.cause)
		if code != tc.code {
			t.Errorf("closeFrame(%v) = %d, want %d", tc.cause, code, tc.code)
		}
		if strings.Contains(reason, "tcp") || strings.Contains(reason, "sig") {
			t.Errorf("reason %q leaks internals", reason)
		}
	}
}
