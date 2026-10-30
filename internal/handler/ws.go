package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/auth"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/fanout"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/service"
)

// Close codes sent to clients. 4xxx are application codes; 1013 is the
// standard "try again later".
const (
	CloseUnauthenticated = 4001
	CloseForbidden       = 4003
	CloseSlowConsumer    = 4008
	CloseTryAgainLater   = websocket.CloseTryAgainLater
)

const (
	DefaultMaxConns         = 50_000
	DefaultMaxTopicsPerConn = 64
	DefaultWriteTimeout     = 10 * time.Second
	DefaultPingInterval     = 25 * time.Second
	DefaultPongTimeout      = 60 * time.Second

	// maxClientMessage caps one client frame. Client messages are a few
	// hundred bytes; anything larger is a bug or abuse.
	maxClientMessage = 4 << 10
	closeGrace       = time.Second
)

var (
	// errOverloaded means the gateway is at its connection limit. The
	// client should reconnect, ideally through the load balancer to
	// another node.
	errOverloaded = errors.New("gateway at connection limit")
	// errClientGone means the client closed or the read side failed.
	errClientGone = errors.New("client went away")
	// errPanic means handling a client message panicked. The client gets
	// 1011 and reconnects; the details are logged only.
	errPanic = errors.New("panic in websocket handler")
)

// TokenVerifier turns a bearer token into a principal.
type TokenVerifier interface {
	Verify(token string) (domain.Principal, error)
}

// SubscriptionService is what the websocket handler needs from the
// subscribe use case.
type SubscriptionService interface {
	// Subscribe authorizes, attaches c and sends the catch-up after
	// lastSeq. confirm runs before any event of topic is written.
	Subscribe(ctx context.Context, p domain.Principal, topic domain.Topic, lastSeq uint64,
		c *fanout.Conn, confirm func(service.Resume) error) error
	Detach(topic domain.Topic, c *fanout.Conn)
	Release(c *fanout.Conn)
}

// WSConfig tunes a WSHandler. Zero values take the defaults.
type WSConfig struct {
	MaxConns         int
	MaxTopicsPerConn int
	// WriteTimeout bounds one frame write. A client that cannot take a
	// frame in this time is closed.
	WriteTimeout time.Duration
	// PingInterval is how often the server pings; PongTimeout is how long
	// it waits for any sign of life before dropping the connection.
	PingInterval time.Duration
	PongTimeout  time.Duration
	// Metrics receives closes and recovered panics; nil means none.
	Metrics Metrics
	// ConnMetrics is passed to each fanout.Conn; nil means none.
	ConnMetrics fanout.Metrics
}

func (c *WSConfig) defaults() {
	if c.MaxConns <= 0 {
		c.MaxConns = DefaultMaxConns
	}
	if c.MaxTopicsPerConn <= 0 {
		c.MaxTopicsPerConn = DefaultMaxTopicsPerConn
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = DefaultWriteTimeout
	}
	if c.PingInterval <= 0 {
		c.PingInterval = DefaultPingInterval
	}
	if c.PongTimeout <= 0 {
		c.PongTimeout = DefaultPongTimeout
	}
	if c.Metrics == nil {
		c.Metrics = noMetrics{}
	}
}

// WSHandler serves GET /ws.
//
// Protocol (JSON text frames):
//
//	client: {"op":"subscribe","topic":"t:acme:trip:42","lastSeq":17}
//	        {"op":"unsubscribe","topic":"..."}
//	        {"op":"ack","topic":"...","lastSeq":18}
//	server: {"type":"subscribed","topic":"...","resume":"replay"|"snapshot","asOfSeq":20}
//	        before any event of the topic; on "snapshot" the client drops
//	        what it has for the topic
//	        {"type":"unsubscribed","topic":"..."}
//	        {"type":"event", ...domain.Event fields}
//	        {"type":"error","code":"bad_request","message":"..."}
//
// A bad request gets an error frame and the connection stays open. A
// denied subscribe closes the connection (see closeFrame for the codes).
type WSHandler struct {
	verifier TokenVerifier
	subs     SubscriptionService
	log      *slog.Logger
	cfg      WSConfig
	upgrader websocket.Upgrader

	conns  atomic.Int64
	active sync.WaitGroup
}

func NewWSHandler(verifier TokenVerifier, subs SubscriptionService, log *slog.Logger, cfg WSConfig) *WSHandler {
	cfg.defaults()
	return &WSHandler{
		verifier: verifier,
		subs:     subs,
		log:      log,
		cfg:      cfg,
		upgrader: websocket.Upgrader{
			ReadBufferSize: 1024,
			// Write buffers are taken from a pool only while a frame is
			// written, so idle connections hold none.
			WriteBufferPool: &sync.Pool{},
			// Auth is a bearer token, not a cookie, so a page on another
			// origin cannot ride a user's session. Any origin may connect.
			CheckOrigin: func(*http.Request) bool { return true },
		},
	}
}

func (h *WSHandler) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /ws", h.serve)
}

// Conns returns the number of open websocket connections.
func (h *WSHandler) Conns() int { return int(h.conns.Load()) }

// Wait blocks until every connection has ended or ctx is done. Cancel the
// requests' base context first so the connections start closing.
func (h *WSHandler) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() { h.active.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *WSHandler) serve(w http.ResponseWriter, r *http.Request) {
	ws, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return // the upgrader already answered with an HTTP error
	}
	h.active.Add(1)
	defer h.active.Done()
	sink := newWSSink(ws, h.cfg.WriteTimeout, h.cfg.Metrics)

	// Checked after the upgrade so a browser sees a close code; an HTTP
	// status on a failed upgrade is invisible to its websocket API.
	if h.conns.Add(1) > int64(h.cfg.MaxConns) {
		h.conns.Add(-1)
		_ = sink.Close(errOverloaded)
		return
	}
	defer h.conns.Add(-1)

	p, err := h.verifier.Verify(bearerToken(r))
	if err != nil {
		h.log.Info("ws auth failed", "err", err)
		_ = sink.Close(domain.ErrUnauthenticated)
		return
	}

	ctx := r.Context()
	conn := fanout.NewConnWithMetrics(sink, h.cfg.ConnMetrics)
	s := &session{h: h, p: p, ws: ws, sink: sink, conn: conn, topics: make(map[domain.Topic]struct{})}

	ws.SetReadLimit(maxClientMessage)
	_ = ws.SetReadDeadline(time.Now().Add(h.cfg.PongTimeout))
	ws.SetPongHandler(func(string) error { return s.extendRead() })

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); s.readLoop(ctx) }()
	go func() { defer wg.Done(); s.pingLoop(ctx) }()

	cause := conn.Run(ctx) // closes the sink, which ends readLoop
	wg.Wait()
	// Only now: readLoop can no longer attach a topic behind Release.
	h.subs.Release(conn)

	code, _ := closeFrame(cause)
	h.log.Debug("ws closed", "tenant", p.Tenant, "role", p.Role, "code", code, "topics", len(s.topics))
}

// bearerToken reads "Authorization: Bearer <token>", or ?token= because
// browsers cannot set headers on a websocket request.
func bearerToken(r *http.Request) string {
	const prefix = "bearer "
	if v := r.Header.Get("Authorization"); len(v) > len(prefix) && strings.EqualFold(v[:len(prefix)], prefix) {
		return strings.TrimSpace(v[len(prefix):])
	}
	return r.URL.Query().Get("token")
}

// closeFrame maps why a connection ended to the code and reason sent to
// the client. Reasons are fixed strings, never internal error text.
func closeFrame(cause error) (code int, reason string) {
	switch {
	case cause == nil, errors.Is(cause, errClientGone):
		return websocket.CloseNormalClosure, ""
	case errors.Is(cause, domain.ErrUnauthenticated):
		return CloseUnauthenticated, "unauthenticated"
	case errors.Is(cause, domain.ErrForbidden):
		return CloseForbidden, "forbidden"
	case errors.Is(cause, fanout.ErrSlowConsumer):
		return CloseSlowConsumer, "slow consumer"
	case errors.Is(cause, fanout.ErrResync):
		// This node lost events of a topic; the client resumes elsewhere
		// or here with its lastSeq.
		return CloseTryAgainLater, "resync"
	case errors.Is(cause, auth.ErrLookupFailed), errors.Is(cause, service.ErrUnavailable),
		errors.Is(cause, errOverloaded):
		return CloseTryAgainLater, "try again later"
	case errors.Is(cause, context.Canceled):
		return websocket.CloseGoingAway, "server shutting down"
	}
	return websocket.CloseInternalServerErr, "internal error"
}

// wsSink is the fanout.Sink over a websocket. gorilla allows one writer at
// a time; the connection's writer and the reader's replies share mu.
type wsSink struct {
	ws           *websocket.Conn
	writeTimeout time.Duration
	metrics      Metrics
	mu           sync.Mutex
}

func newWSSink(ws *websocket.Conn, writeTimeout time.Duration, m Metrics) *wsSink {
	return &wsSink{ws: ws, writeTimeout: writeTimeout, metrics: m}
}

type eventFrame struct {
	Type string `json:"type"`
	domain.Event
}

type replyFrame struct {
	Type    string `json:"type"`
	Topic   string `json:"topic,omitempty"`
	Resume  string `json:"resume,omitempty"`
	AsOfSeq uint64 `json:"asOfSeq,omitempty"`
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

func (s *wsSink) Write(ev domain.Event) error {
	return s.send(eventFrame{Type: "event", Event: ev})
}

func (s *wsSink) send(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ws.SetWriteDeadline(time.Now().Add(s.writeTimeout)); err != nil {
		return err
	}
	return s.ws.WriteMessage(websocket.TextMessage, b)
}

// Close sends the close frame for cause and closes the socket. A client
// that is not reading may never see the frame; that is fine, it will
// reconnect and resume.
func (s *wsSink) Close(cause error) error {
	code, reason := closeFrame(cause)
	s.metrics.WSClosed(closeLabel(reason))
	_ = s.ws.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(code, reason), time.Now().Add(closeGrace))
	return s.ws.Close()
}

// session is one authenticated connection. topics is touched only by the
// reader goroutine.
type session struct {
	h      *WSHandler
	p      domain.Principal
	ws     *websocket.Conn
	sink   *wsSink
	conn   *fanout.Conn
	topics map[domain.Topic]struct{}
}

type clientMessage struct {
	Op    string `json:"op"`
	Topic string `json:"topic"`
	// LastSeq is the last seq the client has for the topic; 0 means none.
	// On subscribe the server resumes after it; on ack it is informational.
	LastSeq uint64 `json:"lastSeq"`
}

const (
	opSubscribe   = "subscribe"
	opUnsubscribe = "unsubscribe"
	opAck         = "ack"
)

func (s *session) extendRead() error {
	return s.ws.SetReadDeadline(time.Now().Add(s.h.cfg.PongTimeout))
}

// readLoop handles client messages until the socket fails or a message
// must end the connection; then it closes the connection with that cause.
//
// A panic while handling a message ends only this connection, not the
// process: it is recovered here, logged and reported as errPanic.
func (s *session) readLoop(ctx context.Context) {
	cause := errClientGone
	defer func() {
		if v := recover(); v != nil {
			s.h.cfg.Metrics.Panicked("ws_reader")
			s.h.log.Error("panic recovered", "route", "GET /ws", "panic", fmt.Sprint(v), "stack", string(debug.Stack()))
			cause = errPanic
		}
		s.conn.Close(cause)
	}()
	for {
		_, data, err := s.ws.ReadMessage()
		if err != nil {
			return
		}
		_ = s.extendRead()
		if err := s.handle(ctx, data); err != nil {
			cause = err
			return
		}
	}
}

// handle runs one client message. A non-nil error ends the connection.
func (s *session) handle(ctx context.Context, data []byte) error {
	var msg clientMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return s.badRequest("message is not valid JSON")
	}
	topic, err := domain.ParseTopic(msg.Topic)
	if err != nil {
		return s.badRequest("topic must look like t:{tenant}:{kind}:{id}")
	}

	switch msg.Op {
	case opSubscribe:
		return s.subscribe(ctx, topic, msg.LastSeq)
	case opUnsubscribe:
		if _, ok := s.topics[topic]; ok {
			s.h.subs.Detach(topic, s.conn)
			delete(s.topics, topic)
		}
		return s.sink.send(replyFrame{Type: "unsubscribed", Topic: topic.String()})
	case opAck:
		// Delivery is at-least-once with client-side dedup by seq, so the
		// server keeps no per-client cursor and an ack needs no reply.
		return nil
	}
	return s.badRequest("op must be subscribe, unsubscribe or ack")
}

func (s *session) subscribe(ctx context.Context, topic domain.Topic, lastSeq uint64) error {
	if _, ok := s.topics[topic]; ok {
		return s.sink.send(replyFrame{Type: "subscribed", Topic: topic.String()})
	}
	if len(s.topics) >= s.h.cfg.MaxTopicsPerConn {
		return s.sink.send(replyFrame{Type: "error", Topic: topic.String(), Code: "too_many_topics",
			Message: "unsubscribe from a topic before adding another"})
	}

	// The reply goes out from inside Subscribe, after the resume mode is
	// known and before any event of the topic.
	confirm := func(r service.Resume) error {
		return s.sink.send(replyFrame{Type: "subscribed", Topic: topic.String(),
			Resume: string(r.Mode), AsOfSeq: r.AsOfSeq})
	}
	err := s.h.subs.Subscribe(ctx, s.p, topic, lastSeq, s.conn, confirm)
	if err != nil {
		if errors.Is(err, domain.ErrForbidden) || errors.Is(err, auth.ErrLookupFailed) ||
			errors.Is(err, service.ErrUnavailable) {
			s.h.log.Info("ws subscribe failed", "tenant", s.p.Tenant, "role", s.p.Role, "topic", topic.String(), "err", err)
		}
		return err
	}
	s.topics[topic] = struct{}{}
	return nil
}

// badRequest tells the client what was wrong and keeps the connection.
func (s *session) badRequest(msg string) error {
	return s.sink.send(replyFrame{Type: "error", Code: "bad_request", Message: msg})
}

// pingLoop pings the client so dead peers and idle NAT entries are found.
// A missing pong shows up as a read deadline error in readLoop.
func (s *session) pingLoop(ctx context.Context) {
	t := time.NewTicker(s.h.cfg.PingInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.conn.Done():
			return
		case <-t.C:
			if err := s.ws.WriteControl(websocket.PingMessage, nil, time.Now().Add(s.h.cfg.WriteTimeout)); err != nil {
				s.conn.Close(errClientGone)
				return
			}
		}
	}
}
