package handler

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"
)

type recMetrics struct {
	mu     sync.Mutex
	closes []string
	panics []string
}

func (m *recMetrics) WSClosed(reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closes = append(m.closes, reason)
}

func (m *recMetrics) Panicked(where string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.panics = append(m.panics, where)
}

func (m *recMetrics) closed() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.closes...)
}

func TestRecoverReturns500WithoutDetails(t *testing.T) {
	var logs bytes.Buffer
	m := &recMetrics{}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/events", func(http.ResponseWriter, *http.Request) {
		panic("redis at 10.0.0.5:6379 returned nil map")
	})
	srv := httptest.NewServer(Recover(slog.New(slog.NewJSONHandler(&logs, nil)), m, mux))
	defer srv.Close()

	got := post(t, srv, "/v1/events", `{}`)
	if got.status != http.StatusInternalServerError || got.body["code"] != "internal" {
		t.Fatalf("got %d %+v, want 500 internal", got.status, got.body)
	}
	if strings.Contains(fmt.Sprint(got.body), "10.0.0.5") {
		t.Fatalf("panic text leaked to client: %v", got.body)
	}
	if !strings.Contains(logs.String(), "10.0.0.5") || !strings.Contains(logs.String(), `"route":"POST /v1/events"`) {
		t.Fatalf("panic not logged with its route:\n%s", logs.String())
	}
	if len(m.panics) != 1 || m.panics[0] != "http" {
		t.Fatalf("panics = %v", m.panics)
	}

	// The server keeps serving.
	if got := post(t, srv, "/v1/events", `{}`); got.status != http.StatusInternalServerError {
		t.Fatalf("second request status = %d", got.status)
	}
}

func TestRecoverPassesAbortHandlerOn(t *testing.T) {
	h := Recover(quietLog, nil, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic(http.ErrAbortHandler)
	}))
	defer func() {
		if v := recover(); v != http.ErrAbortHandler {
			t.Fatalf("recovered %v, want http.ErrAbortHandler", v)
		}
	}()
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil))
}

func TestWSClosesAreCountedByReason(t *testing.T) {
	m := &recMetrics{}
	env := newWSEnv(t, nil, WSConfig{Metrics: m})

	readClose(t, env.dial(t, "", nil)) // no token
	c := env.dialAs(t, alice)
	send(t, c, opSubscribe, "t:globex:trip:42")
	readClose(t, c)

	waitFor(t, "two closes counted", func() bool { return len(m.closed()) == 2 })
	if got := m.closed(); got[0] != "unauthenticated" || got[1] != "forbidden" {
		t.Fatalf("closes = %v, want [unauthenticated forbidden]", got)
	}
}

func TestCloseLabels(t *testing.T) {
	for cause, want := range map[error]string{
		nil:                    "normal",
		domain.ErrForbidden:    "forbidden",
		errOverloaded:          "try_again_later",
		errPanic:               "internal_error",
		errors.New("surprise"): "internal_error",
	} {
		if _, reason := closeFrame(cause); closeLabel(reason) != want {
			t.Errorf("closeLabel(%v) = %q, want %q", cause, closeLabel(reason), want)
		}
	}
}
