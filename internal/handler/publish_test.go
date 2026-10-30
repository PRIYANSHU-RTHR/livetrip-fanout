package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/eventlog/memory"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/service"
)

const statusBody = `{"eventId":"e-1","tenant":"acme","cabId":"cab-7","topic":"t:acme:trip:42","kind":"status","payload":{"status":"arrived"}}`

var quietLog = slog.New(slog.NewTextHandler(io.Discard, nil))

func newServer(svc EventPublisher) *httptest.Server {
	mux := http.NewServeMux()
	NewPublishHandler(svc, quietLog).Register(mux)
	return httptest.NewServer(mux)
}

func newMemoryServer() *httptest.Server {
	log := memory.New(memory.Config{})
	return newServer(service.NewPublisher(log, log, service.PublisherConfig{}))
}

type response struct {
	status int
	body   map[string]any
	header http.Header
}

func post(t *testing.T, srv *httptest.Server, path, body string) response {
	t.Helper()
	res, err := http.Post(srv.URL+path, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var m map[string]any
	if err := json.NewDecoder(res.Body).Decode(&m); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}
	return response{res.StatusCode, m, res.Header}
}

func TestPublishCreatedThenDuplicate(t *testing.T) {
	srv := newMemoryServer()
	defer srv.Close()

	first := post(t, srv, "/v1/events", statusBody)
	if first.status != http.StatusCreated || first.body["seq"] != 1.0 || first.body["duplicate"] != false {
		t.Fatalf("first = %d %v", first.status, first.body)
	}
	again := post(t, srv, "/v1/events", statusBody)
	if again.status != http.StatusOK || again.body["seq"] != 1.0 || again.body["duplicate"] != true {
		t.Fatalf("retry = %d %v", again.status, again.body)
	}
}

func TestPublishLocationReturnsVer(t *testing.T) {
	srv := newMemoryServer()
	defer srv.Close()

	body := `{"eventId":"l-1","tenant":"acme","cabId":"cab-7","topic":"t:acme:cab:cab-7","kind":"location","payload":{"lat":12.9,"lng":77.6}}`
	got := post(t, srv, "/v1/events", body)
	if got.status != http.StatusCreated || got.body["ver"] != 1.0 {
		t.Fatalf("location = %d %v", got.status, got.body)
	}
	if _, ok := got.body["seq"]; ok {
		t.Fatalf("location response has seq: %v", got.body)
	}
}

func TestPublishBadRequests(t *testing.T) {
	srv := newMemoryServer()
	defer srv.Close()

	tests := []struct {
		name   string
		body   string
		status int
		code   string
	}{
		{"missing eventId", strings.Replace(statusBody, `"e-1"`, `""`, 1), 400, "invalid_event"},
		{"unknown kind", strings.Replace(statusBody, `"status"`, `"teleport"`, 1), 400, "invalid_event"},
		{"bad topic", strings.Replace(statusBody, "t:acme:trip:42", "t::trip:42", 1), 400, "invalid_event"},
		{"cross tenant topic", strings.Replace(statusBody, "t:acme:trip:42", "t:globex:trip:42", 1), 400, "invalid_event"},
		{"client sets seq", strings.Replace(statusBody, `{`, `{"seq":99,`, 1), 400, "invalid_event"},
		{"not json", `{"eventId":`, 400, "invalid_json"},
		{"wrong type", strings.Replace(statusBody, `"cab-7"`, `7`, 1), 400, "invalid_json"},
		{"two objects", statusBody + statusBody, 400, "invalid_json"},
		{"too large", strings.Replace(statusBody, `"arrived"`, `"`+strings.Repeat("x", MaxEventBody)+`"`, 1), 413, "body_too_large"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := post(t, srv, "/v1/events", tc.body)
			if got.status != tc.status || got.body["code"] != tc.code {
				t.Fatalf("got %d %v, want %d %s", got.status, got.body, tc.status, tc.code)
			}
			if msg, _ := got.body["message"].(string); msg == "" || strings.Contains(msg, "publishRequest") {
				t.Fatalf("message %q is empty or leaks Go types", msg)
			}
		})
	}
}

func TestSOSEndpoint(t *testing.T) {
	srv := newMemoryServer()
	defer srv.Close()

	noKind := `{"eventId":"sos-1","tenant":"acme","cabId":"cab-7","topic":"t:acme:sos:blr-1"}`
	got := post(t, srv, "/v1/sos", noKind)
	if got.status != http.StatusCreated || got.body["kind"] != "sos" || got.body["seq"] != 1.0 {
		t.Fatalf("sos = %d %v", got.status, got.body)
	}

	got = post(t, srv, "/v1/sos", statusBody)
	if got.status != http.StatusBadRequest || got.body["code"] != "invalid_event" {
		t.Fatalf("status on sos endpoint = %d %v, want 400", got.status, got.body)
	}
}

type failingPublisher struct{ err error }

func (f failingPublisher) Publish(context.Context, domain.Event) (service.PublishResult, error) {
	return service.PublishResult{}, f.err
}

func TestPublishErrorsDoNotLeak(t *testing.T) {
	internal := errors.New("dial tcp 10.0.0.5:6379: connection refused")
	tests := []struct {
		name   string
		err    error
		status int
		code   string
	}{
		{"unavailable", fmt.Errorf("%w: %w", service.ErrUnavailable, internal), 503, "unavailable"},
		{"busy", service.ErrBusy, 503, "busy"},
		{"unknown", internal, 500, "internal"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := newServer(failingPublisher{tc.err})
			defer srv.Close()

			got := post(t, srv, "/v1/events", statusBody)
			if got.status != tc.status || got.body["code"] != tc.code {
				t.Fatalf("got %d %v, want %d %s", got.status, got.body, tc.status, tc.code)
			}
			if msg := got.body["message"].(string); strings.Contains(msg, "10.0.0.5") || strings.Contains(msg, "dial") {
				t.Fatalf("message leaks internals: %q", msg)
			}
			if tc.status == 503 && got.header.Get("Retry-After") == "" {
				t.Fatal("503 without Retry-After")
			}
		})
	}
}

func TestPublishWrongMethod(t *testing.T) {
	srv := newMemoryServer()
	defer srv.Close()

	res, err := http.Get(srv.URL + "/v1/events")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET /v1/events = %d, want 405", res.StatusCode)
	}
}

func TestHealth(t *testing.T) {
	tests := []struct {
		name   string
		check  HealthCheck
		status int
	}{
		{"ok", func(context.Context) error { return nil }, 200},
		{"redis down", func(context.Context) error { return errors.New("dial tcp 10.0.0.5:6379") }, 503},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			Health(quietLog, map[string]HealthCheck{"redis": tc.check})(rec, httptest.NewRequest("GET", "/healthz", nil))
			if rec.Code != tc.status {
				t.Fatalf("status = %d, want %d", rec.Code, tc.status)
			}
			if strings.Contains(rec.Body.String(), "10.0.0.5") {
				t.Fatalf("body leaks internals: %s", rec.Body)
			}
		})
	}
}
