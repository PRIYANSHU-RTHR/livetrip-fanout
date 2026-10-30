package handler

import (
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strings"
)

// Metrics records what the handlers observe. obs.Metrics implements it.
type Metrics interface {
	// WSClosed is called once per websocket with why it ended, e.g.
	// "slow_consumer" or "normal".
	WSClosed(reason string)
	// Panicked is called when a panic was recovered; where names the place.
	Panicked(where string)
}

type noMetrics struct{}

func (noMetrics) WSClosed(string) {}
func (noMetrics) Panicked(string) {}

// Recover turns a panic in next into a logged error, a counted metric and
// a 500 with the usual {code, message} body. The panic value and stack go
// to the log only, never to the client.
//
// http.ErrAbortHandler is re-panicked: it is how a handler asks net/http
// to drop the connection on purpose.
func Recover(log *slog.Logger, m Metrics, next http.Handler) http.Handler {
	if m == nil {
		m = noMetrics{}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			v := recover()
			if v == nil {
				return
			}
			if v == http.ErrAbortHandler {
				panic(v)
			}
			m.Panicked("http")
			log.Error("panic recovered", "route", r.Pattern, "panic", fmt.Sprint(v), "stack", string(debug.Stack()))
			// If the handler already wrote (or hijacked the connection for
			// a websocket), this write fails and the client sees a cut
			// response instead.
			writeError(w, errInternal)
		}()
		next.ServeHTTP(w, r)
	})
}

// closeLabel turns a close reason into a metric label.
func closeLabel(reason string) string {
	if reason == "" {
		return "normal"
	}
	return strings.ReplaceAll(reason, " ", "_")
}
