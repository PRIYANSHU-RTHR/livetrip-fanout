package handler

import (
	"context"
	"log/slog"
	"net/http"
	"time"
)

// HealthCheck reports whether a dependency is reachable.
type HealthCheck func(ctx context.Context) error

const healthTimeout = time.Second

// Health serves GET /healthz: 200 when every check passes, 503 otherwise.
// The failing dependency is logged, not returned to the caller.
func Health(log *slog.Logger, checks map[string]HealthCheck) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), healthTimeout)
		defer cancel()
		for name, check := range checks {
			if err := check(ctx); err != nil {
				log.Warn("health check failed", "check", name, "err", err)
				writeError(w, errNotReady)
				return
			}
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
}
