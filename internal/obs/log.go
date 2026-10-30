package obs

import (
	"io"
	"log/slog"
	"strings"
)

// Redacted replaces the value of any log attribute that may hold personal
// data.
const Redacted = "[redacted]"

// sensitiveKeys are attribute keys whose values never reach the log, at
// any nesting depth. Code should not log them in the first place; this is
// the safety net for when someone does.
var sensitiveKeys = map[string]bool{
	"phone": true, "mobile": true, "email": true,
	"lat": true, "lng": true, "lon": true, "latitude": true, "longitude": true,
	"location": true, "address": true, "home": true,
	"payload": true, "token": true, "authorization": true, "password": true,
}

// NewLogger returns a JSON logger at level ("debug", "info", "warn",
// "error"; anything else is info) that redacts sensitiveKeys.
func NewLogger(w io.Writer, level string) *slog.Logger {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level:       lvl,
		ReplaceAttr: redact,
	}))
}

func redact(_ []string, a slog.Attr) slog.Attr {
	if sensitiveKeys[strings.ToLower(a.Key)] {
		return slog.String(a.Key, Redacted)
	}
	return a
}
