package handler

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/service"
)

// apiError is the body of every error response. Message is written for the
// client and never carries internal details such as Redis addresses.
type apiError struct {
	status  int
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e apiError) Error() string { return e.Code + ": " + e.Message }

var (
	errInvalidJSON = apiError{http.StatusBadRequest, "invalid_json", "request body is not valid JSON"}
	errTooLarge    = apiError{http.StatusRequestEntityTooLarge, "body_too_large", "request body is too large"}
	errBusy        = apiError{http.StatusServiceUnavailable, "busy", "too many events in flight; retry with the same eventId"}
	errUnavailable = apiError{http.StatusServiceUnavailable, "unavailable", "event store unavailable; retry with the same eventId"}
	errInternal    = apiError{http.StatusInternalServerError, "internal", "internal error"}
	errNotReady    = apiError{http.StatusServiceUnavailable, "unavailable", "not ready"}
)

// toAPIError maps an error to its public form. Validation messages come
// from domain code and only describe the request, so they are passed on.
func toAPIError(err error) apiError {
	var api apiError
	switch {
	case errors.As(err, &api):
		return api
	case errors.Is(err, domain.ErrBadEvent), errors.Is(err, domain.ErrBadTopic):
		return apiError{http.StatusBadRequest, "invalid_event", err.Error()}
	case errors.Is(err, service.ErrBusy):
		return errBusy
	case errors.Is(err, service.ErrUnavailable):
		return errUnavailable
	}
	return errInternal
}

func writeError(w http.ResponseWriter, e apiError) {
	if e.status == http.StatusServiceUnavailable {
		w.Header().Set("Retry-After", "1")
	}
	writeJSON(w, e.status, e)
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
