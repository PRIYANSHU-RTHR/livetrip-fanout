package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/service"
)

// MaxEventBody caps a publish request. Cab events are small; anything
// bigger is a client bug or abuse.
const MaxEventBody = 16 << 10

// EventPublisher is the service the publish handler calls.
type EventPublisher interface {
	Publish(ctx context.Context, ev domain.Event) (service.PublishResult, error)
}

// PublishHandler serves the cab-facing API.
type PublishHandler struct {
	svc EventPublisher
	log *slog.Logger
}

func NewPublishHandler(svc EventPublisher, log *slog.Logger) *PublishHandler {
	return &PublishHandler{svc: svc, log: log}
}

// Register adds the publish routes to mux.
func (h *PublishHandler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/events", h.events)
	mux.HandleFunc("POST /v1/sos", h.sos)
}

// publishRequest is what a cab sends. It has no seq or ver: those are
// assigned by the event log, and sending them is rejected as an unknown field.
type publishRequest struct {
	EventID   string           `json:"eventId"`
	Tenant    string           `json:"tenant"`
	CabID     string           `json:"cabId"`
	Topic     domain.Topic     `json:"topic"`
	Kind      domain.EventKind `json:"kind"`
	CreatedAt time.Time        `json:"createdAt"`
	Payload   json.RawMessage  `json:"payload"`
}

func (r publishRequest) event() domain.Event {
	return domain.Event{
		BaseEvent: domain.BaseEvent{ID: r.EventID, Tenant: r.Tenant, CabID: r.CabID, CreatedAt: r.CreatedAt},
		Topic:     r.Topic,
		Kind:      r.Kind,
		Payload:   r.Payload,
	}
}

type publishResponse struct {
	EventID   string           `json:"eventId"`
	Topic     domain.Topic     `json:"topic"`
	Kind      domain.EventKind `json:"kind"`
	Seq       uint64           `json:"seq,omitempty"`
	Ver       uint64           `json:"ver,omitempty"`
	Duplicate bool             `json:"duplicate"`
}

func (h *PublishHandler) events(w http.ResponseWriter, r *http.Request) {
	req, err := decodeEvent(w, r)
	if err != nil {
		writeError(w, toAPIError(err))
		return
	}
	h.publish(w, r, req.event())
}

// sos is the dedicated SOS endpoint. kind may be omitted; anything other
// than sos is rejected so this route only ever carries SOS traffic.
func (h *PublishHandler) sos(w http.ResponseWriter, r *http.Request) {
	req, err := decodeEvent(w, r)
	if err != nil {
		writeError(w, toAPIError(err))
		return
	}
	if req.Kind == "" {
		req.Kind = domain.KindSOS
	}
	if req.Kind != domain.KindSOS {
		writeError(w, toAPIError(fmt.Errorf("%w: kind must be sos on this endpoint", domain.ErrBadEvent)))
		return
	}
	h.publish(w, r, req.event())
}

func (h *PublishHandler) publish(w http.ResponseWriter, r *http.Request, ev domain.Event) {
	res, err := h.svc.Publish(r.Context(), ev)
	if err != nil {
		api := toAPIError(err)
		if api.status >= http.StatusInternalServerError {
			// Payload is never logged: it can hold coordinates.
			h.log.Error("publish failed",
				"eventId", ev.ID, "topic", ev.Topic.String(), "kind", ev.Kind, "code", api.Code, "err", err)
		}
		writeError(w, api)
		return
	}

	status := http.StatusCreated
	if res.Duplicate {
		status = http.StatusOK
	}
	writeJSON(w, status, publishResponse{
		EventID:   ev.ID,
		Topic:     ev.Topic,
		Kind:      ev.Kind,
		Seq:       res.Seq,
		Ver:       res.Ver,
		Duplicate: res.Duplicate,
	})
}

// decodeEvent reads exactly one JSON object. Decoder errors mention Go
// types, so they are replaced with fixed messages.
func decodeEvent(w http.ResponseWriter, r *http.Request) (publishRequest, error) {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, MaxEventBody))
	dec.DisallowUnknownFields()

	var req publishRequest
	err := dec.Decode(&req)
	if err == nil && dec.Decode(&struct{}{}) != io.EOF {
		err = errors.New("trailing data")
	}
	if err == nil {
		return req, nil
	}

	var tooLarge *http.MaxBytesError
	switch {
	case errors.As(err, &tooLarge):
		return publishRequest{}, errTooLarge
	case errors.Is(err, domain.ErrBadTopic):
		return publishRequest{}, fmt.Errorf("%w: topic must look like t:{tenant}:{kind}:{id}", domain.ErrBadTopic)
	case strings.HasPrefix(err.Error(), "json: unknown field "):
		field := strings.TrimPrefix(err.Error(), "json: unknown field ")
		return publishRequest{}, apiError{http.StatusBadRequest, "invalid_event",
			"unknown field " + field + "; seq and ver are assigned by the server"}
	}
	return publishRequest{}, errInvalidJSON
}
