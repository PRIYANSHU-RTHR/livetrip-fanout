package domain

import (
	"encoding/json"
	"fmt"
	"time"
)

// EventKind decides how an event is delivered.
type EventKind string

const (
	// KindLocation is conflatable: only the newest per cab matters, ordered by Ver.
	KindLocation EventKind = "location"
	// KindStatus and KindSOS are reliable: never dropped, ordered by Seq.
	KindStatus EventKind = "status"
	KindSOS    EventKind = "sos"
)

func (k EventKind) Valid() bool {
	switch k {
	case KindLocation, KindStatus, KindSOS:
		return true
	}
	return false
}

// Reliable reports whether events of this kind get a sequence number and
// must reach every subscriber in order.
func (k EventKind) Reliable() bool { return k == KindStatus || k == KindSOS }

// BaseEvent is what the cab sends: identity and origin, before the server
// assigns a topic position.
type BaseEvent struct {
	ID        string    `json:"eventId"`
	Tenant    string    `json:"tenant"`
	CabID     string    `json:"cabId"`
	CreatedAt time.Time `json:"createdAt"`
}

// Event is a BaseEvent placed on a topic. Seq is set only for reliable
// kinds, Ver only for locations.
type Event struct {
	BaseEvent
	Topic   Topic           `json:"topic"`
	Kind    EventKind       `json:"kind"`
	Seq     uint64          `json:"seq,omitempty"`
	Ver     uint64          `json:"ver,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// Validate checks what a publisher must provide. Seq and Ver are assigned
// later by the event log, so they are not checked here.
func (e Event) Validate() error {
	if e.ID == "" {
		return fmt.Errorf("%w: eventId is required", ErrBadEvent)
	}
	if e.Tenant == "" {
		return fmt.Errorf("%w: tenant is required", ErrBadEvent)
	}
	if !e.Kind.Valid() {
		return fmt.Errorf("%w: unknown kind %q", ErrBadEvent, e.Kind)
	}
	if e.CabID == "" {
		return fmt.Errorf("%w: cabId is required", ErrBadEvent)
	}
	if err := e.Topic.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrBadEvent, err)
	}
	if e.Topic.Tenant != e.Tenant {
		return fmt.Errorf("%w: topic tenant does not match event tenant", ErrBadEvent)
	}
	return nil
}
