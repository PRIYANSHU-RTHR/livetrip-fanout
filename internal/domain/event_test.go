package domain

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func validEvent() Event {
	return Event{
		BaseEvent: BaseEvent{ID: "e-1", Tenant: "acme", CabID: "cab-7", CreatedAt: time.Unix(1700000000, 0).UTC()},
		Topic:     Topic{Tenant: "acme", Kind: TopicTrip, ID: "42"},
		Kind:      KindStatus,
		Payload:   json.RawMessage(`{"status":"arrived"}`),
	}
}

func TestEventValidate(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Event)
		ok     bool
	}{
		{"valid", func(*Event) {}, true},
		{"missing id", func(e *Event) { e.ID = "" }, false},
		{"missing tenant", func(e *Event) { e.Tenant = "" }, false},
		{"missing cab", func(e *Event) { e.CabID = "" }, false},
		{"unknown kind", func(e *Event) { e.Kind = "teleport" }, false},
		{"bad topic", func(e *Event) { e.Topic.Tenant = "" }, false},
		{"cross tenant topic", func(e *Event) { e.Topic.Tenant = "globex" }, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := validEvent()
			tc.mutate(&e)
			err := e.Validate()
			if tc.ok && err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}
			if !tc.ok && !errors.Is(err, ErrBadEvent) {
				t.Fatalf("Validate() = %v, want ErrBadEvent", err)
			}
		})
	}
}

func TestEventKindReliable(t *testing.T) {
	tests := []struct {
		kind EventKind
		want bool
	}{
		{KindLocation, false},
		{KindStatus, true},
		{KindSOS, true},
	}
	for _, tc := range tests {
		if got := tc.kind.Reliable(); got != tc.want {
			t.Errorf("%s.Reliable() = %v, want %v", tc.kind, got, tc.want)
		}
	}
}

func TestEventJSONFlattensBase(t *testing.T) {
	b, err := json.Marshal(validEvent())
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"eventId", "tenant", "cabId", "createdAt", "topic", "kind", "payload"} {
		if _, ok := m[k]; !ok {
			t.Errorf("missing key %q in %s", k, b)
		}
	}
	if _, ok := m["seq"]; ok {
		t.Errorf("seq should be omitted when zero: %s", b)
	}

	var back Event
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if back.ID != "e-1" || back.Topic.String() != "t:acme:trip:42" {
		t.Fatalf("round trip = %+v", back)
	}
}

func TestRoleValid(t *testing.T) {
	for _, r := range []Role{RoleEmployee, RoleOfficeAdmin, RoleSecurity, RoleOperator} {
		if !r.Valid() {
			t.Errorf("%s should be valid", r)
		}
	}
	if Role("admin").Valid() {
		t.Error(`"admin" should not be valid`)
	}
}
