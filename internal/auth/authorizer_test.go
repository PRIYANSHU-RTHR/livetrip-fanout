package auth

import (
	"context"
	"errors"
	"testing"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"
)

func topic(s string) domain.Topic {
	t, err := domain.ParseTopic(s)
	if err != nil {
		panic(err)
	}
	return t
}

func testTrips() *StaticTrips {
	trips := NewStaticTrips()
	trips.Put("acme", "42", Trip{EmployeeID: "alice", OfficeID: "blr-1"})
	trips.Put("acme", "43", Trip{EmployeeID: "bob", OfficeID: "blr-2"})
	trips.Put("globex", "42", Trip{EmployeeID: "alice", OfficeID: "blr-1"})
	return trips
}

func TestAuthorize(t *testing.T) {
	a := NewRoleAuthorizer(testTrips())
	alice := employee("acme", "alice")
	admin := domain.Principal{Tenant: "acme", UserID: "adm", Role: domain.RoleOfficeAdmin, OfficeID: "blr-1"}
	desk := domain.Principal{Tenant: "acme", UserID: "desk", Role: domain.RoleSecurity, OfficeID: "blr-1"}
	hq := domain.Principal{Tenant: "acme", UserID: "hq", Role: domain.RoleSecurity}
	ops := domain.Principal{Tenant: "acme", UserID: "ops", Role: domain.RoleOperator}

	tests := []struct {
		name  string
		p     domain.Principal
		topic string
		ok    bool
	}{
		{"employee own trip", alice, "t:acme:trip:42", true},
		{"employee other's trip", alice, "t:acme:trip:43", false},
		{"employee missing trip", alice, "t:acme:trip:999", false},
		{"employee cab topic", alice, "t:acme:cab:cab-7", false},
		{"employee office topic", alice, "t:acme:office:blr-1", false},
		{"employee sos topic", alice, "t:acme:sos:blr-1", false},

		{"admin own office", admin, "t:acme:office:blr-1", true},
		{"admin own sos", admin, "t:acme:sos:blr-1", true},
		{"admin other office", admin, "t:acme:office:blr-2", false},
		{"admin trip of own office", admin, "t:acme:trip:42", true},
		{"admin trip of other office", admin, "t:acme:trip:43", false},
		{"admin cab", admin, "t:acme:cab:cab-7", false},

		{"desk own sos", desk, "t:acme:sos:blr-1", true},
		{"desk other sos", desk, "t:acme:sos:blr-2", false},
		{"desk trip", desk, "t:acme:trip:42", false},
		{"tenant-wide desk any sos", hq, "t:acme:sos:blr-2", true},

		{"operator any trip", ops, "t:acme:trip:43", true},
		{"operator any cab", ops, "t:acme:cab:cab-7", true},

		{"unknown role", domain.Principal{Tenant: "acme", UserID: "x", Role: "root"}, "t:acme:trip:42", false},
		{"empty principal", domain.Principal{}, "t:acme:trip:42", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := a.Authorize(context.Background(), tc.p, topic(tc.topic))
			if tc.ok && err != nil {
				t.Fatalf("Authorize = %v, want nil", err)
			}
			if !tc.ok && !errors.Is(err, domain.ErrForbidden) {
				t.Fatalf("Authorize = %v, want ErrForbidden", err)
			}
		})
	}
}

// The spec's required test: a user of one client tries to read another
// client's trip. Every role, even the all-seeing operator, is refused, and
// the trip directory is never asked.
func TestCrossTenantSubscribeForbidden(t *testing.T) {
	trips := &countingTrips{next: testTrips()}
	a := NewRoleAuthorizer(trips)
	// globex has a trip "42" ridden by a user also called alice.
	other := topic("t:globex:trip:42")
	for _, role := range []domain.Role{domain.RoleEmployee, domain.RoleOfficeAdmin, domain.RoleSecurity, domain.RoleOperator} {
		p := domain.Principal{Tenant: "acme", UserID: "alice", Role: role, OfficeID: "blr-1"}
		if err := a.Authorize(context.Background(), p, other); !errors.Is(err, domain.ErrForbidden) {
			t.Errorf("%s: err = %v, want ErrForbidden", role, err)
		}
	}
	if trips.calls != 0 {
		t.Fatalf("trip directory called %d times for a cross-tenant topic", trips.calls)
	}
}

// A missing trip and someone else's trip must look identical, or the error
// becomes an oracle for which trip IDs exist.
func TestMissingAndNotYoursLookTheSame(t *testing.T) {
	a := NewRoleAuthorizer(testTrips())
	alice := employee("acme", "alice")

	notYours := a.Authorize(context.Background(), alice, topic("t:acme:trip:43"))
	missing := a.Authorize(context.Background(), alice, topic("t:acme:trip:999"))
	if !errors.Is(notYours, domain.ErrForbidden) || !errors.Is(missing, domain.ErrForbidden) {
		t.Fatalf("notYours = %v, missing = %v; both want ErrForbidden", notYours, missing)
	}
	if notYours.Error() != missing.Error() {
		t.Fatalf("errors differ: %q vs %q", notYours, missing)
	}
}

func TestLookupFailureIsNotForbidden(t *testing.T) {
	down := errors.New("dial tcp 10.0.0.5:5432: connection refused")
	a := NewRoleAuthorizer(failingTrips{down})
	err := a.Authorize(context.Background(), employee("acme", "alice"), topic("t:acme:trip:42"))
	if !errors.Is(err, ErrLookupFailed) {
		t.Fatalf("err = %v, want ErrLookupFailed", err)
	}
	if errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("lookup failure reported as forbidden: %v", err)
	}
}

func TestWithRuleReplacesOneRole(t *testing.T) {
	base := NewRoleAuthorizer(testTrips())
	denyAll := AuthorizerFunc(func(context.Context, domain.Principal, domain.Topic) error { return forbidden("off") })
	locked := base.WithRule(domain.RoleOperator, denyAll)

	ops := domain.Principal{Tenant: "acme", UserID: "ops", Role: domain.RoleOperator}
	if err := locked.Authorize(context.Background(), ops, topic("t:acme:trip:42")); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("replaced rule not used: %v", err)
	}
	if err := base.Authorize(context.Background(), ops, topic("t:acme:trip:42")); err != nil {
		t.Fatalf("original authorizer changed: %v", err)
	}
}

type countingTrips struct {
	next  TripDirectory
	calls int
}

func (c *countingTrips) Trip(ctx context.Context, tenant, id string) (Trip, error) {
	c.calls++
	return c.next.Trip(ctx, tenant, id)
}

type failingTrips struct{ err error }

func (f failingTrips) Trip(context.Context, string, string) (Trip, error) { return Trip{}, f.err }
