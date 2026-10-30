package service

import (
	"context"
	"errors"
	"testing"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/auth"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/fanout"
)

type nopSink struct{}

func (nopSink) Write(domain.Event) error { return nil }
func (nopSink) Close(error) error        { return nil }

func TestSubscriptionsLifecycle(t *testing.T) {
	trips := auth.NewStaticTrips()
	trips.Put("acme", "42", auth.Trip{EmployeeID: "alice", OfficeID: "blr-1"})
	hub := fanout.NewHub(fanout.Hooks{})
	s := NewSubscriptions(auth.NewRoleAuthorizer(trips), hub)

	alice := domain.Principal{Tenant: "acme", UserID: "alice", Role: domain.RoleEmployee}
	trip, _ := domain.ParseTopic("t:acme:trip:42")
	other, _ := domain.ParseTopic("t:globex:trip:42")
	ctx := context.Background()

	if err := s.Authorize(ctx, alice, trip); err != nil {
		t.Fatalf("own trip: %v", err)
	}
	if err := s.Authorize(ctx, alice, other); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("other tenant: %v, want ErrForbidden", err)
	}

	c := fanout.NewConn(nopSink{})
	if err := s.Attach(ctx, trip, c); err != nil {
		t.Fatal(err)
	}
	if hub.Subscribers(trip) != 1 {
		t.Fatal("not attached")
	}
	s.Release(c)
	if hub.Topics() != 0 {
		t.Fatal("Release left topics behind")
	}

	c.Close(errors.New("client went away"))
	if err := s.Attach(ctx, trip, c); !errors.Is(err, fanout.ErrClosed) {
		t.Fatalf("attach closed conn: %v, want ErrClosed", err)
	}
}
