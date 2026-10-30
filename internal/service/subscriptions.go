package service

import (
	"context"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/auth"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/fanout"
)

// Subscriptions is the gateway's subscribe use case: check the caller may
// see a topic, then attach its connection to the hub.
//
// Authorize and Attach are separate so the handler can confirm the
// subscribe to the client before any event of the topic is queued.
type Subscriptions struct {
	authz auth.Authorizer
	hub   *fanout.Hub
}

func NewSubscriptions(authz auth.Authorizer, hub *fanout.Hub) *Subscriptions {
	return &Subscriptions{authz: authz, hub: hub}
}

// Authorize returns nil, an error wrapping domain.ErrForbidden, or one
// wrapping auth.ErrLookupFailed.
func (s *Subscriptions) Authorize(ctx context.Context, p domain.Principal, topic domain.Topic) error {
	return s.authz.Authorize(ctx, p, topic)
}

// Attach starts live delivery of topic to c. It returns fanout.ErrClosed if
// c is already closed.
func (s *Subscriptions) Attach(_ context.Context, topic domain.Topic, c *fanout.Conn) error {
	return s.hub.Attach(topic, c)
}

// Detach stops delivery of topic to c.
func (s *Subscriptions) Detach(topic domain.Topic, c *fanout.Conn) {
	s.hub.Detach(topic, c)
}

// Release detaches c from every topic. Call it once the connection ended.
func (s *Subscriptions) Release(c *fanout.Conn) {
	s.hub.DetachAll(c)
}
