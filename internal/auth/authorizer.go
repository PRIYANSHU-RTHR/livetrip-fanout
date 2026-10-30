package auth

import (
	"context"
	"errors"
	"fmt"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"
)

// ErrLookupFailed means ownership could not be checked (trip service down,
// deadline hit). The subscribe is refused but the client may retry; it is
// kept apart from ErrForbidden so it is not reported as a denial.
var ErrLookupFailed = errors.New("authorization lookup failed")

// Authorizer decides whether p may subscribe to topic. It returns nil,
// an error wrapping domain.ErrForbidden, or one wrapping ErrLookupFailed.
type Authorizer interface {
	Authorize(ctx context.Context, p domain.Principal, topic domain.Topic) error
}

// AuthorizerFunc lets a plain function be an Authorizer.
type AuthorizerFunc func(ctx context.Context, p domain.Principal, topic domain.Topic) error

func (f AuthorizerFunc) Authorize(ctx context.Context, p domain.Principal, topic domain.Topic) error {
	return f(ctx, p, topic)
}

// RoleAuthorizer checks tenant isolation once, then hands the decision to
// the strategy registered for the caller's role. A role with no strategy is
// denied, so adding a role never grants access by accident.
type RoleAuthorizer struct {
	byRole map[domain.Role]Authorizer
}

// NewRoleAuthorizer wires the default rule for every role:
//   - employee: only the trip they ride
//   - office_admin: their office and SOS topics, and trips of their office
//   - security: SOS and office topics (only their office if they have one)
//   - operator: any topic in their tenant
func NewRoleAuthorizer(trips TripDirectory) *RoleAuthorizer {
	return &RoleAuthorizer{byRole: map[domain.Role]Authorizer{
		domain.RoleEmployee:    employeeRule{trips},
		domain.RoleOfficeAdmin: officeAdminRule{trips},
		domain.RoleSecurity:    AuthorizerFunc(securityRule),
		domain.RoleOperator:    AuthorizerFunc(operatorRule),
	}}
}

// WithRule returns a copy with the strategy for role replaced.
func (a *RoleAuthorizer) WithRule(role domain.Role, rule Authorizer) *RoleAuthorizer {
	m := make(map[domain.Role]Authorizer, len(a.byRole)+1)
	for r, v := range a.byRole {
		m[r] = v
	}
	m[role] = rule
	return &RoleAuthorizer{byRole: m}
}

func (a *RoleAuthorizer) Authorize(ctx context.Context, p domain.Principal, topic domain.Topic) error {
	if err := topic.Validate(); err != nil {
		return fmt.Errorf("%w: %w", domain.ErrForbidden, err)
	}
	if p.Tenant == "" || p.Tenant != topic.Tenant {
		return forbidden("other tenant")
	}
	rule, ok := a.byRole[p.Role]
	if !ok {
		return forbidden("unknown role")
	}
	return rule.Authorize(ctx, p, topic)
}

func forbidden(reason string) error {
	return fmt.Errorf("%w: %s", domain.ErrForbidden, reason)
}

// lookupTrip returns the trip, or ErrForbidden when it does not exist, so a
// caller cannot tell "no such trip" from "not your trip".
func lookupTrip(ctx context.Context, trips TripDirectory, tenant, id string) (Trip, error) {
	t, err := trips.Trip(ctx, tenant, id)
	switch {
	case err == nil:
		return t, nil
	case errors.Is(err, ErrTripNotFound):
		return Trip{}, forbidden("trip not visible")
	}
	return Trip{}, fmt.Errorf("%w: %w", ErrLookupFailed, err)
}

type employeeRule struct{ trips TripDirectory }

func (r employeeRule) Authorize(ctx context.Context, p domain.Principal, topic domain.Topic) error {
	if topic.Kind != domain.TopicTrip {
		return forbidden("employees may only watch their trip")
	}
	t, err := lookupTrip(ctx, r.trips, topic.Tenant, topic.ID)
	if err != nil {
		return err
	}
	if t.EmployeeID != p.UserID {
		// Same message as a missing trip on purpose.
		return forbidden("trip not visible")
	}
	return nil
}

type officeAdminRule struct{ trips TripDirectory }

func (r officeAdminRule) Authorize(ctx context.Context, p domain.Principal, topic domain.Topic) error {
	switch topic.Kind {
	case domain.TopicOffice, domain.TopicSOS:
		if topic.ID != p.OfficeID {
			return forbidden("other office")
		}
		return nil
	case domain.TopicTrip:
		t, err := lookupTrip(ctx, r.trips, topic.Tenant, topic.ID)
		if err != nil {
			return err
		}
		if t.OfficeID != p.OfficeID {
			return forbidden("trip not visible")
		}
		return nil
	}
	return forbidden("office admins may not watch cab topics")
}

func securityRule(_ context.Context, p domain.Principal, topic domain.Topic) error {
	if topic.Kind != domain.TopicSOS && topic.Kind != domain.TopicOffice {
		return forbidden("security may watch sos and office topics")
	}
	if p.OfficeID != "" && topic.ID != p.OfficeID {
		return forbidden("other office")
	}
	return nil
}

// operatorRule allows everything: the tenant check already ran.
func operatorRule(context.Context, domain.Principal, domain.Topic) error { return nil }
