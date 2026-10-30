package auth

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrTripNotFound means the trip does not exist in that tenant.
var ErrTripNotFound = errors.New("trip not found")

// Trip is who may watch a trip: the employee riding it and its office.
type Trip struct {
	EmployeeID string
	OfficeID   string
}

// TripDirectory looks up trip ownership. In production this is the trip
// service or its database; here it is an in-memory map.
type TripDirectory interface {
	Trip(ctx context.Context, tenant, tripID string) (Trip, error)
}

// StaticTrips is an in-memory TripDirectory for tests and the demo.
type StaticTrips struct {
	mu    sync.RWMutex
	trips map[tripKey]Trip
}

type tripKey struct{ tenant, id string }

func NewStaticTrips() *StaticTrips {
	return &StaticTrips{trips: make(map[tripKey]Trip)}
}

func (s *StaticTrips) Put(tenant, tripID string, t Trip) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.trips[tripKey{tenant, tripID}] = t
}

func (s *StaticTrips) Trip(_ context.Context, tenant, tripID string) (Trip, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.trips[tripKey{tenant, tripID}]
	if !ok {
		return Trip{}, ErrTripNotFound
	}
	return t, nil
}

const (
	DefaultTripCacheSize = 100_000
	DefaultTripCacheTTL  = 30 * time.Second
)

// CachedTrips puts an LRU with a TTL in front of a TripDirectory, because
// every reconnect re-authorizes and a reconnect storm after a node crash
// would otherwise hit the trip service once per client.
//
// Staleness: a trip reassigned to another employee is still visible to the
// old one for at most TTL. Only found trips are cached, so a newly created
// trip is visible at once. Lookup errors are never cached.
type CachedTrips struct {
	next  TripDirectory
	cache *ttlLRU[tripKey, Trip]
}

// CacheConfig tunes CachedTrips. Zero values take the defaults.
type CacheConfig struct {
	Size int
	TTL  time.Duration
	// Now is the clock, replaceable in tests.
	Now func() time.Time
}

func NewCachedTrips(next TripDirectory, cfg CacheConfig) *CachedTrips {
	if cfg.Size <= 0 {
		cfg.Size = DefaultTripCacheSize
	}
	if cfg.TTL <= 0 {
		cfg.TTL = DefaultTripCacheTTL
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &CachedTrips{next: next, cache: newTTLLRU[tripKey, Trip](cfg.Size, cfg.TTL, cfg.Now)}
}

func (c *CachedTrips) Trip(ctx context.Context, tenant, tripID string) (Trip, error) {
	k := tripKey{tenant, tripID}
	if t, ok := c.cache.get(k); ok {
		return t, nil
	}
	t, err := c.next.Trip(ctx, tenant, tripID)
	if err != nil {
		return Trip{}, err
	}
	c.cache.put(k, t)
	return t, nil
}
