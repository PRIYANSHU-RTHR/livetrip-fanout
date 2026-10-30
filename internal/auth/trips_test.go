package auth

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type stubTrips struct {
	calls atomic.Int64
	mu    sync.Mutex
	trips map[string]Trip
	err   error
}

func (s *stubTrips) Trip(_ context.Context, _, id string) (Trip, error) {
	s.calls.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return Trip{}, s.err
	}
	t, ok := s.trips[id]
	if !ok {
		return Trip{}, ErrTripNotFound
	}
	return t, nil
}

func (s *stubTrips) set(id string, t Trip) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.trips[id] = t
}

func newStub() *stubTrips {
	return &stubTrips{trips: map[string]Trip{"42": {EmployeeID: "alice", OfficeID: "blr-1"}}}
}

func TestCacheServesHitsWithoutBackend(t *testing.T) {
	stub := newStub()
	c := NewCachedTrips(stub, CacheConfig{})
	for range 100 {
		if _, err := c.Trip(context.Background(), "acme", "42"); err != nil {
			t.Fatal(err)
		}
	}
	if n := stub.calls.Load(); n != 1 {
		t.Fatalf("backend calls = %d, want 1", n)
	}
}

// A reassigned trip is visible to the old owner for at most TTL.
func TestCacheStalenessBoundedByTTL(t *testing.T) {
	clk := newClock()
	stub := newStub()
	c := NewCachedTrips(stub, CacheConfig{TTL: 30 * time.Second, Now: clk.now})

	if got, _ := c.Trip(context.Background(), "acme", "42"); got.EmployeeID != "alice" {
		t.Fatalf("got %+v", got)
	}
	stub.set("42", Trip{EmployeeID: "bob", OfficeID: "blr-1"})

	clk.add(29 * time.Second)
	if got, _ := c.Trip(context.Background(), "acme", "42"); got.EmployeeID != "alice" {
		t.Fatalf("before TTL got %+v, want cached alice", got)
	}
	clk.add(time.Second)
	if got, _ := c.Trip(context.Background(), "acme", "42"); got.EmployeeID != "bob" {
		t.Fatalf("after TTL got %+v, want bob", got)
	}
}

func TestCacheDoesNotStoreMissesOrErrors(t *testing.T) {
	stub := newStub()
	c := NewCachedTrips(stub, CacheConfig{})

	if _, err := c.Trip(context.Background(), "acme", "77"); !errors.Is(err, ErrTripNotFound) {
		t.Fatalf("err = %v, want ErrTripNotFound", err)
	}
	stub.set("77", Trip{EmployeeID: "carol"})
	if got, err := c.Trip(context.Background(), "acme", "77"); err != nil || got.EmployeeID != "carol" {
		t.Fatalf("new trip not visible at once: %+v %v", got, err)
	}

	stub.err = errors.New("timeout")
	if _, err := c.Trip(context.Background(), "acme", "88"); err == nil {
		t.Fatal("want error")
	}
	stub.err = nil
	stub.set("88", Trip{EmployeeID: "dan"})
	if got, err := c.Trip(context.Background(), "acme", "88"); err != nil || got.EmployeeID != "dan" {
		t.Fatalf("error was cached: %+v %v", got, err)
	}
}

func TestCacheKeysIncludeTenant(t *testing.T) {
	trips := NewStaticTrips()
	trips.Put("acme", "42", Trip{EmployeeID: "alice"})
	trips.Put("globex", "42", Trip{EmployeeID: "mallory"})
	c := NewCachedTrips(trips, CacheConfig{})

	a, _ := c.Trip(context.Background(), "acme", "42")
	g, _ := c.Trip(context.Background(), "globex", "42")
	if a.EmployeeID != "alice" || g.EmployeeID != "mallory" {
		t.Fatalf("acme = %+v, globex = %+v", a, g)
	}
}

func TestLRUEvictsLeastRecentlyUsed(t *testing.T) {
	c := newTTLLRU[string, int](2, time.Minute, time.Now)
	c.put("a", 1)
	c.put("b", 2)
	c.get("a") // b is now the oldest
	c.put("c", 3)

	if _, ok := c.get("b"); ok {
		t.Fatal("b should have been evicted")
	}
	if v, ok := c.get("a"); !ok || v != 1 {
		t.Fatalf("a = %d %v", v, ok)
	}
	if c.len() != 2 {
		t.Fatalf("len = %d, want 2", c.len())
	}
}

func TestCacheConcurrent(t *testing.T) {
	trips := NewStaticTrips()
	for i := range 50 {
		trips.Put("acme", fmt.Sprint(i), Trip{EmployeeID: fmt.Sprint("u", i)})
	}
	c := NewCachedTrips(trips, CacheConfig{Size: 16})

	var wg sync.WaitGroup
	for g := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 500 {
				id := fmt.Sprint((g + i) % 50)
				got, err := c.Trip(context.Background(), "acme", id)
				if err != nil || got.EmployeeID != "u"+id {
					t.Errorf("trip %s = %+v %v", id, got, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if n := c.cache.len(); n > 16 {
		t.Fatalf("cache len = %d, want <= 16", n)
	}
}
