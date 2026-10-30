package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/auth"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/eventlog"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/fanout"
)

const (
	DefaultAuthTimeout = 2 * time.Second
	DefaultReadTimeout = 2 * time.Second
)

// ResumeMode tells the client how the events after "subscribed" relate to
// what it already has.
type ResumeMode string

const (
	// ResumeReplay: exactly the reliable events after the client's lastSeq.
	ResumeReplay ResumeMode = "replay"
	// ResumeSnapshot: the client is new or too far behind. It drops what
	// it has for the topic and takes the newest event per (cab, kind).
	ResumeSnapshot ResumeMode = "snapshot"
)

// Resume is what the client is told before its catch-up starts. AsOfSeq
// is the topic Seq the catch-up brings it to; live events follow it.
type Resume struct {
	Mode    ResumeMode
	AsOfSeq uint64
}

// SubscriptionsConfig tunes Subscriptions. Zero values take the defaults.
type SubscriptionsConfig struct {
	// AuthTimeout bounds one authorization (trip lookup).
	AuthTimeout time.Duration
	// ReadTimeout bounds reading the catch-up from the event log.
	ReadTimeout time.Duration
	// Metrics receives subscribe outcomes; nil means none.
	Metrics SubscribeMetrics
}

// Subscriptions is the gateway's subscribe use case: authorize, attach the
// connection to the hub, and catch the client up from the event log.
type Subscriptions struct {
	authz auth.Authorizer
	hub   *fanout.Hub
	log   eventlog.Log
	cfg   SubscriptionsConfig
}

func NewSubscriptions(authz auth.Authorizer, hub *fanout.Hub, log eventlog.Log, cfg SubscriptionsConfig) *Subscriptions {
	if cfg.AuthTimeout <= 0 {
		cfg.AuthTimeout = DefaultAuthTimeout
	}
	if cfg.ReadTimeout <= 0 {
		cfg.ReadTimeout = DefaultReadTimeout
	}
	if cfg.Metrics == nil {
		cfg.Metrics = noMetrics{}
	}
	return &Subscriptions{authz: authz, hub: hub, log: log, cfg: cfg}
}

// Subscribe starts delivery of topic to c, resuming after lastSeq (0 means
// the client has nothing yet):
//
//  1. authorize p;
//  2. hold the topic on c, then attach c to the hub: live events now wait
//     in the hold instead of racing the catch-up;
//  3. read the replay after lastSeq, or a snapshot if lastSeq is 0 or
//     trimmed from the log;
//  4. confirm(resume), so the client hears the mode before any event;
//  5. send the catch-up, then held events not already in it, then the
//     newest location per cab.
//
// Live events published between 2 and 3 are in both the log read and the
// hold; Conn.Resume sends them once, by Seq.
//
// Errors wrap domain.ErrForbidden, auth.ErrLookupFailed, ErrUnavailable
// (event log or broker subscribe), fanout.ErrClosed, or whatever confirm
// returned. On error c
// is left detached from topic.
func (s *Subscriptions) Subscribe(ctx context.Context, p domain.Principal, topic domain.Topic, lastSeq uint64,
	c *fanout.Conn, confirm func(Resume) error) error {
	err := s.subscribe(ctx, p, topic, lastSeq, c, confirm)
	s.cfg.Metrics.Subscribed(subscribeOutcome(err))
	return err
}

func (s *Subscriptions) subscribe(ctx context.Context, p domain.Principal, topic domain.Topic, lastSeq uint64,
	c *fanout.Conn, confirm func(Resume) error) error {
	if err := s.authorize(ctx, p, topic); err != nil {
		return err
	}

	c.Hold(topic)
	if err := s.hub.Attach(topic, c); err != nil {
		c.CancelHold(topic)
		if errors.Is(err, fanout.ErrClosed) {
			return err
		}
		// The node could not subscribe to the topic in the broker.
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	err := s.catchUp(ctx, topic, lastSeq, c, confirm)
	if err != nil {
		s.hub.Detach(topic, c)
		c.CancelHold(topic)
	}
	return err
}

func (s *Subscriptions) authorize(ctx context.Context, p domain.Principal, topic domain.Topic) error {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.AuthTimeout)
	defer cancel()
	err := s.authz.Authorize(ctx, p, topic)
	if err == nil || errors.Is(err, domain.ErrForbidden) || errors.Is(err, auth.ErrLookupFailed) {
		return err
	}
	return fmt.Errorf("%w: %w", auth.ErrLookupFailed, err)
}

func (s *Subscriptions) catchUp(ctx context.Context, topic domain.Topic, lastSeq uint64,
	c *fanout.Conn, confirm func(Resume) error) error {
	resume, events, locations, err := s.read(ctx, topic, lastSeq)
	if err != nil {
		return err
	}
	if err := confirm(resume); err != nil {
		return err
	}
	if err := c.Resume(ctx, topic, events, resume.AsOfSeq); err != nil {
		return err
	}
	for _, ev := range locations {
		if err := c.Enqueue(ev); err != nil {
			return err
		}
	}
	return nil
}

// read loads the catch-up for topic from the log.
func (s *Subscriptions) read(ctx context.Context, topic domain.Topic, lastSeq uint64) (
	resume Resume, events, locations []domain.Event, err error) {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.ReadTimeout)
	defer cancel()

	resume = Resume{Mode: ResumeReplay, AsOfSeq: lastSeq}
	metric := string(ResumeReplay)
	if lastSeq > 0 {
		events, err = s.log.After(ctx, topic, lastSeq)
	}
	if lastSeq == 0 || errors.Is(err, eventlog.ErrTrimmed) {
		metric = string(ResumeSnapshot)
		if lastSeq > 0 {
			metric = ResumeSnapshotTrimmed
		}
		var snap eventlog.Snapshot
		snap, err = s.log.Snapshot(ctx, topic)
		resume, events = Resume{Mode: ResumeSnapshot, AsOfSeq: snap.AsOfSeq}, snap.Events
	}
	if err != nil {
		return Resume{}, nil, nil, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	if n := len(events); n > 0 && resume.Mode == ResumeReplay {
		resume.AsOfSeq = events[n-1].Seq
	}

	locations, err = s.log.LatestLocation(ctx, topic)
	if err != nil {
		return Resume{}, nil, nil, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	s.cfg.Metrics.Resumed(metric)
	return resume, events, locations, nil
}

// Detach stops delivery of topic to c.
func (s *Subscriptions) Detach(topic domain.Topic, c *fanout.Conn) {
	s.hub.Detach(topic, c)
}

// Release detaches c from every topic. Call it once the connection ended.
func (s *Subscriptions) Release(c *fanout.Conn) {
	s.hub.DetachAll(c)
}
