package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/eventlog"
)

var (
	// ErrBusy means too many non-SOS publishes are in flight. The cab should
	// retry later with the same eventId.
	ErrBusy = errors.New("publisher busy")
	// ErrUnavailable means the event log could not be reached. Retrying with
	// the same eventId is safe: the log dedups it.
	ErrUnavailable = errors.New("event log unavailable")
)

const (
	DefaultMaxInFlight = 256
	DefaultTimeout     = 2 * time.Second
	DefaultSOSTimeout  = time.Second
)

// PublisherConfig tunes a Publisher. Zero values take the defaults.
type PublisherConfig struct {
	// MaxInFlight bounds concurrent location/status publishes. SOS is not
	// counted, so a location flood cannot hold SOS back.
	MaxInFlight int
	// Timeout is the deadline for one location/status publish.
	Timeout time.Duration
	// SOSTimeout is the total deadline for an SOS, retries included.
	SOSTimeout time.Duration
	// Now is the clock, replaceable in tests.
	Now func() time.Time
	// Metrics receives publish outcomes; nil means none.
	Metrics PublishMetrics
}

// PublishResult is where the event landed. Seq is set for reliable kinds,
// Ver for locations.
type PublishResult struct {
	Seq       uint64
	Ver       uint64
	Duplicate bool
}

// Publisher accepts cab events and writes them to the event log. SOS events
// take their own lane: a separate log (own Redis pool), no in-flight limit
// and one retry.
type Publisher struct {
	events     eventlog.Log
	sos        eventlog.Log
	slots      chan struct{}
	timeout    time.Duration
	sosTimeout time.Duration
	now        func() time.Time
	metrics    PublishMetrics
}

// NewPublisher wires the two lanes. events and sos may be the same log in
// tests; in production they share Redis but not connections.
func NewPublisher(events, sos eventlog.Log, cfg PublisherConfig) *Publisher {
	if cfg.MaxInFlight <= 0 {
		cfg.MaxInFlight = DefaultMaxInFlight
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = DefaultTimeout
	}
	if cfg.SOSTimeout <= 0 {
		cfg.SOSTimeout = DefaultSOSTimeout
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Metrics == nil {
		cfg.Metrics = noMetrics{}
	}
	return &Publisher{
		events:     events,
		sos:        sos,
		slots:      make(chan struct{}, cfg.MaxInFlight),
		timeout:    cfg.Timeout,
		sosTimeout: cfg.SOSTimeout,
		now:        cfg.Now,
		metrics:    cfg.Metrics,
	}
}

// Publish validates ev and stores it. Errors are domain.ErrBadEvent (caller's
// fault), ErrBusy or ErrUnavailable (retry with the same eventId).
func (p *Publisher) Publish(ctx context.Context, ev domain.Event) (PublishResult, error) {
	start := time.Now()
	res, err := p.publish(ctx, ev)
	kind := ev.Kind
	if !kind.Valid() {
		kind = "unknown" // client input: never a label value as-is
	}
	p.metrics.Published(kind, publishOutcome(res, err), time.Since(start))
	return res, err
}

func (p *Publisher) publish(ctx context.Context, ev domain.Event) (PublishResult, error) {
	if ev.CreatedAt.IsZero() {
		ev.CreatedAt = p.now().UTC()
	}
	if err := validate(ev); err != nil {
		return PublishResult{}, err
	}
	if ev.Kind == domain.KindSOS {
		return p.publishSOS(ctx, ev)
	}

	select {
	case p.slots <- struct{}{}:
		defer func() { <-p.slots }()
	default:
		return PublishResult{}, ErrBusy
	}
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	return publishOnce(ctx, p.events, ev)
}

// publishSOS retries once on a log error. The eventId makes the retry safe:
// if the first attempt was stored, the second returns the same seq.
func (p *Publisher) publishSOS(ctx context.Context, ev domain.Event) (PublishResult, error) {
	ctx, cancel := context.WithTimeout(ctx, p.sosTimeout)
	defer cancel()
	res, err := publishOnce(ctx, p.sos, ev)
	if errors.Is(err, ErrUnavailable) && ctx.Err() == nil {
		p.metrics.SOSRetried()
		res, err = publishOnce(ctx, p.sos, ev)
	}
	return res, err
}

func publishOnce(ctx context.Context, log eventlog.Log, ev domain.Event) (PublishResult, error) {
	pos, dup, err := log.Publish(ctx, ev)
	if errors.Is(err, domain.ErrBadEvent) {
		return PublishResult{}, err
	}
	if err != nil {
		return PublishResult{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	res := PublishResult{Duplicate: dup}
	if ev.Kind.Reliable() {
		res.Seq = pos
	} else {
		res.Ver = pos
	}
	return res, nil
}

// validate adds the publisher's rules to Event.Validate: an SOS topic only
// carries SOS events, so security desks never get location traffic there.
func validate(ev domain.Event) error {
	if err := ev.Validate(); err != nil {
		return err
	}
	if ev.Topic.Kind == domain.TopicSOS && ev.Kind != domain.KindSOS {
		return fmt.Errorf("%w: only sos events may be published on an sos topic", domain.ErrBadEvent)
	}
	return nil
}
