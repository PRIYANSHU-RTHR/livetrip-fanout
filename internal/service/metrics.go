package service

import (
	"errors"
	"time"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/auth"
	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"
)

// Outcome labels reported to metrics. They are a small fixed set so a
// metric never gets a label per client or per error text.
const (
	OutcomeOK          = "ok"
	OutcomeDuplicate   = "duplicate"
	OutcomeInvalid     = "invalid"
	OutcomeBusy        = "busy"
	OutcomeUnavailable = "unavailable"
	OutcomeForbidden   = "forbidden"
	OutcomeLookup      = "lookup_failed"
	OutcomeClosed      = "closed" // the client went away mid-subscribe

	// ResumeSnapshotTrimmed is a snapshot given because the client's
	// lastSeq was already trimmed from the log: it was away too long.
	ResumeSnapshotTrimmed = "snapshot_trimmed"
)

// SubscribeMetrics records subscribe outcomes. obs.Metrics implements it.
type SubscribeMetrics interface {
	// Subscribed is called once per subscribe with an Outcome* label.
	Subscribed(outcome string)
	// Resumed is called for each successful subscribe with "replay",
	// "snapshot" or ResumeSnapshotTrimmed.
	Resumed(mode string)
}

// PublishMetrics records publish outcomes. obs.Metrics implements it.
type PublishMetrics interface {
	// Published is called once per publish with an Outcome* label and how
	// long it took, retries included.
	Published(kind domain.EventKind, outcome string, took time.Duration)
	// SOSRetried is called when an SOS publish is retried.
	SOSRetried()
}

type noMetrics struct{}

func (noMetrics) Subscribed(string)                                 {}
func (noMetrics) Resumed(string)                                    {}
func (noMetrics) Published(domain.EventKind, string, time.Duration) {}
func (noMetrics) SOSRetried()                                       {}

func subscribeOutcome(err error) string {
	switch {
	case err == nil:
		return OutcomeOK
	case errors.Is(err, domain.ErrForbidden):
		return OutcomeForbidden
	case errors.Is(err, auth.ErrLookupFailed):
		return OutcomeLookup
	case errors.Is(err, ErrUnavailable):
		return OutcomeUnavailable
	}
	// The connection closed, ctx ended or confirm could not write the
	// reply: all mean the client is gone.
	return OutcomeClosed
}

func publishOutcome(res PublishResult, err error) string {
	switch {
	case err == nil && res.Duplicate:
		return OutcomeDuplicate
	case err == nil:
		return OutcomeOK
	case errors.Is(err, domain.ErrBadEvent), errors.Is(err, domain.ErrBadTopic):
		return OutcomeInvalid
	case errors.Is(err, ErrBusy):
		return OutcomeBusy
	}
	return OutcomeUnavailable
}
