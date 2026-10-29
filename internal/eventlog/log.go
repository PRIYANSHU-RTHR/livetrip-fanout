package eventlog

import (
	"context"
	"errors"

	"github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"
)

// ErrTrimmed means the requested position is older than what the log still
// keeps. The caller falls back to a Snapshot.
var ErrTrimmed = errors.New("position trimmed from log")

// Snapshot is the current state of a topic: the newest reliable event per
// (cab, kind), sorted by Seq. AsOfSeq is the topic's last Seq when the
// snapshot was taken; live events after it are delivered normally.
type Snapshot struct {
	AsOfSeq uint64
	Events  []domain.Event
}

// Log is the per-topic sequencer and replay log.
//
// Reliable events (status, SOS) get a gap-free Seq per topic starting at 1
// and are kept in a capped log. Location events get a Ver per topic and only
// the newest per cab is kept. Publishing the same eventId twice on a topic
// (within the dedup window) returns the first result and stores nothing.
type Log interface {
	// Publish validates and stores ev. pos is the assigned Seq for reliable
	// kinds and Ver for locations. dup is true if ev.ID was already seen on
	// ev.Topic; pos is then the original position.
	Publish(ctx context.Context, ev domain.Event) (pos uint64, dup bool, err error)

	// After returns the retained reliable events with Seq > seq, in order.
	// It returns ErrTrimmed if events right after seq are no longer kept.
	After(ctx context.Context, topic domain.Topic, seq uint64) ([]domain.Event, error)

	// FirstSeq is the oldest Seq still kept, or 0 if the topic has none.
	FirstSeq(ctx context.Context, topic domain.Topic) (uint64, error)

	// LatestLocation returns the newest location per cab on topic.
	LatestLocation(ctx context.Context, topic domain.Topic) ([]domain.Event, error)

	// Snapshot returns the topic's current state for clients too far behind
	// to replay.
	Snapshot(ctx context.Context, topic domain.Topic) (Snapshot, error)
}
