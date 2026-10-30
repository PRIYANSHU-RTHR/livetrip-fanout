package fanout

import "github.com/PRIYANSHU-RTHR/livetrip-fanout/internal/domain"

// Metrics receives what connections observe. Methods are called on the
// hot path from many goroutines, so implementations must be cheap and safe
// for concurrent use. obs.Metrics implements it.
type Metrics interface {
	// Delivered is called after a live event was written to the client.
	// Catch-up events are not reported: they are old on purpose.
	Delivered(ev domain.Event)
	// ReliableQueued reports the reliable lane's depth after an enqueue.
	ReliableQueued(depth int)
	// LocationConflated is called when a location replaces one the client
	// had not been sent yet.
	LocationConflated()
}

type noMetrics struct{}

func (noMetrics) Delivered(domain.Event) {}
func (noMetrics) ReliableQueued(int)     {}
func (noMetrics) LocationConflated()     {}
