package main

import (
	"math"
	"slices"
	"sync"
	"time"
)

// latencies collects samples from many goroutines.
type latencies struct {
	mu      sync.Mutex
	samples []time.Duration
}

func (l *latencies) add(d time.Duration) {
	if d < 0 {
		d = 0
	}
	l.mu.Lock()
	l.samples = append(l.samples, d)
	l.mu.Unlock()
}

// summary is a percentile view of a latency sample.
type summary struct {
	Count int           `json:"count"`
	P50   time.Duration `json:"p50"`
	P99   time.Duration `json:"p99"`
	Max   time.Duration `json:"max"`
	// Over is how many samples exceeded the threshold passed to summarize.
	Over int `json:"over"`
}

func (l *latencies) summarize(threshold time.Duration) summary {
	l.mu.Lock()
	s := slices.Clone(l.samples)
	l.mu.Unlock()
	if len(s) == 0 {
		return summary{}
	}
	slices.Sort(s)
	over := len(s) - sortSearch(s, threshold)
	return summary{
		Count: len(s),
		P50:   percentile(s, 0.50),
		P99:   percentile(s, 0.99),
		Max:   s[len(s)-1],
		Over:  over,
	}
}

// sortSearch returns the index of the first sample above threshold.
func sortSearch(sorted []time.Duration, threshold time.Duration) int {
	i, _ := slices.BinarySearchFunc(sorted, threshold, func(d, t time.Duration) int {
		if d <= t {
			return -1
		}
		return 1
	})
	return i
}

// percentile uses the nearest-rank method on a sorted, non-empty sample.
func percentile(sorted []time.Duration, p float64) time.Duration {
	rank := int(math.Ceil(p*float64(len(sorted)))) - 1
	return sorted[max(rank, 0)]
}

// seqTracker checks one client's view of one topic: reliable events must
// arrive gap-free and in order; duplicates are dropped, as a real client
// would. It is used by a single goroutine.
type seqTracker struct {
	last uint64
	// inSnapshot is set after a snapshot resume until the first live event:
	// snapshot contents have seq <= last and are not duplicates.
	inSnapshot bool

	received int
	dups     int
	gaps     uint64 // seqs that never arrived
	skipped  uint64 // seqs jumped over by a snapshot resume (client away too long)
	resumes  map[string]int
	locVer   map[string]uint64
	locStale int // a location older than one already seen
}

func newSeqTracker() *seqTracker {
	return &seqTracker{resumes: map[string]int{}, locVer: map[string]uint64{}}
}

// subscribed applies the server's "subscribed" reply.
func (t *seqTracker) subscribed(mode string, asOfSeq uint64) {
	t.resumes[mode]++
	if mode != "snapshot" {
		return
	}
	if asOfSeq > t.last {
		if t.last > 0 {
			t.skipped += asOfSeq - t.last
		}
		t.last = asOfSeq
	}
	t.inSnapshot = true
}

// reliable applies one status or SOS event.
func (t *seqTracker) reliable(seq uint64) (fresh bool) {
	switch {
	case seq <= t.last:
		if !t.inSnapshot {
			t.dups++
		}
		return false
	case seq > t.last+1:
		t.gaps += seq - t.last - 1
	}
	t.inSnapshot = false
	t.last = seq
	t.received++
	return true
}

// location applies one location event; only newer versions count.
func (t *seqTracker) location(cabID string, ver uint64) (fresh bool) {
	prev := t.locVer[cabID]
	if ver < prev {
		t.locStale++
	}
	if ver <= prev {
		return false
	}
	t.locVer[cabID] = ver
	return true
}
