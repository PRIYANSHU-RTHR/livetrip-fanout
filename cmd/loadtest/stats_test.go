package main

import (
	"testing"
	"time"
)

func TestPercentile(t *testing.T) {
	var l latencies
	for i := 1; i <= 100; i++ {
		l.add(time.Duration(i) * time.Millisecond)
	}
	l.add(-time.Second) // clock skew clamps to 0

	s := l.summarize(90 * time.Millisecond)
	if s.Count != 101 || s.P50 != 50*time.Millisecond || s.P99 != 99*time.Millisecond ||
		s.Max != 100*time.Millisecond || s.Over != 10 {
		t.Fatalf("summary = %+v", s)
	}
	if (&latencies{}).summarize(time.Second) != (summary{}) {
		t.Fatal("empty sample should give a zero summary")
	}
}

func TestSeqTracker(t *testing.T) {
	tests := []struct {
		name  string
		steps func(*seqTracker)
		last  uint64
		gaps  uint64
		skip  uint64
		dups  int
	}{
		{"in order", func(s *seqTracker) {
			s.subscribed("snapshot", 0)
			s.reliable(1)
			s.reliable(2)
			s.reliable(3)
		}, 3, 0, 0, 0},
		{"duplicate dropped", func(s *seqTracker) {
			s.subscribed("snapshot", 0)
			s.reliable(1)
			s.reliable(1)
			s.reliable(2)
		}, 2, 0, 0, 1},
		{"gap counted", func(s *seqTracker) {
			s.subscribed("snapshot", 0)
			s.reliable(1)
			s.reliable(4)
		}, 4, 2, 0, 0},
		{"replay after reconnect continues", func(s *seqTracker) {
			s.subscribed("snapshot", 0)
			s.reliable(1)
			s.subscribed("replay", 3)
			s.reliable(2)
			s.reliable(3)
			s.reliable(4)
		}, 4, 0, 0, 0},
		{"snapshot contents are not duplicates", func(s *seqTracker) {
			s.subscribed("snapshot", 5)
			s.reliable(3)
			s.reliable(5)
			s.reliable(6)
		}, 6, 0, 0, 0},
		{"snapshot after being away too long is a skip", func(s *seqTracker) {
			s.subscribed("snapshot", 0)
			s.reliable(1)
			s.subscribed("snapshot", 9)
			s.reliable(10)
		}, 10, 0, 8, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newSeqTracker()
			tt.steps(s)
			if s.last != tt.last || s.gaps != tt.gaps || s.skipped != tt.skip || s.dups != tt.dups {
				t.Fatalf("last=%d gaps=%d skipped=%d dups=%d", s.last, s.gaps, s.skipped, s.dups)
			}
		})
	}
}

func TestSeqTrackerLocations(t *testing.T) {
	s := newSeqTracker()
	if !s.location("cab-1", 2) || s.location("cab-1", 2) || s.location("cab-1", 1) || !s.location("cab-2", 1) {
		t.Fatal("only newer versions per cab are fresh")
	}
	if s.locStale != 1 {
		t.Fatalf("locStale = %d, want 1", s.locStale)
	}
}
