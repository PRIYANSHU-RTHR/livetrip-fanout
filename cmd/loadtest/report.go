package main

import (
	"fmt"
	"io"
	"maps"
	"runtime"
	"slices"
	"strings"
	"time"
)

type report struct {
	Tenant    string        `json:"tenant"`
	Machine   string        `json:"machine"`
	Clients   int           `json:"clients"`
	Desks     int           `json:"desks"`
	Cabs      int           `json:"cabs"`
	Published time.Duration `json:"publishedFor"`
	Killed    string        `json:"killed,omitempty"`

	Publishes struct {
		Locations, LocationsFailed int64
		Statuses, StatusesFailed   int64
		SOS, SOSFailed             int64
		Retries                    int64
		PerSecond                  float64
	} `json:"publishes"`

	FramesReceived  int64   `json:"framesReceived"`
	FramesPerSecond float64 `json:"framesPerSecond"`

	// SOS is send → received by a desk, for every desk that watches it.
	SOS     summary            `json:"sos"`
	ByKind  map[string]summary `json:"e2eByKind"`
	SOSSLO  time.Duration      `json:"sosSlo"`
	Reconn  summary            `json:"reconnect"`
	Closes  map[string]int     `json:"closes"`
	Errors  map[string]int     `json:"errors,omitempty"`
	Resumes map[string]int     `json:"resumes"`

	Verify struct {
		Complete       int    `json:"complete"`
		Incomplete     int    `json:"incomplete"`
		MissingSeqs    uint64 `json:"missingSeqs"`
		SnapshotSkips  uint64 `json:"snapshotSkips"`
		Duplicates     int    `json:"duplicatesDropped"`
		StaleLocations int    `json:"staleLocations"`
		ReliableEvents int    `json:"reliableEventsDelivered"`
	} `json:"verify"`

	Pass     bool     `json:"pass"`
	Failures []string `json:"failures,omitempty"`
}

func buildReport(cfg config, clients []*client, exp *expected, st *clientStats, pst *pubStats,
	pubTime time.Duration, killed time.Time) report {
	r := report{
		Tenant: cfg.tenant, Clients: cfg.clients, Desks: cfg.desks, Cabs: cfg.cabs,
		Published: pubTime.Round(time.Millisecond), SOSSLO: cfg.sosSLO,
		Machine: fmt.Sprintf("%s/%s, %d CPUs", runtime.GOOS, runtime.GOARCH, runtime.NumCPU()),
	}
	if !killed.IsZero() {
		r.Killed = cfg.kill
	}
	p := &r.Publishes
	p.Locations, p.LocationsFailed = pst.locations.Load(), pst.locFailed.Load()
	p.Statuses, p.StatusesFailed = pst.statuses.Load(), pst.statusFailed.Load()
	p.SOS, p.SOSFailed = pst.sos.Load(), pst.sosFailed.Load()
	p.Retries = pst.retries.Load()
	p.PerSecond = float64(p.Locations+p.Statuses+p.SOS) / pubTime.Seconds()

	r.FramesReceived = st.frames.Load()
	r.FramesPerSecond = float64(r.FramesReceived) / pubTime.Seconds()
	r.SOS = st.sosDesk.summarize(cfg.sosSLO)
	r.ByKind = map[string]summary{}
	for k, l := range st.latency {
		r.ByKind[k] = l.summarize(cfg.sosSLO)
	}
	r.Reconn = st.reconnect.summarize(cfg.sosSLO)
	r.Closes, r.Errors = st.closes, st.errored

	r.Resumes = map[string]int{}
	v := &r.Verify
	for _, c := range clients {
		t := c.seq
		for m, n := range t.resumes {
			r.Resumes[m] += n
		}
		v.MissingSeqs += t.gaps
		v.SnapshotSkips += t.skipped
		v.Duplicates += t.dups
		v.StaleLocations += t.locStale
		v.ReliableEvents += t.received
		if t.gaps == 0 && t.skipped == 0 && t.last >= exp.get(c.topic) {
			v.Complete++
		} else {
			v.Incomplete++
		}
	}

	if v.Incomplete > 0 {
		r.Failures = append(r.Failures, fmt.Sprintf("%d clients missed reliable events", v.Incomplete))
	}
	if p.StatusesFailed+p.SOSFailed > 0 {
		r.Failures = append(r.Failures, fmt.Sprintf("%d reliable publishes failed", p.StatusesFailed+p.SOSFailed))
	}
	if r.SOS.Count == 0 {
		r.Failures = append(r.Failures, "no SOS was delivered")
	} else if r.SOS.P99 > cfg.sosSLO {
		r.Failures = append(r.Failures, fmt.Sprintf("SOS p99 %s is over %s", r.SOS.P99, cfg.sosSLO))
	}
	r.Pass = len(r.Failures) == 0
	return r
}

func (r report) print(w io.Writer) {
	ms := func(d time.Duration) string { return fmt.Sprintf("%.1fms", float64(d)/float64(time.Millisecond)) }
	line := func(name string, s summary) {
		fmt.Fprintf(w, "  %-22s n=%-9d p50=%-10s p99=%-10s max=%-10s >%s: %d\n",
			name, s.Count, ms(s.P50), ms(s.P99), ms(s.Max), r.SOSSLO, s.Over)
	}
	fmt.Fprintln(w, strings.Repeat("=", 78))
	fmt.Fprintf(w, "LOAD TEST  %s  tenant=%s\n", r.Machine, r.Tenant)
	fmt.Fprintf(w, "  clients=%d (desks=%d)  cabs=%d  publishing=%s  killed=%s\n",
		r.Clients, r.Desks, r.Cabs, r.Published, orNone(r.Killed))
	p := r.Publishes
	fmt.Fprintf(w, "Publishes  %.0f/s  locations=%d (failed %d)  statuses=%d (failed %d)  sos=%d (failed %d)  retries=%d\n",
		p.PerSecond, p.Locations, p.LocationsFailed, p.Statuses, p.StatusesFailed, p.SOS, p.SOSFailed, p.Retries)
	fmt.Fprintf(w, "Delivered  %d frames, %.0f/s\n", r.FramesReceived, r.FramesPerSecond)
	fmt.Fprintln(w, "Latency")
	line("SOS send -> desk", r.SOS)
	for _, k := range []string{"sos", "status", "location"} {
		line("e2e "+k, r.ByKind[k])
	}
	line("reconnect -> resumed", r.Reconn)
	fmt.Fprintf(w, "Closes     %s\n", kv(r.Closes))
	fmt.Fprintf(w, "Resumes    %s\n", kv(r.Resumes))
	if len(r.Errors) > 0 {
		fmt.Fprintf(w, "Errors     %s\n", kv(r.Errors))
	}
	v := r.Verify
	fmt.Fprintf(w, "Verify     complete=%d incomplete=%d missing=%d snapshotSkips=%d reliableDelivered=%d dupsDropped=%d staleLocations=%d\n",
		v.Complete, v.Incomplete, v.MissingSeqs, v.SnapshotSkips, v.ReliableEvents, v.Duplicates, v.StaleLocations)
	if r.Pass {
		fmt.Fprintln(w, "RESULT     PASS")
	} else {
		fmt.Fprintf(w, "RESULT     FAIL: %s\n", strings.Join(r.Failures, "; "))
	}
	fmt.Fprintln(w, strings.Repeat("=", 78))
}

func kv(m map[string]int) string {
	if len(m) == 0 {
		return "none"
	}
	var b strings.Builder
	for _, k := range slices.Sorted(maps.Keys(m)) {
		fmt.Fprintf(&b, "%s=%d ", k, m[k])
	}
	return strings.TrimSpace(b.String())
}

func orNone(s string) string {
	if s == "" {
		return "none"
	}
	return s
}
