// Command loadtest drives the running stack (make up) through the load
// balancer: N websocket clients, M cabs publishing locations and statuses,
// an SOS every few seconds to security desks, and optionally a gateway
// killed mid-run. It reports throughput and latency (SOS p50/p99) and
// verifies that every client received every reliable seq of its topic.
//
// Exit status is 1 if a client missed a reliable event or SOS p99 is over
// the SLO.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type config struct {
	baseURL     string
	clients     int
	desks       int
	offices     int
	cabs        int
	locEvery    time.Duration
	statusEvery time.Duration
	sosEvery    time.Duration
	ramp        time.Duration
	duration    time.Duration
	drain       time.Duration
	kill        string
	killAt      time.Duration
	restore     bool
	compose     string
	httpConns   int
	sosSLO      time.Duration
	out         string
	tenant      string
}

func parseFlags(args []string) (config, error) {
	var c config
	fs := flag.NewFlagSet("loadtest", flag.ContinueOnError)
	fs.StringVar(&c.baseURL, "url", "http://localhost:8080", "load balancer base URL")
	fs.IntVar(&c.clients, "clients", 10_000, "websocket clients in total (screens + desks)")
	fs.IntVar(&c.desks, "desks", 100, "of those, security desks watching SOS topics")
	fs.IntVar(&c.offices, "offices", 10, "offices; desks and SOS are spread over them")
	fs.IntVar(&c.cabs, "cabs", 2_000, "publishing cabs")
	fs.DurationVar(&c.locEvery, "loc-every", time.Second, "location interval per cab")
	fs.DurationVar(&c.statusEvery, "status-every", 5*time.Second, "status interval per cab")
	fs.DurationVar(&c.sosEvery, "sos-every", 2*time.Second, "interval between SOS events")
	fs.DurationVar(&c.ramp, "ramp", 20*time.Second, "time to open all connections")
	fs.DurationVar(&c.duration, "duration", 60*time.Second, "publishing time")
	fs.DurationVar(&c.drain, "drain", 15*time.Second, "max wait for clients to catch up after publishing stops")
	fs.StringVar(&c.kill, "kill", "", "compose service to kill during the run, e.g. gateway-2")
	fs.DurationVar(&c.killAt, "kill-at", 30*time.Second, "when to kill, after publishing starts")
	fs.BoolVar(&c.restore, "restore", true, "start the killed service again at the end")
	fs.StringVar(&c.compose, "compose", "deploy/docker-compose.yml", "compose file for --kill")
	fs.IntVar(&c.httpConns, "http-conns", 256, "max HTTP connections for cab publishes")
	fs.DurationVar(&c.sosSLO, "sos-slo", 2*time.Second, "SOS latency objective (p99)")
	fs.StringVar(&c.out, "out", "", "also write the report as JSON to this file")
	if err := fs.Parse(args); err != nil {
		return c, err
	}
	switch {
	case c.clients <= c.desks || c.desks < 1:
		return c, errors.New("need 1 <= desks < clients")
	case c.cabs < 1 || c.offices < 1:
		return c, errors.New("need at least one cab and one office")
	case c.locEvery <= 0 || c.statusEvery <= 0 || c.sosEvery <= 0:
		return c, errors.New("intervals must be positive")
	}
	// A fresh tenant per run: every topic starts at seq 1.
	c.tenant = "lt" + strconv.FormatInt(time.Now().Unix(), 36)
	return c, nil
}

func main() {
	cfg, err := parseFlags(os.Args[1:])
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "loadtest:", err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	rep, err := run(ctx, cfg, os.Stdout)
	if err != nil {
		fmt.Fprintln(os.Stderr, "loadtest:", err)
		os.Exit(2)
	}
	if !rep.Pass {
		os.Exit(1)
	}
}

func logf(w io.Writer, format string, args ...any) {
	fmt.Fprintf(w, "%s  "+format+"\n", append([]any{time.Now().Format("15:04:05.000")}, args...)...)
}

func run(ctx context.Context, cfg config, w io.Writer) (report, error) {
	a := newAPI(strings.TrimRight(cfg.baseURL, "/"), cfg.httpConns)
	wsURL := "ws" + strings.TrimPrefix(a.base, "http") + "/ws"

	// One operator token for the screens and one security token per office.
	screenTok, err := a.token(ctx, tokenRequest{Tenant: cfg.tenant, Sub: "screen", Role: "operator", TTLSeconds: 7200})
	if err != nil {
		return report{}, err
	}
	deskTok := make([]string, cfg.offices)
	for o := range deskTok {
		deskTok[o], err = a.token(ctx, tokenRequest{Tenant: cfg.tenant, Sub: "desk-" + strconv.Itoa(o),
			Role: "security", OfficeID: officeID(o), TTLSeconds: 7200})
		if err != nil {
			return report{}, err
		}
	}

	sent := &sync.Map{}
	stats := newClientStats(sent)
	clients := make([]*client, cfg.clients)
	for i := range clients {
		c := &client{url: wsURL, stats: stats, seq: newSeqTracker(), done: make(chan struct{})}
		if i < cfg.desks {
			c.desk, c.token, c.topic = true, deskTok[i%cfg.offices], sosTopic(cfg.tenant, i%cfg.offices)
		} else {
			c.token, c.topic = screenTok, cabTopic(cfg.tenant, i%cfg.cabs)
		}
		clients[i] = c
	}

	// Phase 1: open every connection, spread over the ramp.
	logf(w, "tenant %s: connecting %d clients (%d desks) to %s over %s", cfg.tenant, cfg.clients, cfg.desks, wsURL, cfg.ramp)
	clientCtx, stopClients := context.WithCancel(context.Background())
	defer stopClients()
	var ready sync.WaitGroup
	var readyFailed int64
	var mu sync.Mutex
	ready.Add(len(clients))
	gap := cfg.ramp / time.Duration(len(clients))
	rampStart := time.Now()
	for i, c := range clients {
		if !sleep(ctx, time.Until(rampStart.Add(gap*time.Duration(i)))) {
			break
		}
		go c.run(clientCtx, func(ok bool) {
			if !ok {
				mu.Lock()
				readyFailed++
				mu.Unlock()
			}
			ready.Done()
		})
	}
	if ctx.Err() != nil {
		return report{}, ctx.Err()
	}
	waitOrTimeout(&ready, 30*time.Second)
	logf(w, "connected %d/%d in %s (%d first dials failed and kept retrying)",
		stats.connected.Load(), cfg.clients, time.Since(rampStart).Round(time.Millisecond), readyFailed)

	// Phase 2: publish, and kill a node part way through.
	logf(w, "publishing: %d cabs, location every %s, status every %s, SOS every %s, for %s",
		cfg.cabs, cfg.locEvery, cfg.statusEvery, cfg.sosEvery, cfg.duration)
	exp := &expected{last: map[string]uint64{}}
	pst := &pubStats{}
	pubCtx, stopPub := context.WithTimeout(ctx, cfg.duration)
	defer stopPub()
	var pubs sync.WaitGroup
	for id := range cfg.cabs {
		pubs.Go(func() { runCab(pubCtx, cfg, a, id, exp, pst) })
	}
	pubs.Go(func() { runSOS(pubCtx, cfg, a, sent, exp, pst) })

	var killed time.Time
	if cfg.kill != "" {
		pubs.Go(func() {
			if !sleep(pubCtx, cfg.killAt) {
				return
			}
			logf(w, "killing %s (connected before: %d)", cfg.kill, stats.connected.Load())
			killed = time.Now()
			if out, err := compose(cfg, "kill", cfg.kill); err != nil {
				logf(w, "kill failed: %v: %s", err, out)
			}
		})
	}
	pubStart := time.Now()
	progress := time.NewTicker(10 * time.Second)
	for done := false; !done; {
		select {
		case <-pubCtx.Done():
			done = true
		case <-progress.C:
			logf(w, "T+%2.0fs connected=%d frames=%d locations=%d statuses=%d sos=%d retries=%d",
				time.Since(pubStart).Seconds(), stats.connected.Load(), stats.frames.Load(),
				pst.locations.Load(), pst.statuses.Load(), pst.sos.Load(), pst.retries.Load())
		}
	}
	progress.Stop()
	pubs.Wait()
	pubTime := time.Since(pubStart)

	// Phase 3: let clients catch up, then verify.
	logf(w, "publishing stopped; waiting up to %s for clients to catch up", cfg.drain)
	deadline := time.Now().Add(cfg.drain)
	for time.Now().Before(deadline) && behind(clients, exp) > 0 && ctx.Err() == nil {
		sleep(ctx, 200*time.Millisecond)
	}
	stopClients()
	for _, c := range clients {
		<-c.done
	}

	if cfg.kill != "" && cfg.restore && !killed.IsZero() {
		if out, err := compose(cfg, "start", cfg.kill); err != nil {
			logf(w, "restore failed: %v: %s", err, out)
		}
	}

	rep := buildReport(cfg, clients, exp, stats, pst, pubTime, killed)
	rep.print(w)
	if cfg.out != "" {
		b, _ := json.MarshalIndent(rep, "", "  ")
		if err := os.WriteFile(cfg.out, b, 0o644); err != nil {
			return rep, err
		}
	}
	return rep, nil
}

// behind counts clients that have not yet seen their topic's last seq.
func behind(clients []*client, exp *expected) int {
	n := 0
	for _, c := range clients {
		if c.last.Load() < exp.get(c.topic) {
			n++
		}
	}
	return n
}

func compose(cfg config, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return exec.CommandContext(ctx, "docker", append([]string{"compose", "-f", cfg.compose}, args...)...).CombinedOutput()
}

func waitOrTimeout(wg *sync.WaitGroup, d time.Duration) {
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(d):
	}
}
