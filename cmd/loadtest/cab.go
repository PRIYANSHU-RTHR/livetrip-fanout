package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// api talks to the publisher through the load balancer.
type api struct {
	base string
	http *http.Client
	// sos has its own connection pool, like a cab's SOS button would, so
	// SOS requests never queue behind location traffic in this process.
	sos *http.Client
}

func newAPI(base string, maxConns int) *api {
	pool := func(n int) *http.Client {
		return &http.Client{
			Timeout: 5 * time.Second,
			Transport: &http.Transport{
				DialContext:         (&net.Dialer{Timeout: 2 * time.Second}).DialContext,
				MaxConnsPerHost:     n,
				MaxIdleConnsPerHost: n,
				IdleConnTimeout:     30 * time.Second,
			},
		}
	}
	return &api{base: base, http: pool(maxConns), sos: pool(16)}
}

type publishRequest struct {
	EventID   string          `json:"eventId"`
	Tenant    string          `json:"tenant"`
	CabID     string          `json:"cabId"`
	Topic     string          `json:"topic"`
	Kind      string          `json:"kind"`
	CreatedAt time.Time       `json:"createdAt"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

type publishResponse struct {
	Seq       uint64 `json:"seq"`
	Ver       uint64 `json:"ver"`
	Duplicate bool   `json:"duplicate"`
}

// errRetry marks a failure the cab should retry with the same eventId.
var errRetry = errors.New("retryable")

func (a *api) publish(ctx context.Context, path string, req publishRequest) (publishResponse, error) {
	client := a.http
	if path == "/v1/sos" {
		client = a.sos
	}
	body, _ := json.Marshal(req)
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, a.base+path, bytes.NewReader(body))
	if err != nil {
		return publishResponse{}, err
	}
	r.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(r)
	if err != nil {
		return publishResponse{}, fmt.Errorf("%w: %w", errRetry, err)
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated:
		var out publishResponse
		err := json.NewDecoder(resp.Body).Decode(&out)
		return out, err
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		return publishResponse{}, fmt.Errorf("%w: http %d", errRetry, resp.StatusCode)
	}
	return publishResponse{}, fmt.Errorf("http %d", resp.StatusCode)
}

// publishReliable retries a status or SOS until it is stored or ctx ends.
// The same eventId makes each retry safe: the log dedups it.
func (a *api) publishReliable(ctx context.Context, path string, req publishRequest, retries *atomic.Int64) (publishResponse, error) {
	backoff := 50 * time.Millisecond
	for {
		res, err := a.publish(ctx, path, req)
		if err == nil || !errors.Is(err, errRetry) {
			return res, err
		}
		retries.Add(1)
		select {
		case <-ctx.Done():
			return res, err
		case <-time.After(backoff + rand.N(backoff)):
		}
		backoff = min(backoff*2, time.Second)
	}
}

// expected records, per topic, the highest seq the publisher confirmed:
// every subscriber of the topic must end with at least this seq.
type expected struct {
	mu   sync.Mutex
	last map[string]uint64
}

func (e *expected) confirm(topic string, seq uint64) {
	e.mu.Lock()
	if seq > e.last[topic] {
		e.last[topic] = seq
	}
	e.mu.Unlock()
}

func (e *expected) get(topic string) uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.last[topic]
}

// pubStats counts what the cabs and the SOS generator sent.
type pubStats struct {
	locations, locFailed   atomic.Int64
	statuses, statusFailed atomic.Int64
	sos, sosFailed         atomic.Int64
	retries                atomic.Int64
}

// cab publishes a location every locEvery and a status every statusEvery
// to its own cab topic until ctx ends.
func runCab(ctx context.Context, cfg config, a *api, id int, exp *expected, st *pubStats) {
	cabID := "cab-" + strconv.Itoa(id)
	topic := cabTopic(cfg.tenant, id)
	// Spread cabs over the interval so they don't all fire together.
	select {
	case <-ctx.Done():
		return
	case <-time.After(rand.N(cfg.locEvery)):
	}
	loc := time.NewTicker(cfg.locEvery)
	defer loc.Stop()
	statusEvery := int(max(cfg.statusEvery/cfg.locEvery, 1))
	for n := 1; ; n++ {
		select {
		case <-ctx.Done():
			return
		case <-loc.C:
		}
		payload := fmt.Appendf(nil, `{"lat":%.5f,"lng":%.5f}`, 12.9+rand.Float64()/10, 77.5+rand.Float64()/10)
		_, err := a.publish(ctx, "/v1/events", publishRequest{
			EventID: cabID + "-l" + strconv.Itoa(n), Tenant: cfg.tenant, CabID: cabID,
			Topic: topic, Kind: "location", CreatedAt: time.Now(), Payload: payload,
		})
		// A lost location is fine: the next one replaces it.
		count(err, &st.locations, &st.locFailed)

		if n%statusEvery == 0 {
			res, err := a.publishReliable(ctx, "/v1/events", publishRequest{
				EventID: cabID + "-s" + strconv.Itoa(n), Tenant: cfg.tenant, CabID: cabID,
				Topic: topic, Kind: "status", CreatedAt: time.Now(),
				Payload: fmt.Appendf(nil, `{"status":"step-%d"}`, n/statusEvery),
			}, &st.retries)
			if count(err, &st.statuses, &st.statusFailed) {
				exp.confirm(topic, res.Seq)
			}
		}
	}
}

// runSOS raises an SOS every cfg.sosEvery, cycling over the offices.
// sent maps eventId → send time for the desks' latency measurement.
func runSOS(ctx context.Context, cfg config, a *api, sent *sync.Map, exp *expected, st *pubStats) {
	t := time.NewTicker(cfg.sosEvery)
	defer t.Stop()
	for n := 0; ; n++ {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		office := n % cfg.offices
		cab := rand.N(cfg.cabs)
		topic := sosTopic(cfg.tenant, office)
		id := "sos-" + strconv.Itoa(n)
		now := time.Now()
		sent.Store(id, now)
		// Not tied to ctx: an SOS started just before the end still lands.
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		res, err := a.publishReliable(sctx, "/v1/sos", publishRequest{
			EventID: id, Tenant: cfg.tenant, CabID: "cab-" + strconv.Itoa(cab),
			Topic: topic, Kind: "sos", CreatedAt: now,
		}, &st.retries)
		cancel()
		if count(err, &st.sos, &st.sosFailed) {
			exp.confirm(topic, res.Seq)
		}
	}
}

func count(err error, ok, failed *atomic.Int64) bool {
	if err != nil {
		failed.Add(1)
		return false
	}
	ok.Add(1)
	return true
}

type tokenRequest struct {
	Tenant     string `json:"tenant"`
	Sub        string `json:"sub"`
	Role       string `json:"role"`
	OfficeID   string `json:"officeId,omitempty"`
	TTLSeconds int    `json:"ttlSeconds"`
}

// token mints a test token from the dev endpoint.
func (a *api) token(ctx context.Context, req tokenRequest) (string, error) {
	body, _ := json.Marshal(req)
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, a.base+"/v1/dev/token", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	resp, err := a.http.Do(r)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("dev token: http %d (is DEV_TOKENS=true?)", resp.StatusCode)
	}
	var out struct {
		Token string `json:"token"`
	}
	err = json.NewDecoder(resp.Body).Decode(&out)
	return out.Token, err
}

func cabTopic(tenant string, id int) string {
	return "t:" + tenant + ":cab:" + "cab-" + strconv.Itoa(id)
}

func sosTopic(tenant string, office int) string {
	return "t:" + tenant + ":sos:" + officeID(office)
}

func officeID(n int) string { return "office-" + strconv.Itoa(n) }
