// botfleet: the distributed load generator. Each replica claims load shards
// from a Redis Streams consumer group and runs N simulated market
// participants against the sandboxed engine: seeded random walks emitting
// limit orders, market sweeps and cancels over keep-alive HTTP, with a
// linear 10%->100% rate ramp so the ingester can find the saturation TPS.
//
// Every ack is validated in-line (id echo, overfill, limit-price bounds,
// status/fill consistency, per-observer seq monotonicity) — correctness is
// measured on the same hot path that measures latency, not in a separate
// slow pass. Telemetry leaves as mergeable deltas every 2s.
//
// Scale-out is trivial: `docker compose up --scale botfleet=8` (or an HPA on
// the k8s Deployment); shards are claimed competitively, no coordinator.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"arena/internal/protocol"
	"arena/internal/telemetry"
)

const (
	streamLoad = "load.jobs"
	streamTele = "telemetry.raw"
	group      = "fleet"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: env("REDIS_ADDR", "localhost:6379")})
	ctx := context.Background()
	host, _ := os.Hostname()
	rdb.XGroupCreateMkStream(ctx, streamLoad, group, "0")
	log.Printf("botfleet %s ready", host)

	for {
		res, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: group, Consumer: host,
			Streams: []string{streamLoad, ">"},
			Count:   1, Block: 5 * time.Second,
		}).Result()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			log.Printf("xreadgroup: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}
		for _, st := range res {
			for _, msg := range st.Messages {
				// Ack on claim (at-most-once). A replica dying mid-shard just
				// means less offered load; the ingester's stale-run sweep
				// still finalizes the verdict. Cheaper than XAUTOCLAIM
				// re-delivery, which would double-count telemetry.
				rdb.XAck(ctx, streamLoad, group, msg.ID)
				var shard protocol.LoadShard
				if err := json.Unmarshal([]byte(msg.Values["payload"].(string)), &shard); err != nil {
					log.Printf("bad shard payload: %v", err)
					continue
				}
				runShard(ctx, rdb, shard)
			}
		}
	}
}

// shardState aggregates telemetry across all bots of one shard.
type shardState struct {
	sent, acked, errors, violations atomic.Int64
	hist                            *telemetry.Hist
	mu                              sync.Mutex
	secAcks, secErrs                map[int64]int64
	samples                         []string
}

func (st *shardState) tickAck(sec int64)  { st.mu.Lock(); st.secAcks[sec]++; st.mu.Unlock() }
func (st *shardState) tickErr(sec int64)  { st.mu.Lock(); st.secErrs[sec]++; st.mu.Unlock() }
func (st *shardState) violate(msg string) {
	st.violations.Add(1)
	st.mu.Lock()
	if len(st.samples) < 10 {
		st.samples = append(st.samples, msg)
	}
	st.mu.Unlock()
}

func runShard(ctx context.Context, rdb *redis.Client, shard protocol.LoadShard) {
	log.Printf("run %s shard %d/%d: %d bots, %ds, budget %d rps",
		shard.RunID, shard.Shard+1, shard.TotalShards, shard.Bots, shard.DurationS, shard.MaxRPS)

	st := &shardState{
		hist:    telemetry.NewHist(),
		secAcks: map[int64]int64{},
		secErrs: map[int64]int64{},
	}
	transport := &http.Transport{
		MaxIdleConns:        shard.Bots + 16,
		MaxIdleConnsPerHost: shard.Bots + 16,
		IdleConnTimeout:     90 * time.Second,
	}
	cli := &http.Client{Transport: transport, Timeout: 2 * time.Second}

	start := time.Now()
	deadline := start.Add(time.Duration(shard.DurationS) * time.Second)
	var wg sync.WaitGroup
	for i := 0; i < shard.Bots; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			bot(cli, shard, idx, st, start, deadline)
		}(i)
	}

	// Flusher: deltas every 2s keep the leaderboard live without ever
	// shipping cumulative state (idempotent merge on the ingester side).
	var prev protocol.TelemetryBatch
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			flush(ctx, rdb, shard, st, &prev, false)
		case <-done:
			flush(ctx, rdb, shard, st, &prev, true)
			transport.CloseIdleConnections()
			log.Printf("run %s shard %d done: sent=%d acked=%d errs=%d viol=%d",
				shard.RunID, shard.Shard+1, st.sent.Load(), st.acked.Load(),
				st.errors.Load(), st.violations.Load())
			return
		}
	}
}

func flush(ctx context.Context, rdb *redis.Client, shard protocol.LoadShard,
	st *shardState, prev *protocol.TelemetryBatch, final bool) {

	st.mu.Lock()
	secAcks := st.secAcks
	secErrs := st.secErrs
	st.secAcks = map[int64]int64{}
	st.secErrs = map[int64]int64{}
	samples := st.samples
	st.samples = nil
	st.mu.Unlock()

	cur := protocol.TelemetryBatch{
		RunID: shard.RunID, Shard: shard.Shard, TotalShards: shard.TotalShards,
		Final:      final,
		Sent:       st.sent.Load(),
		Acked:      st.acked.Load(),
		Errors:     st.errors.Load(),
		Violations: st.violations.Load(),
		Hist:       st.hist.TakeDelta(),
		SecAcks:    secAcks,
		SecErrs:    secErrs,
		Samples:    samples,
	}
	delta := cur
	delta.Sent -= prev.Sent
	delta.Acked -= prev.Acked
	delta.Errors -= prev.Errors
	delta.Violations -= prev.Violations
	*prev = cur

	payload, _ := json.Marshal(delta)
	if err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: streamTele, Values: map[string]any{"payload": payload},
	}).Err(); err != nil {
		log.Printf("telemetry xadd: %v", err)
	}
}

// bot is one simulated market participant: a price random walk around the
// 100.00 mid, mixing resting limit orders, aggressive crossers, market
// sweeps and cancels of its own resting orders.
func bot(cli *http.Client, shard protocol.LoadShard, idx int, st *shardState, start, deadline time.Time) {
	rng := rand.New(rand.NewSource(shard.Seed + int64(idx)*1_000_003))
	mid := int64(10000 + rng.Intn(60) - 30)
	var resting []string
	var lastSeq int64
	perBotMax := float64(shard.MaxRPS) / float64(shard.Bots)
	durS := float64(shard.DurationS)
	n := 0

	for time.Now().Before(deadline) {
		// linear ramp 10% -> 100% of the per-bot budget
		frac := time.Since(start).Seconds() / durS
		rate := perBotMax * (0.10 + 0.90*frac)
		if rate < 0.5 {
			rate = 0.5
		}
		time.Sleep(time.Duration(float64(time.Second) / rate * (0.5 + rng.Float64())))

		mid += int64(rng.Intn(7)) - 3
		if mid < 9000 {
			mid = 9000
		}
		if mid > 11000 {
			mid = 11000
		}

		var verdicts []string
		switch p := rng.Float64(); {
		case p < 0.20 && len(resting) > 0: // cancel own resting order
			i := rng.Intn(len(resting))
			id := resting[i]
			resting = append(resting[:i], resting[i+1:]...)
			ack, err := send(cli, st, http.MethodDelete, shard.Target+"/orders/"+id, nil)
			if err != nil {
				continue
			}
			if ack.Status != protocol.StatusCanceled && ack.Status != protocol.StatusUnknown {
				verdicts = append(verdicts, fmt.Sprintf("cancel %s got status %q", id, ack.Status))
			}
			verdicts = append(verdicts, checkSeq(&lastSeq, ack)...)
		case p < 0.40: // market sweep
			o := protocol.Order{
				ID: fmt.Sprintf("b%d-%d-%d", shard.Shard, idx, n), Qty: 1 + int64(rng.Intn(50)),
				Side: side(rng), Type: protocol.Market,
			}
			n++
			ack, err := sendOrder(cli, st, shard.Target, o)
			if err != nil {
				continue
			}
			verdicts = append(verdicts, validate(o, ack)...)
			verdicts = append(verdicts, checkSeq(&lastSeq, ack)...)
		default: // limit order near the touch; some cross, some rest
			s := side(rng)
			price := mid - int64(rng.Intn(25)) + 5 // buys mostly passive, some cross
			if s == protocol.Sell {
				price = mid + int64(rng.Intn(25)) - 5
			}
			if price < 1 {
				price = 1
			}
			o := protocol.Order{
				ID: fmt.Sprintf("b%d-%d-%d", shard.Shard, idx, n), Qty: 1 + int64(rng.Intn(100)),
				Side: s, Type: protocol.Limit, Price: price,
			}
			n++
			ack, err := sendOrder(cli, st, shard.Target, o)
			if err != nil {
				continue
			}
			if (ack.Status == protocol.StatusResting || ack.Status == protocol.StatusPartial) && len(resting) < 50 {
				resting = append(resting, o.ID)
			}
			verdicts = append(verdicts, validate(o, ack)...)
			verdicts = append(verdicts, checkSeq(&lastSeq, ack)...)
		}
		for _, v := range verdicts {
			st.violate(v)
		}
	}
}

func side(rng *rand.Rand) protocol.Side {
	if rng.Intn(2) == 0 {
		return protocol.Buy
	}
	return protocol.Sell
}

func sendOrder(cli *http.Client, st *shardState, target string, o protocol.Order) (protocol.Ack, error) {
	body, _ := json.Marshal(o)
	ack, err := send(cli, st, http.MethodPost, target+"/orders", body)
	if err == nil && ack.OrderID != o.ID {
		st.violate(fmt.Sprintf("ack order_id %q for order %q", ack.OrderID, o.ID))
	}
	return ack, err
}

// send performs one request and owns the latency/error accounting:
// latency is wall time from write to fully-decoded ack.
func send(cli *http.Client, st *shardState, method, url string, body []byte) (protocol.Ack, error) {
	var ack protocol.Ack
	var rdr *bytes.Reader
	req, err := http.NewRequest(method, url, nil)
	if body != nil {
		rdr = bytes.NewReader(body)
		req, err = http.NewRequest(method, url, rdr)
		req.Header.Set("Content-Type", "application/json")
	}
	if err != nil {
		return ack, err
	}
	st.sent.Add(1)
	t0 := time.Now()
	resp, err := cli.Do(req)
	now := time.Now()
	if err != nil {
		st.errors.Add(1)
		st.tickErr(now.Unix())
		return ack, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		st.errors.Add(1)
		st.tickErr(now.Unix())
		return ack, fmt.Errorf("http %d", resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(&ack); err != nil {
		st.errors.Add(1)
		st.tickErr(now.Unix())
		return ack, err
	}
	lat := time.Since(t0)
	st.hist.Record(lat.Nanoseconds())
	st.acked.Add(1)
	st.tickAck(now.Unix())
	return ack, nil
}

// validate enforces the live correctness invariants on a single ack.
func validate(o protocol.Order, ack protocol.Ack) []string {
	var v []string
	var filled int64
	for _, f := range ack.Fills {
		filled += f.Qty
		if o.Type == protocol.Limit {
			if o.Side == protocol.Buy && f.Price > o.Price {
				v = append(v, fmt.Sprintf("%s: buy fill @%d above limit %d", o.ID, f.Price, o.Price))
			}
			if o.Side == protocol.Sell && f.Price < o.Price {
				v = append(v, fmt.Sprintf("%s: sell fill @%d below limit %d", o.ID, f.Price, o.Price))
			}
		}
	}
	if filled > o.Qty {
		v = append(v, fmt.Sprintf("%s: overfill %d > qty %d", o.ID, filled, o.Qty))
	}
	if ack.Status == protocol.StatusFilled && filled != o.Qty {
		v = append(v, fmt.Sprintf("%s: status filled but %d/%d", o.ID, filled, o.Qty))
	}
	return v
}

// checkSeq: a single observer must see strictly increasing engine sequence
// numbers across its own (serial) requests.
func checkSeq(last *int64, ack protocol.Ack) []string {
	if ack.Seq <= *last {
		old := *last
		*last = ack.Seq
		return []string{fmt.Sprintf("seq regression %d after %d", ack.Seq, old)}
	}
	*last = ack.Seq
	return nil
}
