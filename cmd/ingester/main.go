// ingester: folds telemetry deltas from every fleet shard into per-run
// aggregates, computes p50/p90/p99 latency, max sustained TPS (best
// one-second window with <1% errors), correctness, and the composite score;
// maintains the Redis leaderboard + live snapshot pub/sub; archives final
// results to TimescaleDB (best-effort — the platform stays up without it).
//
// Finalization is two-path: the happy path counts Final batches from all
// shards; a sweeper finalizes any run silent for 60s so a dead fleet replica
// can never wedge a verdict.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"

	"arena/internal/protocol"
	"arena/internal/score"
	"arena/internal/telemetry"
)

const (
	streamTele  = "telemetry.raw"
	group       = "ingest"
	chanEvents  = "events.leaderboard"
	keySnapshot = "leaderboard:snapshot"
	staleAfter  = 60 * time.Second
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

type runAgg struct {
	hist        map[int]int64
	secAcks     map[int64]int64
	secErrs     map[int64]int64
	sent        int64
	acked       int64
	errors      int64
	violations  int64
	samples     []string
	totalShards int
	finalShards map[int]bool
	lastSeen    time.Time
	finalized   bool
}

type ingester struct {
	rdb   *redis.Client
	pgDSN string
	mu    sync.Mutex
	runs  map[string]*runAgg
}

func main() {
	ing := &ingester{
		rdb:   redis.NewClient(&redis.Options{Addr: env("REDIS_ADDR", "localhost:6379")}),
		pgDSN: env("PG_DSN", ""),
		runs:  map[string]*runAgg{},
	}
	ctx := context.Background()
	host, _ := os.Hostname()
	ing.rdb.XGroupCreateMkStream(ctx, streamTele, group, "0")
	go ing.sweep(ctx)
	log.Printf("ingester %s up (pg=%v)", host, ing.pgDSN != "")

	for {
		res, err := ing.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: group, Consumer: host,
			Streams: []string{streamTele, ">"},
			Count:   64, Block: 5 * time.Second,
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
				var b protocol.TelemetryBatch
				if err := json.Unmarshal([]byte(msg.Values["payload"].(string)), &b); err == nil {
					ing.absorb(ctx, b)
				}
				ing.rdb.XAck(ctx, streamTele, group, msg.ID)
			}
		}
	}
}

func (ing *ingester) absorb(ctx context.Context, b protocol.TelemetryBatch) {
	ing.mu.Lock()
	agg, ok := ing.runs[b.RunID]
	if !ok {
		agg = &runAgg{
			hist: map[int]int64{}, secAcks: map[int64]int64{}, secErrs: map[int64]int64{},
			finalShards: map[int]bool{},
		}
		ing.runs[b.RunID] = agg
	}
	if agg.finalized {
		ing.mu.Unlock()
		return
	}
	telemetry.Merge(agg.hist, b.Hist)
	for s, c := range b.SecAcks {
		agg.secAcks[s] += c
	}
	for s, c := range b.SecErrs {
		agg.secErrs[s] += c
	}
	agg.sent += b.Sent
	agg.acked += b.Acked
	agg.errors += b.Errors
	agg.violations += b.Violations
	if len(agg.samples) < 10 {
		agg.samples = append(agg.samples, b.Samples...)
	}
	agg.totalShards = b.TotalShards
	agg.lastSeen = time.Now()
	if b.Final {
		agg.finalShards[b.Shard] = true
	}
	complete := agg.totalShards > 0 && len(agg.finalShards) >= agg.totalShards
	if complete {
		agg.finalized = true
	}
	ing.mu.Unlock()

	if complete {
		ing.finalize(ctx, b.RunID, agg)
	} else {
		ing.live(ctx, b.RunID, agg)
	}
}

// sweep finalizes runs whose telemetry went silent (lost shard / dead replica).
func (ing *ingester) sweep(ctx context.Context) {
	for range time.Tick(10 * time.Second) {
		var stale []string
		ing.mu.Lock()
		for id, agg := range ing.runs {
			if !agg.finalized && time.Since(agg.lastSeen) > staleAfter {
				agg.finalized = true
				stale = append(stale, id)
			}
		}
		ing.mu.Unlock()
		for _, id := range stale {
			log.Printf("run %s: telemetry stale >%s, finalizing with partial data", id, staleAfter)
			ing.finalize(ctx, id, ing.runs[id])
		}
	}
}

type metrics struct {
	p50, p90, p99 int64
	tps           float64
	correctness   float64
	errRate       float64
}

func compute(agg *runAgg) metrics {
	var m metrics
	m.p50 = telemetry.Quantile(agg.hist, 0.50)
	m.p90 = telemetry.Quantile(agg.hist, 0.90)
	m.p99 = telemetry.Quantile(agg.hist, 0.99)

	// Max sustained TPS: best 1s window whose error rate stayed under 1%.
	secs := make([]int64, 0, len(agg.secAcks))
	for s := range agg.secAcks {
		secs = append(secs, s)
	}
	sort.Slice(secs, func(i, j int) bool { return secs[i] < secs[j] })
	for _, s := range secs {
		a, e := float64(agg.secAcks[s]), float64(agg.secErrs[s])
		if a+e == 0 || e/(a+e) >= 0.01 {
			continue
		}
		if a > m.tps {
			m.tps = a
		}
	}
	if agg.acked > 0 {
		m.correctness = 1 - float64(agg.violations)/float64(agg.acked)
		if m.correctness < 0 {
			m.correctness = 0
		}
	}
	if agg.sent > 0 {
		m.errRate = float64(agg.errors) / float64(agg.sent)
	}
	return m
}

func (ing *ingester) writeResult(ctx context.Context, runID string, m metrics, agg *runAgg, sc float64, final bool) {
	fields := map[string]any{
		"p50_us": m.p50 / 1000, "p90_us": m.p90 / 1000, "p99_us": m.p99 / 1000,
		"tps": fmt.Sprintf("%.0f", m.tps),
		"correctness": fmt.Sprintf("%.4f", m.correctness),
		"err_rate":    fmt.Sprintf("%.4f", m.errRate),
		"sent":        agg.sent, "acked": agg.acked,
		"errors": agg.errors, "violations": agg.violations,
	}
	if len(agg.samples) > 0 {
		fields["violation_samples"] = strings.Join(agg.samples, " | ")
	}
	if final {
		fields["score"] = fmt.Sprintf("%.2f", sc)
	}
	ing.rdb.HSet(ctx, "result:"+runID, fields)
}

var liveThrottle sync.Map // runID -> last publish time

func (ing *ingester) live(ctx context.Context, runID string, agg *runAgg) {
	if t, ok := liveThrottle.Load(runID); ok && time.Since(t.(time.Time)) < time.Second {
		return
	}
	liveThrottle.Store(runID, time.Now())
	ing.mu.Lock()
	m := compute(agg)
	ing.mu.Unlock()
	ing.writeResult(ctx, runID, m, agg, 0, false)
}

func (ing *ingester) finalize(ctx context.Context, runID string, agg *runAgg) {
	ing.mu.Lock()
	m := compute(agg)
	ing.mu.Unlock()

	conf := 0.0
	if v, err := ing.rdb.HGet(ctx, "run:"+runID, "conformance_score").Result(); err == nil {
		conf, _ = strconv.ParseFloat(v, 64)
	}
	sc := score.Composite(m.p99, m.tps, m.correctness, conf)
	ing.writeResult(ctx, runID, m, agg, sc, true)
	ing.rdb.HSet(ctx, "run:"+runID, "status", "done", "finished", time.Now().Unix())

	subID, _ := ing.rdb.HGet(ctx, "run:"+runID, "submission_id").Result()
	if subID != "" {
		// Leaderboard keeps each submission's best run.
		cur, err := ing.rdb.ZScore(ctx, "leaderboard", subID).Result()
		if err != nil || sc >= cur {
			ing.rdb.ZAdd(ctx, "leaderboard", redis.Z{Score: sc, Member: subID})
			ing.rdb.Set(ctx, "best:"+subID, runID, 0)
		}
	}
	log.Printf("run %s FINAL: score=%.2f p50=%dµs p99=%dµs tps=%.0f correct=%.3f conf=%.2f acked=%d",
		runID, sc, m.p50/1000, m.p99/1000, m.tps, m.correctness, conf, agg.acked)

	ing.publishSnapshot(ctx)
	ing.persist(ctx, runID, subID, m, sc, agg)

	ing.mu.Lock()
	delete(ing.runs, runID)
	ing.mu.Unlock()
}

type boardRow struct {
	Rank         int     `json:"rank"`
	SubmissionID string  `json:"submission_id"`
	Team         string  `json:"team"`
	Name         string  `json:"name"`
	RunID        string  `json:"run_id"`
	Score        float64 `json:"score"`
	P50us        int64   `json:"p50_us"`
	P99us        int64   `json:"p99_us"`
	TPS          float64 `json:"tps"`
	Correctness  float64 `json:"correctness"`
}

func (ing *ingester) publishSnapshot(ctx context.Context) {
	members, err := ing.rdb.ZRevRangeWithScores(ctx, "leaderboard", 0, 24).Result()
	if err != nil {
		return
	}
	rows := make([]boardRow, 0, len(members))
	for i, z := range members {
		subID := z.Member.(string)
		sub, _ := ing.rdb.HGetAll(ctx, "submission:"+subID).Result()
		runID, _ := ing.rdb.Get(ctx, "best:"+subID).Result()
		res, _ := ing.rdb.HGetAll(ctx, "result:"+runID).Result()
		gi := func(k string) int64 { v, _ := strconv.ParseInt(res[k], 10, 64); return v }
		gf := func(k string) float64 { v, _ := strconv.ParseFloat(res[k], 64); return v }
		rows = append(rows, boardRow{
			Rank: i + 1, SubmissionID: subID,
			Team: sub["team"], Name: sub["name"], RunID: runID,
			Score: z.Score, P50us: gi("p50_us"), P99us: gi("p99_us"),
			TPS: gf("tps"), Correctness: gf("correctness"),
		})
	}
	payload, _ := json.Marshal(rows)
	ing.rdb.Set(ctx, keySnapshot, payload, 0)
	ing.rdb.Publish(ctx, chanEvents, payload)
}

// persist archives the final verdict to TimescaleDB. Best-effort by design:
// the leaderboard's source of truth is Redis; Timescale is the analytical
// history (grafana, post-hoc queries). A down DB never blocks a verdict.
func (ing *ingester) persist(ctx context.Context, runID, subID string, m metrics, sc float64, agg *runAgg) {
	if ing.pgDSN == "" {
		return
	}
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(cctx, ing.pgDSN)
	if err != nil {
		log.Printf("pg connect (skipping archive): %v", err)
		return
	}
	defer conn.Close(cctx)
	_, err = conn.Exec(cctx, `
		CREATE TABLE IF NOT EXISTS arena_runs (
			ts           TIMESTAMPTZ NOT NULL DEFAULT now(),
			run_id       TEXT NOT NULL,
			submission_id TEXT NOT NULL,
			p50_us BIGINT, p90_us BIGINT, p99_us BIGINT,
			tps DOUBLE PRECISION, correctness DOUBLE PRECISION,
			score DOUBLE PRECISION,
			sent BIGINT, acked BIGINT, errors BIGINT, violations BIGINT
		)`)
	if err == nil {
		// hypertable conversion is idempotent-ish; ignore "already a hypertable"
		conn.Exec(cctx, `SELECT create_hypertable('arena_runs','ts',if_not_exists=>TRUE)`)
		_, err = conn.Exec(cctx, `
			INSERT INTO arena_runs
			(run_id, submission_id, p50_us, p90_us, p99_us, tps, correctness, score, sent, acked, errors, violations)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
			runID, subID, m.p50/1000, m.p90/1000, m.p99/1000, m.tps, m.correctness, sc,
			agg.sent, agg.acked, agg.errors, agg.violations)
	}
	if err != nil {
		log.Printf("pg archive: %v", err)
	}
}
