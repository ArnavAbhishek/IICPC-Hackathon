// orchestrator: turns a queued run into a live benchmark.
//
//	build submission image -> run it sandboxed on the isolated arena network
//	-> wait healthy -> deterministic conformance probe (price-time priority)
//	-> fan load shards out to the bot fleet -> await ingester verdict -> teardown.
//
// Container control is deliberately the docker CLI over the mounted socket
// rather than the Go SDK: the CLI is the stable, audited interface and the
// SDK's type surface churns across majors. Every sandbox gets --cap-drop ALL,
// no-new-privileges, a read-only rootfs, pid/memory/cpu quotas and a network
// with no internet route. See ARCHITECTURE.md "Isolation".
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"arena/internal/matching"
	"arena/internal/protocol"
)

const (
	streamRuns = "runs.jobs"
	streamLoad = "load.jobs"
	group      = "orch"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

type orch struct {
	rdb          *redis.Client
	dataDir      string
	arenaNet     string
	cpus, mem    string
	pids         string
	cores        int // host cores available for pinning submissions onto
	botsPerShard int
}

func main() {
	cores, _ := strconv.Atoi(env("SUB_CORES", strconv.Itoa(runtime.NumCPU())))
	if cores < 1 {
		cores = 1
	}
	o := &orch{
		rdb:          redis.NewClient(&redis.Options{Addr: env("REDIS_ADDR", "localhost:6379")}),
		dataDir:      env("DATA_DIR", "/data/submissions"),
		arenaNet:     env("ARENA_NET", "arena-net"),
		cpus:         env("SUB_CPUS", "1.0"),
		mem:          env("SUB_MEMORY", "512m"),
		pids:         env("SUB_PIDS", "256"),
		cores:        cores,
		botsPerShard: 250,
	}
	ctx := context.Background()
	host, _ := os.Hostname()
	o.rdb.XGroupCreateMkStream(ctx, streamRuns, group, "0")
	log.Printf("orchestrator %s up (net=%s cpus=%s mem=%s cores=%d)", host, o.arenaNet, o.cpus, o.mem, o.cores)

	for {
		res, err := o.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: group, Consumer: host,
			Streams: []string{streamRuns, ">"},
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
				var job protocol.RunJob
				if err := json.Unmarshal([]byte(msg.Values["payload"].(string)), &job); err != nil {
					log.Printf("bad job payload: %v", err)
				} else {
					o.handle(ctx, job)
				}
				o.rdb.XAck(ctx, streamRuns, group, msg.ID)
			}
		}
	}
}

func (o *orch) setRun(ctx context.Context, runID string, kv ...any) {
	o.rdb.HSet(ctx, "run:"+runID, kv)
}

func (o *orch) fail(ctx context.Context, runID, msg string) {
	log.Printf("run %s FAILED: %s", runID, msg)
	o.setRun(ctx, runID, "status", "failed", "error", msg, "finished", time.Now().Unix())
}

func docker(ctx context.Context, stdin *os.File, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "docker", args...)
	if stdin != nil {
		cmd.Stdin = stdin
	}
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	return out.String(), err
}

func tail(s string, n int) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

func (o *orch) handle(ctx context.Context, job protocol.RunJob) {
	runID := job.RunID
	img := "arena/sub:" + runID
	name := "arena-sub-" + runID
	log.Printf("run %s: building submission %s", runID, job.SubmissionID)
	o.setRun(ctx, runID, "status", "building", "started", time.Now().Unix())

	bundle, err := os.Open(filepath.Join(o.dataDir, job.SubmissionID+".tar.gz"))
	if err != nil {
		o.fail(ctx, runID, "bundle missing: "+err.Error())
		return
	}
	bctx, bcancel := context.WithTimeout(ctx, 5*time.Minute)
	out, err := docker(bctx, bundle, "build", "-t", img, "-")
	bundle.Close()
	bcancel()
	if err != nil {
		o.fail(ctx, runID, "image build failed:\n"+tail(out, 15))
		return
	}

	// Sandbox: hostile code gets quotas, no capabilities, no privilege
	// escalation, immutable rootfs and a network with no internet route.
	// --cpuset-cpus pins each submission to one core (chosen by a stable hash
	// of the submission id) so concurrent benchmarks land on different cores
	// and don't contend — fair, repeatable latency numbers. --cpus still caps
	// the quota; together: "this submission gets exactly core N, one core's
	// worth, and no more."
	pin := pinCore(job.SubmissionID, o.cores)
	rctx, rcancel := context.WithTimeout(ctx, time.Minute)
	out, err = docker(rctx, nil, "run", "-d",
		"--name", name,
		"--network", o.arenaNet,
		"--cpus", o.cpus,
		"--cpuset-cpus", pin,
		"--memory", o.mem,
		"--pids-limit", o.pids,
		"--cap-drop", "ALL",
		"--security-opt", "no-new-privileges:true",
		"--read-only",
		"--tmpfs", "/tmp:rw,size=64m",
		"--label", "arena.run="+runID,
		img)
	rcancel()
	defer o.teardown(name, img)
	if err != nil {
		o.fail(ctx, runID, "sandbox start failed:\n"+tail(out, 15))
		return
	}

	target := "http://" + name + ":8080"
	if !waitHealthy(target, 45*time.Second) {
		logs, _ := docker(ctx, nil, "logs", "--tail", "15", name)
		o.fail(ctx, runID, "engine never became healthy\n"+tail(logs, 15))
		return
	}

	log.Printf("run %s: conformance probe", runID)
	o.setRun(ctx, runID, "status", "conformance")
	confScore, detail := conformance(target)
	o.setRun(ctx, runID, "conformance_score", fmt.Sprintf("%.4f", confScore), "conformance_detail", detail)
	log.Printf("run %s: conformance %.0f%% (%s)", runID, confScore*100, detail)

	shards := int(math.Ceil(float64(job.Bots) / float64(o.botsPerShard)))
	o.setRun(ctx, runID, "status", "load", "total_shards", shards)
	log.Printf("run %s: dispatching %d shard(s), %d bots, %ds", runID, shards, job.Bots, job.DurationS)
	for i := 0; i < shards; i++ {
		bots := job.Bots / shards
		if i < job.Bots%shards {
			bots++
		}
		shard := protocol.LoadShard{
			RunID: runID, Target: target,
			Shard: i, TotalShards: shards,
			Bots: bots, DurationS: job.DurationS,
			MaxRPS: job.MaxRPS / shards,
			Seed:   time.Now().UnixNano() + int64(i)*7919,
		}
		payload, _ := json.Marshal(shard)
		o.rdb.XAdd(ctx, &redis.XAddArgs{Stream: streamLoad, Values: map[string]any{"payload": payload}})
	}

	// The ingester owns the verdict; we babysit the sandbox until it lands.
	deadline := time.Now().Add(time.Duration(job.DurationS)*time.Second + 2*time.Minute)
	for time.Now().Before(deadline) {
		time.Sleep(2 * time.Second)
		status, _ := o.rdb.HGet(ctx, "run:"+runID, "status").Result()
		if status == "done" || status == "failed" {
			log.Printf("run %s: %s, tearing sandbox down", runID, status)
			return
		}
	}
	o.fail(ctx, runID, "run timed out waiting for telemetry verdict")
}

// pinCore maps a submission id to a CPU core in [0, cores) by a stable
// FNV-ish hash, so the same submission always benchmarks on the same core
// and different submissions spread across cores.
func pinCore(submissionID string, cores int) string {
	h := 0
	for _, c := range submissionID {
		h = (h*31 + int(c)) & 0x7fffffff
	}
	return strconv.Itoa(h % cores)
}

func (o *orch) teardown(name, img string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	docker(ctx, nil, "rm", "-f", name)
	docker(ctx, nil, "rmi", "-f", img)
}

func waitHealthy(target string, timeout time.Duration) bool {
	cli := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if resp, err := cli.Get(target + "/health"); err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return true
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return false
}

// --- conformance probe --------------------------------------------------
//
// A fixed scenario covering time priority at equal price, partial fills,
// IOC market sweeps and cancel semantics is replayed against both the
// reference book and the contestant engine; acks are diffed field by field.

type step struct {
	cancel string // order id to cancel; empty means submit Order
	order  protocol.Order
}

func scenario() []step {
	o := func(id string, side protocol.Side, typ protocol.OrdType, price, qty int64) step {
		return step{order: protocol.Order{ID: id, Side: side, Type: typ, Price: price, Qty: qty}}
	}
	return []step{
		o("conf-A", protocol.Buy, protocol.Limit, 10000, 10),
		o("conf-B", protocol.Buy, protocol.Limit, 10000, 5), // behind A in time priority
		o("conf-C", protocol.Sell, protocol.Limit, 9900, 12), // must fill A fully, then B
		o("conf-D", protocol.Sell, protocol.Limit, 10100, 10),
		o("conf-E", protocol.Buy, protocol.Market, 0, 4), // takes D
		{cancel: "conf-D"},
		o("conf-F", protocol.Buy, protocol.Limit, 10100, 3),
		o("conf-G", protocol.Sell, protocol.Market, 0, 100), // sweeps F then B remainder, IOC rest
		o("conf-H", protocol.Sell, protocol.Limit, 0, 5),    // invalid price: must reject
		{cancel: "conf-D"},                                  // already gone: must be unknown
	}
}

func conformance(target string) (float64, string) {
	ref := matching.New()
	cli := &http.Client{Timeout: 5 * time.Second}
	passed, checks := 0, 0
	var notes []string

	for i, st := range scenario() {
		var want, got protocol.Ack
		var err error
		if st.cancel != "" {
			want = ref.Cancel(st.cancel)
			got, err = doCancel(cli, target, st.cancel)
		} else {
			want = ref.Submit(st.order)
			got, err = doOrder(cli, target, st.order)
		}
		checks += 3
		if err != nil {
			notes = append(notes, fmt.Sprintf("step %d: %v", i, err))
			continue
		}
		if got.Status == want.Status {
			passed++
		} else {
			notes = append(notes, fmt.Sprintf("step %d: status %q want %q", i, got.Status, want.Status))
		}
		if sumFills(got.Fills) == sumFills(want.Fills) {
			passed++
		} else {
			notes = append(notes, fmt.Sprintf("step %d: filled %d want %d", i, sumFills(got.Fills), sumFills(want.Fills)))
		}
		if fillsEqual(got.Fills, want.Fills) {
			passed++
		} else {
			notes = append(notes, fmt.Sprintf("step %d: fill sequence mismatch (price-time priority?)", i))
		}
	}
	detail := "all checks passed"
	if len(notes) > 0 {
		if len(notes) > 5 {
			notes = notes[:5]
		}
		detail = strings.Join(notes, "; ")
	}
	return float64(passed) / float64(checks), detail
}

func sumFills(fs []protocol.Fill) int64 {
	var s int64
	for _, f := range fs {
		s += f.Qty
	}
	return s
}

func fillsEqual(a, b []protocol.Fill) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Price != b[i].Price || a[i].Qty != b[i].Qty || a[i].MakerID != b[i].MakerID {
			return false
		}
	}
	return true
}

func doOrder(cli *http.Client, target string, o protocol.Order) (protocol.Ack, error) {
	body, _ := json.Marshal(o)
	resp, err := cli.Post(target+"/orders", "application/json", bytes.NewReader(body))
	return decodeAck(resp, err)
}

func doCancel(cli *http.Client, target, id string) (protocol.Ack, error) {
	req, _ := http.NewRequest(http.MethodDelete, target+"/orders/"+id, nil)
	resp, err := cli.Do(req)
	return decodeAck(resp, err)
}

func decodeAck(resp *http.Response, err error) (protocol.Ack, error) {
	var ack protocol.Ack
	if err != nil {
		return ack, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return ack, fmt.Errorf("http %d", resp.StatusCode)
	}
	return ack, json.NewDecoder(resp.Body).Decode(&ack)
}
