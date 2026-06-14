## Prerequisites

```bash
go version       # need Go 1.25+
docker --version # need Docker (only for Levels 2 & 3)
docker compose version
```
---

## Unit tests

This proves the *logic* — matching rules, histogram math, scoring — is
correct. This is what you run constantly while learning/editing.

```bash
make test
# or directly:
go test ./... -v
```

What each suite proves:

| Test | Proves |
|---|---|
| `internal/matching` `TestPriceTimePriority` | equal-price orders fill in FIFO (time) order |
| `internal/matching` `TestBetterPriceWins` | best price fills first |
| `internal/matching` `TestMarketIOC` | market remainder is canceled, never rests |
| `internal/matching` `TestCancel` / `TestRejections` | cancel + validation semantics |
| `internal/telemetry` `TestQuantileAccuracy` | p50/p90/p99 within 5% of truth |
| `internal/telemetry` `TestMergeEqualsSingle` | merging shard histograms == one big one |
| `internal/score` `TestMonotonicity` | worse on any axis ⇒ lower score |

Expected tail:
```
ok  	arena/internal/matching	0.00Xs
ok  	arena/internal/score	0.00Xs
ok  	arena/internal/telemetry	0.00Xs
```

You can also compile everything instead:
```bash
make build      # go build ./...
go vet ./...     # static checks, should print nothing
```

---

## Test one engine by hand 

Run an engine *alone* and poke it with `curl`. The best way to
understand the contract in `SPEC.md`.

```bash
# Build & run the Go reference engine on port 8080
cd examples/reference-engine
docker build -t ref-engine .
docker run --rm -p 8080:8080 ref-engine
```

```bash
# health
curl localhost:8080/health           # -> ok

# rest a buy, then cross it with a sell — watch the fill come back
curl -s localhost:8080/orders -d '{"id":"A","side":"buy","type":"limit","price":10000,"qty":10}'
curl -s localhost:8080/orders -d '{"id":"B","side":"buy","type":"limit","price":10000,"qty":5}'
curl -s localhost:8080/orders -d '{"id":"C","side":"sell","type":"limit","price":9900,"qty":12}'
#   C should be "filled" with fills [A:10 @10000, B:2 @10000]  <- price-time priority!

# top of book
curl -s localhost:8080/book

# cancel
curl -s -X DELETE localhost:8080/orders/B   # -> "canceled"
curl -s -X DELETE localhost:8080/orders/B   # -> "unknown" (already gone)
```

Swap `examples/reference-engine` for `examples/python-orderbook` and repeat —
**identical replies, different language.** That's the language-agnostic claim,
proven by hand.

---

## Full platform end-to-end

The whole pipeline: upload → sandbox → conformance → distributed load → live
score.

```bash
make up        # builds images, starts redis, timescaledb, gateway,
               # orchestrator, botfleet, ingester
```

Wait ~10s, then open **http://localhost:8090** in a browser. Then:

```bash
make demo      # uploads Go + Python + a 'chaos' engine, fires 3 benchmark
               # runs (150 bots, 40s each), and tails their status
```

**What you should see:**
- The web page: three runs marching `building → conformance → load → done`,
  live metrics ticking, then a ranked leaderboard.
- Expected order: **go-reference > python-orderbook > chaos** (chaos is
  deliberately slow, so its latency
  scores tank).
- Terminal: per-run status lines, ending with the final leaderboard JSON.

Watch the services think:
```bash
make logs                              # all services
docker compose logs -f orchestrator    # build + conformance + teardown
docker compose logs -f ingester        # "run X FINAL: score=… p99=… tps=…"
```

### Scale the bot fleet (the 'distributed' proof)
```bash
make fleet N=8         # 8 botfleet replicas
docker compose ps      # see 8 botfleet containers
# fire a bigger run and watch shards get claimed across replicas:
docker compose logs -f botfleet        # different container ids, each "shard k/total"
```
Bigger run via API:
```bash
# grab a submission id from `curl localhost:8090/api/runs` or the demo output
curl -s localhost:8090/api/runs -X POST -H 'Content-Type: application/json' \
  -d '{"submission_id":"<ID>","bots":2000,"duration_s":60,"max_rps":20000}'
```

### Verify the sandbox isolation (security claim)
```bash
# find the running submission container
docker ps --filter label=arena.run

# 1) it has NO internet route (internal network):
docker exec <container> sh -c 'wget -T2 -qO- http://example.com || echo "BLOCKED (correct)"'

# 2) it can NOT reach redis/postgres (control plane):
docker exec <container> sh -c 'nc -z -w2 redis 6379 && echo OPEN || echo "BLOCKED (correct)"'

# 3) read-only rootfs:
docker exec <container> sh -c 'touch /pwned 2>&1 || echo "READ-ONLY (correct)"'

# 4) capped & pinned (inspect):
docker inspect <container> --format \
  'cpus={{.HostConfig.NanoCpus}} cpuset={{.HostConfig.CpusetCpus}} mem={{.HostConfig.Memory}} pids={{.HostConfig.PidsLimit}} caps_dropped={{.HostConfig.CapDrop}}'
```

### Tear down
```bash
make clean     # stops everything, removes volumes, sweeps stray sandboxes
```
