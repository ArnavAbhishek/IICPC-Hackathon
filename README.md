# IICPC Hackathon Submission

**A distributed benchmarking & hosting platform for contestant-submitted
trading infrastructure**

Upload a matching engine as a Docker build context. The platform builds it,
hosts it in a hardened sandbox on an internet-less network, unleashes a
horizontally scalable fleet of trading bots that ramp limit orders, market
sweeps and cancels against it, validates every ack for price-time-priority
violations on the hot path, and streams a composite verdict [latency
percentiles, max sustained TPS, correctness] to a live WebSocket
leaderboard.


**code upload → sandboxed deploy → conformance probe → bot-fleet bombardment → live verdict**


## Quickstart (60 seconds)

Requires Docker + Compose. From the repo root:

```bash
make up      # build images, start the platform
make demo    # submit reference + chaos engines, fire runs, tail verdicts
```

Open **http://localhost:8090** — both runs move through
`building → conformance → load → done`, live metrics ticking, and the
reference engine beat the deliberately laggy, occasionally-wrong "chaos"
variant on the board.

Scale the attack:

```bash
make fleet N=8                  # 8 bot-fleet replicas
curl -X POST localhost:8090/api/runs -d '{"submission_id":"<id>","bots":2000,"duration_s":60,"max_rps":20000}'
```

## Repo map

| Path | What |
|---|---|
| `cmd/gateway` | Edge API: uploads, run control, leaderboard REST + WS, web UI |
| `cmd/orchestrator` | Sandbox lifecycle: build → hardened run → conformance probe → shard fan-out → teardown |
| `cmd/botfleet` | The load generator: seeded market-participant bots, in-line ack validation, mergeable telemetry deltas |
| `cmd/ingester` | Telemetry fold: p50/p90/p99, max sustained TPS, correctness, composite score, Timescale archive |
| `internal/matching` | Reference price-time-priority book — the correctness oracle (tested) |
| `internal/telemetry` | Mergeable log-bucket latency histogram (tested) |
| `internal/score` | Composite scoring (tested) |
| `examples/reference-engine` | A complete, passing contestant submission (Go) |
| `examples/python-orderbook` | A second passing submission (Python) — proves the platform is language-agnostic |
| `web/` | Live leaderboard UI |
| `deploy/k8s/` | Kubernetes manifests incl. botfleet HPA |
| `ARCHITECTURE.md` | System design: topology, decisions, isolation, failure modes |
| `SPEC.md` | The API contract contestants implement |
| `TESTING.md` | How to test - unit tests, single engine, full e2e |

## Deliverables

1. **Working prototype** — the whole repo; `make up && make demo` runs the
   full pipeline end-to-end on one machine.
2. **Architecture blueprint** — [`ARCHITECTURE.md`](ARCHITECTURE.md).
3. **IaC** — [`docker-compose.yml`](docker-compose.yml) (local/single-node,
   fleet scales with `--scale`) and [`deploy/k8s/arena.yaml`](deploy/k8s/arena.yaml)
   (cluster: Deployments, StatefulSet, LoadBalancer, botfleet HPA 2→32).

## Submitting your own engine

Implement the contract in [`SPEC.md`](SPEC.md) (any language — the platform
only sees a Dockerfile), then:

```bash
tar czf my-engine.tar.gz -C my-engine-dir .
curl -F team="Your Team" -F name="engine-v1" -F bundle=@my-engine.tar.gz \
     localhost:8090/api/submissions
```

## Tests

```bash
make test   # vets + unit tests: matching oracle, histogram math, score monotonicity
```
[`TESTING.md`](TESTING.md) walks the three test
levels (unit → single engine → full pipeline).
