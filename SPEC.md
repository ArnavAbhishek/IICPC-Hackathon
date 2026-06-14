# Contestant Engine Contract

The engine is a **gzipped tar build context** with a `Dockerfile` at its
root. The platform builds it, runs it sandboxed (1 CPU, 512 MB RAM, 256 pids,
no capabilities, read-only rootfs, **no internet**), and expects an HTTP
server on **port 8080** speaking the API below. `examples/reference-engine`
is a complete, passing implementation.

## Conventions

- Prices are **integer ticks** (1 tick = 0.01 quote units). No floats.
- Quantities are positive integers.
- Market orders are **IOC**: any unfilled remainder is canceled, never rests.
- `seq` is a global engine sequence number, strictly increasing per
  operation — any single client must observe strictly increasing values
  across its own serial requests.

## Endpoints

### `GET /health`
`200` Ready for traffic. Polled for up to 45s after start.

### `POST /orders`

```json
{"id": "client-uid", "side": "buy|sell", "type": "limit|market", "price": 10000, "qty": 10}
```

`price` is required for `limit`, ignored for `market`. Respond `200`:

```json
{
  "order_id": "client-uid",
  "seq": 42,
  "status": "resting|partial|filled|rejected|canceled",
  "fills": [{"price": 10000, "qty": 5, "maker_id": "other-id"}],
  "ts": 1730000000000000000
}
```

Status semantics:

| status | meaning |
|---|---|
| `resting` | no fills, limit order now on the book |
| `partial` | some fills; remainder rests (limit) or was canceled (market) |
| `filled` | fully executed |
| `canceled` | market order with zero fills (empty opposite book) |
| `rejected` | invalid: non-positive qty, limit without positive price, duplicate id, bad side/type |

`fills` lists executions **in match order**; `maker_id` is the id of the
resting order the engine is matched against. This is how price-time priority is
audited — fills must come best-price-first, and FIFO within a price level.

### `DELETE /orders/{id}`

```json
{"order_id": "id", "seq": 43, "status": "canceled", "ts": ...}
```

`status: "unknown"` if the id is not currently resting (already filled,
already canceled, or never seen). Both are `200`.

### `GET /book` *(optional, for debugging)*
Top of book: `{"bid": {"price": ..., "qty": ...}, "ask": ...}`.

## Scoring

1. **Conformance probe** (before load): a deterministic scenario checking
   time priority at equal price, price priority, partial fills, IOC market
   sweeps, cancel semantics, and rejects — diffed against the reference book.
2. **Bombardment**: up to thousands of concurrent bots ramp from 10% to 100%
   of the configured rate. Every ack is validated live (limit-price bounds,
   overfills, status consistency, seq monotonicity); violations costs score.
3. **Verdict**: `100 × (0.35·latency(p99) + 0.35·throughput(max sustained TPS
   @ <1% errors) + 0.30·correctness)`. Non-200s and timeouts count as errors.

**FUNFACT - A 2µs engine that misorders fills loses to a
200µs engine that never does.**
