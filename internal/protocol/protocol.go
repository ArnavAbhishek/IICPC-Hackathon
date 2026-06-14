// Package protocol defines the wire types shared by the arena services and
// the contract every contestant engine must speak (see SPEC.md).
package protocol

type Side string
type OrdType string

const (
	Buy  Side = "buy"
	Sell Side = "sell"

	Limit  OrdType = "limit"
	Market OrdType = "market"
)

// Order is what bots POST to /orders on a contestant engine.
// Prices are integer ticks (1 tick = 0.01 quote units) to keep matching exact.
type Order struct {
	ID    string  `json:"id"`
	Side  Side    `json:"side"`
	Type  OrdType `json:"type"`
	Price int64   `json:"price,omitempty"` // required for limit orders
	Qty   int64   `json:"qty"`
}

type Fill struct {
	Price   int64  `json:"price"`
	Qty     int64  `json:"qty"`
	MakerID string `json:"maker_id"`
}

// Order lifecycle statuses an engine may return.
const (
	StatusResting  = "resting"
	StatusPartial  = "partial"
	StatusFilled   = "filled"
	StatusRejected = "rejected"
	StatusCanceled = "canceled"
	StatusUnknown  = "unknown"
)

type Ack struct {
	OrderID string `json:"order_id"`
	Seq     int64  `json:"seq"`
	Status  string `json:"status"`
	Fills   []Fill `json:"fills,omitempty"`
	TS      int64  `json:"ts"` // engine-local nanos; informational only
}

// RunJob: gateway -> orchestrator (stream runs.jobs).
type RunJob struct {
	RunID        string `json:"run_id"`
	SubmissionID string `json:"submission_id"`
	Bots         int    `json:"bots"`
	DurationS    int    `json:"duration_s"`
	MaxRPS       int    `json:"max_rps"`
}

// LoadShard: orchestrator -> botfleet (stream load.jobs). One message per shard;
// any fleet replica may claim any shard, which is what lets the fleet scale out.
type LoadShard struct {
	RunID       string `json:"run_id"`
	Target      string `json:"target"` // base URL of the sandboxed engine
	Shard       int    `json:"shard"`
	TotalShards int    `json:"total_shards"`
	Bots        int    `json:"bots"`
	DurationS   int    `json:"duration_s"`
	MaxRPS      int    `json:"max_rps"` // rate budget for this shard
	Seed        int64  `json:"seed"`
}

// TelemetryBatch: botfleet -> ingester (stream telemetry.raw).
// All fields are deltas since the previous batch, so batches merge
// commutatively regardless of shard or arrival order.
type TelemetryBatch struct {
	RunID       string          `json:"run_id"`
	Shard       int             `json:"shard"`
	TotalShards int             `json:"total_shards"`
	Final       bool            `json:"final"`
	Sent        int64           `json:"sent"`
	Acked       int64           `json:"acked"`
	Errors      int64           `json:"errors"`
	Violations  int64           `json:"violations"`
	Hist        map[int]int64   `json:"hist"`     // latency histogram bucket -> count
	SecAcks     map[int64]int64 `json:"sec_acks"` // unix second -> acks
	SecErrs     map[int64]int64 `json:"sec_errs"` // unix second -> errors
	Samples     []string        `json:"samples,omitempty"`
}
