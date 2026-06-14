// Package score computes the composite leaderboard score.
//
//	score = 100 * (0.35*latency + 0.35*throughput + 0.30*correctness)
//
//	latency:    log-linear on p99 ack time — 1ms or better is full marks,
//	            1s or worse is zero. Log scale because trading-system latency
//	            quality is multiplicative, not additive.
//	throughput: log on max sustained TPS (the best one-second window whose
//	            error rate stayed under 1%) — 50k TPS saturates the axis.
//	correctness: half live invariant validation by the bots (price bounds,
//	            overfills, seq monotonicity), half the deterministic
//	            conformance probe (price-time priority vs the reference book).
package score

import "math"

const (
	latFullNs = 1e6 // p99 <= 1ms  -> latency component = 1
	latZeroNs = 1e9 // p99 >= 1s   -> latency component = 0
	tpsFull   = 50_000
)

func Composite(p99Ns int64, tps, liveCorrectness, conformance float64) float64 {
	lat := clamp((math.Log10(latZeroNs) - math.Log10(math.Max(float64(p99Ns), 1))) /
		(math.Log10(latZeroNs) - math.Log10(latFullNs)))
	thr := clamp(math.Log10(math.Max(tps, 1)) / math.Log10(tpsFull))
	cor := clamp(0.5*liveCorrectness + 0.5*conformance)
	return 100 * (0.35*lat + 0.35*thr + 0.30*cor)
}

func clamp(x float64) float64 { return math.Max(0, math.Min(1, x)) }
