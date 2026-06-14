// Package telemetry implements a tiny mergeable log-bucket latency histogram.
//
// Buckets grow geometrically (5% per step) from 1µs upward, so any reported
// quantile is within ±2.5% of the true value — plenty for ranking engines —
// while a full histogram is a few hundred sparse int64 counters. Shards ship
// deltas, never cumulative state, so the ingester can fold batches from any
// shard in any order with one addition per bucket.
package telemetry

import (
	"math"
	"sort"
	"sync"
)

const minNs = 1_000 // 1µs floor

var logGrowth = math.Log(1.05)

func bucketOf(ns int64) int {
	if ns < minNs {
		return 0
	}
	return int(math.Log(float64(ns)/float64(minNs))/logGrowth) + 1
}

// bucketValue is the geometric midpoint latency the bucket represents.
func bucketValue(b int) int64 {
	if b <= 0 {
		return minNs
	}
	return int64(float64(minNs) * math.Pow(1.05, float64(b)-0.5))
}

type Hist struct {
	mu sync.Mutex
	c  map[int]int64
}

func NewHist() *Hist { return &Hist{c: map[int]int64{}} }

func (h *Hist) Record(ns int64) {
	h.mu.Lock()
	h.c[bucketOf(ns)]++
	h.mu.Unlock()
}

// TakeDelta returns counts accumulated since the last call and resets.
func (h *Hist) TakeDelta() map[int]int64 {
	h.mu.Lock()
	d := h.c
	h.c = map[int]int64{}
	h.mu.Unlock()
	return d
}

// Merge folds src into dst.
func Merge(dst, src map[int]int64) {
	for k, v := range src {
		dst[k] += v
	}
}

// Quantile returns the latency in ns at quantile q (0 < q <= 1) of a merged
// histogram, or 0 if empty.
func Quantile(c map[int]int64, q float64) int64 {
	var total int64
	keys := make([]int, 0, len(c))
	for k, v := range c {
		keys = append(keys, k)
		total += v
	}
	if total == 0 {
		return 0
	}
	sort.Ints(keys)
	rank := int64(math.Ceil(q * float64(total)))
	var cum int64
	for _, k := range keys {
		cum += c[k]
		if cum >= rank {
			return bucketValue(k)
		}
	}
	return bucketValue(keys[len(keys)-1])
}
