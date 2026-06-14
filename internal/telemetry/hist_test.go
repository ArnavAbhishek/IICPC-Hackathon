package telemetry

import (
	"math"
	"testing"
)

func TestQuantileAccuracy(t *testing.T) {
	h := NewHist()
	// Uniform 1..1000 µs: p50 ≈ 500µs, p99 ≈ 990µs, ±5% bucket error budget.
	for i := 1; i <= 1000; i++ {
		h.Record(int64(i) * 1000)
	}
	c := h.TakeDelta()
	for _, tc := range []struct {
		q    float64
		want float64
	}{{0.50, 500e3}, {0.90, 900e3}, {0.99, 990e3}} {
		got := float64(Quantile(c, tc.q))
		if math.Abs(got-tc.want)/tc.want > 0.05 {
			t.Errorf("q%.2f = %.0f, want %.0f ±5%%", tc.q, got, tc.want)
		}
	}
}

func TestMergeEqualsSingle(t *testing.T) {
	a, b := NewHist(), NewHist()
	whole := NewHist()
	for i := 1; i <= 2000; i++ {
		ns := int64(i) * 37_000
		whole.Record(ns)
		if i%2 == 0 {
			a.Record(ns)
		} else {
			b.Record(ns)
		}
	}
	merged := a.TakeDelta()
	Merge(merged, b.TakeDelta())
	w := whole.TakeDelta()
	for _, q := range []float64{0.5, 0.9, 0.99} {
		if Quantile(merged, q) != Quantile(w, q) {
			t.Errorf("q%.2f: merged %d != whole %d", q, Quantile(merged, q), Quantile(w, q))
		}
	}
}

func TestTakeDeltaResets(t *testing.T) {
	h := NewHist()
	h.Record(5000)
	if n := len(h.TakeDelta()); n != 1 {
		t.Fatalf("first delta: %d buckets", n)
	}
	if n := len(h.TakeDelta()); n != 0 {
		t.Fatalf("second delta not empty: %d buckets", n)
	}
}
