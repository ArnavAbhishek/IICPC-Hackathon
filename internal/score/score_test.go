package score

import "testing"

func TestCompositeBounds(t *testing.T) {
	if s := Composite(500_000, 60_000, 1, 1); s < 99.9 || s > 100 {
		t.Errorf("perfect engine: %.2f", s)
	}
	if s := Composite(5e9, 0, 0, 0); s != 0 {
		t.Errorf("dead engine: %.2f", s)
	}
}

func TestMonotonicity(t *testing.T) {
	base := Composite(2_000_000, 5_000, 1, 1)
	if Composite(20_000_000, 5_000, 1, 1) >= base {
		t.Error("higher p99 must not score higher")
	}
	if Composite(2_000_000, 500, 1, 1) >= base {
		t.Error("lower tps must not score higher")
	}
	if Composite(2_000_000, 5_000, 0.5, 1) >= base {
		t.Error("worse correctness must not score higher")
	}
	if Composite(2_000_000, 5_000, 1, 0.5) >= base {
		t.Error("worse conformance must not score higher")
	}
}
