package matching

import (
	"testing"

	"arena/internal/protocol"
)

func lim(id string, s protocol.Side, price, qty int64) protocol.Order {
	return protocol.Order{ID: id, Side: s, Type: protocol.Limit, Price: price, Qty: qty}
}

func mkt(id string, s protocol.Side, qty int64) protocol.Order {
	return protocol.Order{ID: id, Side: s, Type: protocol.Market, Qty: qty}
}

func TestPriceTimePriority(t *testing.T) {
	b := New()
	if got := b.Submit(lim("A", protocol.Buy, 10000, 10)).Status; got != protocol.StatusResting {
		t.Fatalf("A: %s", got)
	}
	if got := b.Submit(lim("B", protocol.Buy, 10000, 5)).Status; got != protocol.StatusResting {
		t.Fatalf("B: %s", got)
	}
	// Crossing sell must consume A fully before touching B (time priority).
	ack := b.Submit(lim("C", protocol.Sell, 9900, 12))
	if ack.Status != protocol.StatusFilled {
		t.Fatalf("C status: %s", ack.Status)
	}
	want := []protocol.Fill{{Price: 10000, Qty: 10, MakerID: "A"}, {Price: 10000, Qty: 2, MakerID: "B"}}
	if len(ack.Fills) != 2 || ack.Fills[0] != want[0] || ack.Fills[1] != want[1] {
		t.Fatalf("C fills: %+v", ack.Fills)
	}
}

func TestBetterPriceWins(t *testing.T) {
	b := New()
	b.Submit(lim("low", protocol.Buy, 9900, 5))
	b.Submit(lim("high", protocol.Buy, 10000, 5))
	ack := b.Submit(mkt("m", protocol.Sell, 7))
	if len(ack.Fills) != 2 || ack.Fills[0].MakerID != "high" || ack.Fills[1].MakerID != "low" {
		t.Fatalf("price priority violated: %+v", ack.Fills)
	}
	if ack.Fills[0].Price != 10000 || ack.Fills[1].Price != 9900 {
		t.Fatalf("fill prices: %+v", ack.Fills)
	}
}

func TestMarketIOC(t *testing.T) {
	b := New()
	b.Submit(lim("A", protocol.Sell, 10100, 4))
	ack := b.Submit(mkt("m", protocol.Buy, 10))
	if ack.Status != protocol.StatusPartial {
		t.Fatalf("partial market: %s", ack.Status)
	}
	// Remainder must NOT rest: a new sell should not match anything.
	ack2 := b.Submit(lim("S", protocol.Sell, 1, 5))
	if len(ack2.Fills) != 0 || ack2.Status != protocol.StatusResting {
		t.Fatalf("market remainder rested: %+v", ack2)
	}
	// Empty-book market order is canceled outright.
	b2 := New()
	if got := b2.Submit(mkt("m2", protocol.Buy, 5)).Status; got != protocol.StatusCanceled {
		t.Fatalf("empty-book market: %s", got)
	}
}

func TestCancel(t *testing.T) {
	b := New()
	b.Submit(lim("A", protocol.Sell, 10100, 10))
	if got := b.Cancel("A").Status; got != protocol.StatusCanceled {
		t.Fatalf("cancel: %s", got)
	}
	if got := b.Cancel("A").Status; got != protocol.StatusUnknown {
		t.Fatalf("double cancel: %s", got)
	}
	// Canceled order must not match.
	ack := b.Submit(mkt("m", protocol.Buy, 5))
	if len(ack.Fills) != 0 {
		t.Fatalf("matched canceled order: %+v", ack.Fills)
	}
}

func TestRejections(t *testing.T) {
	b := New()
	if got := b.Submit(lim("z", protocol.Sell, 0, 5)).Status; got != protocol.StatusRejected {
		t.Fatalf("zero price: %s", got)
	}
	if got := b.Submit(lim("q", protocol.Buy, 100, 0)).Status; got != protocol.StatusRejected {
		t.Fatalf("zero qty: %s", got)
	}
	b.Submit(lim("dup", protocol.Buy, 100, 1))
	if got := b.Submit(lim("dup", protocol.Buy, 100, 1)).Status; got != protocol.StatusRejected {
		t.Fatalf("duplicate id: %s", got)
	}
}

func TestSeqMonotonic(t *testing.T) {
	b := New()
	var last int64
	for i := 0; i < 100; i++ {
		ack := b.Submit(lim(string(rune('a'+i%26))+string(rune('0'+i/26)), protocol.Buy, 100, 1))
		if ack.Seq <= last {
			t.Fatalf("seq %d after %d", ack.Seq, last)
		}
		last = ack.Seq
	}
}
