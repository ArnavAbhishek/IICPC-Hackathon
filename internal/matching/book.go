// Package matching is the platform's reference limit order book: strict
// price-time priority, integer ticks, IOC semantics for market orders.
//
// The orchestrator's conformance probe replays a deterministic scenario
// through this book and diffs the contestant engine's acks against it, so
// this implementation *is* the correctness oracle for the platform.
package matching

import "arena/internal/protocol"

type resting struct {
	id    string
	side  protocol.Side
	price int64
	qty   int64
}

type level struct {
	price int64
	queue []*resting // FIFO: index 0 has time priority
}

type Book struct {
	bids []*level // sorted descending by price
	asks []*level // sorted ascending by price
	byID map[string]*resting
	seq  int64
}

func New() *Book { return &Book{byID: map[string]*resting{}} }

func (b *Book) next() int64 { b.seq++; return b.seq }

// Submit matches an incoming order against the book. Limit remainders rest;
// market remainders are canceled (IOC).
func (b *Book) Submit(o protocol.Order) protocol.Ack {
	ack := protocol.Ack{OrderID: o.ID, Seq: b.next()}
	if o.Qty <= 0 ||
		(o.Side != protocol.Buy && o.Side != protocol.Sell) ||
		(o.Type != protocol.Limit && o.Type != protocol.Market) ||
		(o.Type == protocol.Limit && o.Price <= 0) {
		ack.Status = protocol.StatusRejected
		return ack
	}
	if _, dup := b.byID[o.ID]; dup {
		ack.Status = protocol.StatusRejected
		return ack
	}

	remaining := o.Qty
	opp := &b.asks
	if o.Side == protocol.Sell {
		opp = &b.bids
	}
	for remaining > 0 && len(*opp) > 0 {
		best := (*opp)[0]
		if o.Type == protocol.Limit {
			if o.Side == protocol.Buy && best.price > o.Price {
				break
			}
			if o.Side == protocol.Sell && best.price < o.Price {
				break
			}
		}
		for remaining > 0 && len(best.queue) > 0 {
			maker := best.queue[0]
			q := min(remaining, maker.qty)
			ack.Fills = append(ack.Fills, protocol.Fill{Price: maker.price, Qty: q, MakerID: maker.id})
			remaining -= q
			maker.qty -= q
			if maker.qty == 0 {
				best.queue = best.queue[1:]
				delete(b.byID, maker.id)
			}
		}
		if len(best.queue) == 0 {
			*opp = (*opp)[1:]
		}
	}

	switch {
	case remaining == 0:
		ack.Status = protocol.StatusFilled
	case o.Type == protocol.Market:
		if remaining < o.Qty {
			ack.Status = protocol.StatusPartial
		} else {
			ack.Status = protocol.StatusCanceled
		}
	default:
		b.rest(&resting{id: o.ID, side: o.Side, price: o.Price, qty: remaining})
		if remaining < o.Qty {
			ack.Status = protocol.StatusPartial
		} else {
			ack.Status = protocol.StatusResting
		}
	}
	return ack
}

func (b *Book) rest(r *resting) {
	b.byID[r.id] = r
	side := &b.asks
	if r.side == protocol.Buy {
		side = &b.bids
	}
	i := 0
	for ; i < len(*side); i++ {
		lv := (*side)[i]
		if lv.price == r.price {
			lv.queue = append(lv.queue, r)
			return
		}
		if r.side == protocol.Buy && lv.price < r.price {
			break
		}
		if r.side == protocol.Sell && lv.price > r.price {
			break
		}
	}
	lv := &level{price: r.price, queue: []*resting{r}}
	*side = append(*side, nil)
	copy((*side)[i+1:], (*side)[i:])
	(*side)[i] = lv
}

func (b *Book) Cancel(id string) protocol.Ack {
	ack := protocol.Ack{OrderID: id, Seq: b.next()}
	r, ok := b.byID[id]
	if !ok {
		ack.Status = protocol.StatusUnknown
		return ack
	}
	delete(b.byID, id)
	side := &b.asks
	if r.side == protocol.Buy {
		side = &b.bids
	}
	for li, lv := range *side {
		if lv.price != r.price {
			continue
		}
		for qi, q := range lv.queue {
			if q.id == id {
				lv.queue = append(lv.queue[:qi], lv.queue[qi+1:]...)
				break
			}
		}
		if len(lv.queue) == 0 {
			*side = append((*side)[:li], (*side)[li+1:]...)
		}
		break
	}
	ack.Status = protocol.StatusCanceled
	return ack
}
