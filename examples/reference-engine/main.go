// refengine: a self-contained example contestant submission implementing the
// arena engine contract (SPEC.md): a price-time priority limit order book
// over REST, integer ticks, IOC market orders.
//
// Knobs for demos (set via ENV in the Dockerfile):
//
//	DELAY_MS  artificial per-request latency, to demote it on the board
//	CHAOS     "1" injects rare wrong-price fills and seq regressions, to
//	          light up the correctness checks
package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type side string
type ordType string

const (
	buy  side = "buy"
	sell side = "sell"

	limit  ordType = "limit"
	market ordType = "market"
)

type order struct {
	ID    string  `json:"id"`
	Side  side    `json:"side"`
	Type  ordType `json:"type"`
	Price int64   `json:"price,omitempty"`
	Qty   int64   `json:"qty"`
}

type fill struct {
	Price   int64  `json:"price"`
	Qty     int64  `json:"qty"`
	MakerID string `json:"maker_id"`
}

type ack struct {
	OrderID string `json:"order_id"`
	Seq     int64  `json:"seq"`
	Status  string `json:"status"`
	Fills   []fill `json:"fills,omitempty"`
	TS      int64  `json:"ts"`
}

type resting struct {
	id    string
	side  side
	price int64
	qty   int64
}

type level struct {
	price int64
	queue []*resting
}

type book struct {
	mu   sync.Mutex
	bids []*level // desc
	asks []*level // asc
	byID map[string]*resting
	seq  int64
}

func newBook() *book { return &book{byID: map[string]*resting{}} }

func (b *book) submit(o order) ack {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.seq++
	a := ack{OrderID: o.ID, Seq: b.seq, TS: time.Now().UnixNano()}
	if o.Qty <= 0 ||
		(o.Side != buy && o.Side != sell) ||
		(o.Type != limit && o.Type != market) ||
		(o.Type == limit && o.Price <= 0) {
		a.Status = "rejected"
		return a
	}
	if _, dup := b.byID[o.ID]; dup {
		a.Status = "rejected"
		return a
	}

	remaining := o.Qty
	opp := &b.asks
	if o.Side == sell {
		opp = &b.bids
	}
	for remaining > 0 && len(*opp) > 0 {
		best := (*opp)[0]
		if o.Type == limit {
			if o.Side == buy && best.price > o.Price {
				break
			}
			if o.Side == sell && best.price < o.Price {
				break
			}
		}
		for remaining > 0 && len(best.queue) > 0 {
			maker := best.queue[0]
			q := min(remaining, maker.qty)
			a.Fills = append(a.Fills, fill{Price: maker.price, Qty: q, MakerID: maker.id})
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
		a.Status = "filled"
	case o.Type == market: // IOC: remainder never rests
		if remaining < o.Qty {
			a.Status = "partial"
		} else {
			a.Status = "canceled"
		}
	default:
		r := &resting{id: o.ID, side: o.Side, price: o.Price, qty: remaining}
		b.rest(r)
		if remaining < o.Qty {
			a.Status = "partial"
		} else {
			a.Status = "resting"
		}
	}
	return a
}

func (b *book) rest(r *resting) {
	b.byID[r.id] = r
	sd := &b.asks
	if r.side == buy {
		sd = &b.bids
	}
	i := 0
	for ; i < len(*sd); i++ {
		lv := (*sd)[i]
		if lv.price == r.price {
			lv.queue = append(lv.queue, r)
			return
		}
		if r.side == buy && lv.price < r.price {
			break
		}
		if r.side == sell && lv.price > r.price {
			break
		}
	}
	lv := &level{price: r.price, queue: []*resting{r}}
	*sd = append(*sd, nil)
	copy((*sd)[i+1:], (*sd)[i:])
	(*sd)[i] = lv
}

func (b *book) cancel(id string) ack {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.seq++
	a := ack{OrderID: id, Seq: b.seq, TS: time.Now().UnixNano()}
	r, ok := b.byID[id]
	if !ok {
		a.Status = "unknown"
		return a
	}
	delete(b.byID, id)
	sd := &b.asks
	if r.side == buy {
		sd = &b.bids
	}
	for li, lv := range *sd {
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
			*sd = append((*sd)[:li], (*sd)[li+1:]...)
		}
		break
	}
	a.Status = "canceled"
	return a
}

func (b *book) top() map[string]any {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := map[string]any{"bid": nil, "ask": nil}
	if len(b.bids) > 0 {
		var q int64
		for _, r := range b.bids[0].queue {
			q += r.qty
		}
		out["bid"] = map[string]int64{"price": b.bids[0].price, "qty": q}
	}
	if len(b.asks) > 0 {
		var q int64
		for _, r := range b.asks[0].queue {
			q += r.qty
		}
		out["ask"] = map[string]int64{"price": b.asks[0].price, "qty": q}
	}
	return out
}

func main() {
	bk := newBook()
	delayMs, _ := strconv.Atoi(os.Getenv("DELAY_MS"))
	chaos := os.Getenv("CHAOS") == "1"
	var chaosMu sync.Mutex
	chaosRng := rand.New(rand.NewSource(42))

	mangle := func(a *ack) {
		if !chaos {
			return
		}
		chaosMu.Lock()
		defer chaosMu.Unlock()
		if len(a.Fills) > 0 && chaosRng.Float64() < 0.05 {
			a.Fills[0].Price += 3 // violates the taker's limit bound sometimes
		}
		if chaosRng.Float64() < 0.02 {
			a.Seq -= 5 // seq regression
		}
	}

	reply := func(w http.ResponseWriter, a ack) {
		if delayMs > 0 {
			time.Sleep(time.Duration(delayMs) * time.Millisecond)
		}
		mangle(&a)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(a)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("POST /orders", func(w http.ResponseWriter, r *http.Request) {
		var o order
		if err := json.NewDecoder(r.Body).Decode(&o); err != nil {
			http.Error(w, `{"error":"bad json"}`, 400)
			return
		}
		reply(w, bk.submit(o))
	})
	mux.HandleFunc("DELETE /orders/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimSpace(r.PathValue("id"))
		reply(w, bk.cancel(id))
	})
	mux.HandleFunc("GET /book", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(bk.top())
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("refengine on :%s (delay=%dms chaos=%v)", port, delayMs, chaos)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
