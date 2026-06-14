"""A second contestant submission, in Python, implementing the same arena
engine contract as examples/reference-engine (see SPEC.md).

Its purpose is to prove the platform is language-agnostic: the platform only
sees a Docker build context, never the language. This engine is correct
(passes the conformance probe) but single-threaded CPython, so it lands lower
on latency/throughput than the Go reference — a clean, honest spread on the
leaderboard.

Price-time priority over integer ticks; market orders are IOC. Standard
library only, no framework, so the image stays tiny.
"""
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os
import threading


class Book:
    def __init__(self):
        self.bids = []  # list[ [price, [ [id, qty], ... ] ] ], price-desc
        self.asks = []  # price-asc
        self.by_id = {}  # id -> (side, price)
        self.seq = 0
        self.lock = threading.Lock()

    def _next(self):
        self.seq += 1
        return self.seq

    def submit(self, o):
        with self.lock:
            ack = {"order_id": o.get("id"), "seq": self._next(), "fills": []}
            oid = o.get("id")
            side = o.get("side")
            typ = o.get("type")
            price = int(o.get("price", 0) or 0)
            qty = int(o.get("qty", 0) or 0)
            if (qty <= 0 or side not in ("buy", "sell")
                    or typ not in ("limit", "market")
                    or (typ == "limit" and price <= 0)
                    or oid in self.by_id):
                ack["status"] = "rejected"
                return ack

            remaining = qty
            opp = self.asks if side == "buy" else self.bids
            while remaining > 0 and opp:
                best_price, queue = opp[0]
                if typ == "limit":
                    if side == "buy" and best_price > price:
                        break
                    if side == "sell" and best_price < price:
                        break
                while remaining > 0 and queue:
                    maker_id, maker_qty = queue[0]
                    traded = min(remaining, maker_qty)
                    ack["fills"].append(
                        {"price": best_price, "qty": traded, "maker_id": maker_id})
                    remaining -= traded
                    maker_qty -= traded
                    if maker_qty == 0:
                        queue.pop(0)
                        self.by_id.pop(maker_id, None)
                    else:
                        queue[0][1] = maker_qty
                if not queue:
                    opp.pop(0)

            if remaining == 0:
                ack["status"] = "filled"
            elif typ == "market":
                ack["status"] = "partial" if remaining < qty else "canceled"
            else:
                self._rest(oid, side, price, remaining)
                ack["status"] = "partial" if remaining < qty else "resting"
            return ack

    def _rest(self, oid, side, price, qty):
        self.by_id[oid] = (side, price)
        book = self.bids if side == "buy" else self.asks
        for level in book:
            if level[0] == price:
                level[1].append([oid, qty])
                return
        book.append([price, [[oid, qty]]])
        # keep bids price-descending, asks price-ascending
        book.sort(key=lambda lv: lv[0], reverse=(side == "buy"))

    def cancel(self, oid):
        with self.lock:
            ack = {"order_id": oid, "seq": self._next(), "fills": []}
            if oid not in self.by_id:
                ack["status"] = "unknown"
                return ack
            side, price = self.by_id.pop(oid)
            book = self.bids if side == "buy" else self.asks
            for li, level in enumerate(book):
                if level[0] != price:
                    continue
                level[1] = [e for e in level[1] if e[0] != oid]
                if not level[1]:
                    book.pop(li)
                break
            ack["status"] = "canceled"
            return ack


BOOK = Book()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass  # quiet; the platform captures container logs on failure only

    def _send(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            self._send(200, {"ok": True})
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/orders":
            self._send(404, {"error": "not found"})
            return
        n = int(self.headers.get("Content-Length", 0))
        try:
            order = json.loads(self.rfile.read(n) or b"{}")
        except json.JSONDecodeError:
            self._send(400, {"error": "bad json"})
            return
        self._send(200, BOOK.submit(order))

    def do_DELETE(self):
        if not self.path.startswith("/orders/"):
            self._send(404, {"error": "not found"})
            return
        oid = self.path[len("/orders/"):]
        self._send(200, BOOK.cancel(oid))


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    print(f"python orderbook engine on :{port}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
