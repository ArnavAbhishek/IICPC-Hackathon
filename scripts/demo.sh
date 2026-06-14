#!/usr/bin/env bash
# End-to-end demo: package the reference engine (and a degraded "chaos"
# variant), submit both, fire benchmark runs, and tail the leaderboard.
set -euo pipefail
cd "$(dirname "$0")/.."
API=${API:-http://localhost:8090}

jqr() { python3 -c "import sys,json;print(json.load(sys.stdin)$1)"; }

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

echo "==> packaging engines (Go reference, Python, and a slow+buggy chaos build)"
tar czf "$TMP/ref.tar.gz" -C examples/reference-engine .
tar czf "$TMP/py.tar.gz"  -C examples/python-orderbook .
cp -r examples/reference-engine "$TMP/chaos"
printf '\nENV DELAY_MS=4\nENV CHAOS=1\n' >> "$TMP/chaos/Dockerfile"
tar czf "$TMP/chaos.tar.gz" -C "$TMP/chaos" .

echo "==> uploading submissions"
REF=$(curl -sf -F team="HFT Cavemen" -F name="go-reference" \
  -F bundle=@"$TMP/ref.tar.gz" "$API/api/submissions" | jqr "['id']")
PY=$(curl -sf -F team="Team Serpent" -F name="python-orderbook" \
  -F bundle=@"$TMP/py.tar.gz" "$API/api/submissions" | jqr "['id']")
CHA=$(curl -sf -F team="Chaos Capital" -F name="laggy-engine" \
  -F bundle=@"$TMP/chaos.tar.gz" "$API/api/submissions" | jqr "['id']")
echo "    go:$REF  python:$PY  chaos:$CHA"

echo "==> starting benchmark runs (150 bots, 40s, 3000 rps each)"
run() { curl -sf -X POST "$API/api/runs" -H 'Content-Type: application/json' \
  -d "{\"submission_id\":\"$1\",\"bots\":150,\"duration_s\":40,\"max_rps\":3000}" | jqr "['id']"; }
R1=$(run "$REF"); R2=$(run "$PY"); R3=$(run "$CHA")
echo "    runs: $R1 $R2 $R3"
echo
echo "==> watch it live:  $API"
echo "==> tailing run states (ctrl-c anytime)"
while true; do
  sleep 5
  S1=$(curl -sf "$API/api/runs/$R1" | jqr ".get('status','?')")
  S2=$(curl -sf "$API/api/runs/$R2" | jqr ".get('status','?')")
  S3=$(curl -sf "$API/api/runs/$R3" | jqr ".get('status','?')")
  echo "    $(date +%T)  go=$S1  python=$S2  chaos=$S3"
  done_p() { [[ "$1" == done || "$1" == failed ]]; }
  done_p "$S1" && done_p "$S2" && done_p "$S3" && break
done
echo
echo "==> final leaderboard (expect Go > Python > chaos)"
curl -sf "$API/api/leaderboard" | python3 -m json.tool
