#!/usr/bin/env bash
# scripts/generic-regression.sh
# Runs generic SQL regression queries against an existing CH-format data dir
# via zighouse serve + HTTP, and compares combined output with a golden file.
#
# Usage: scripts/generic-regression.sh <data_dir> [queries.sql] [expected.txt]
#
# The default expected file covers the 10M ClickBench store.
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/generic-regression.sh <data_dir> [queries.sql] [expected.txt]

Runs generic SQL regression queries against an existing store via zighouse serve
and compares the combined output with a golden file.
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then usage; exit 0; fi
if [[ $# -lt 1 || $# -gt 3 ]]; then usage >&2; exit 2; fi

ROOT=$(git rev-parse --show-toplevel)
cd "$ROOT"

DATA_DIR=$1
QUERIES=${2:-assets/generic_regression.sql}
EXPECTED=${3:-assets/generic_regression_1m.expected}
ZIGHOUSE=${ZIGHOUSE:-zig-out/bin/zighouse}
PORT=${ZIGHOUSE_REGRESSION_PORT:-19970}
HTTP_PORT=$((PORT + 1))
CURL="curl -s --noproxy localhost"
SERVER_PID=""

cleanup() {
  [[ -n "$SERVER_PID" ]] && kill "$SERVER_PID" 2>/dev/null || true
}
trap cleanup EXIT

# Build if binary is missing or outdated.
if [[ ! -f "$ZIGHOUSE" ]]; then
  echo "Building zighouse..."
  zig build -Dduckdb=false -Dstatic-libs=false >/dev/null
fi

# Start server.
"$ZIGHOUSE" serve "--data-dir=$DATA_DIR" "--port=$PORT" >/tmp/zh-regression-server.log 2>&1 &
SERVER_PID=$!

# Wait for HTTP port to be ready.
for i in $(seq 1 40); do
  if $CURL "http://localhost:${HTTP_PORT}/?query=SELECT+1" >/dev/null 2>&1; then break; fi
  sleep 0.25
  if [[ $i -eq 40 ]]; then
    echo "ERROR: server did not start on port $HTTP_PORT"
    cat /tmp/zh-regression-server.log
    exit 1
  fi
done

tmp_out=$(mktemp)
trap 'rm -f "$tmp_out"; cleanup' EXIT

# Run queries via HTTP (TabSeparated output, no header).
while IFS= read -r query; do
  [[ -z "${query//[[:space:]]/}" ]] && continue
  encoded=$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))" "$query")
  result=$($CURL "http://localhost:${HTTP_PORT}/?query=${encoded}&default_format=TabSeparated")
  printf '%s\n' "$result" >>"$tmp_out"
  echo "" >>"$tmp_out"
done <"$QUERIES"

diff -u "$EXPECTED" "$tmp_out"
echo "generic regression: PASS"
