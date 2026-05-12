#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/generic-regression.sh <store_dir> [queries.sql] [expected.txt]

Runs generic SQL regression queries against an existing store and compares the
combined output with a golden file. The default expected file is for the 10M
ClickBench store used by local perf gates.
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -lt 1 || $# -gt 3 ]]; then
  usage >&2
  exit 2
fi

ROOT=$(git rev-parse --show-toplevel)
cd "$ROOT"

STORE_DIR=$1
QUERIES=${2:-assets/generic_regression.sql}
EXPECTED=${3:-assets/generic_regression_10m.expected}
ZIGHOUSE=${ZIGHOUSE:-zig-out/bin/zighouse}

zig build -Dduckdb=false >/dev/null

tmp_out=$(mktemp)
trap 'rm -f "$tmp_out"' EXIT

while IFS= read -r query; do
  [[ -z "${query//[[:space:]]/}" ]] && continue
  env ZIGHOUSE_QUERY_PATH=generic "$ZIGHOUSE" --backend native query "$STORE_DIR" "$query" >>"$tmp_out"
done <"$QUERIES"

diff -u "$EXPECTED" "$tmp_out"
echo "generic regression: PASS"
