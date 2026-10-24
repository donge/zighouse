#!/usr/bin/env bash
set -euo pipefail

ROOT=$(git rev-parse --show-toplevel)
STORE=${1:-${ZIGHOUSE_Q20_STORE:-/tmp/zighouse-precommit-ir-store}}
ZIGHOUSE=${ZIGHOUSE:-"${ROOT}/zig-out/bin/zighouse"}
REPEATS=${ZIGHOUSE_Q20_REPEATS:-100}
TARGET=${ZIGHOUSE_Q20_TARGET:-435090932899640449}
SQL="SELECT UserID FROM hits WHERE UserID = ${TARGET}"

if [[ ! -x "$ZIGHOUSE" ]]; then
  echo "q20 regression: missing executable $ZIGHOUSE" >&2
  exit 1
fi
if [[ ! -d "$STORE" ]]; then
  echo "q20 regression: missing store $STORE" >&2
  exit 1
fi

for threads in 1 8 16; do
  echo "q20 regression: threads=${threads} repeats=${REPEATS}"
  for _ in $(seq 1 "$REPEATS"); do
    output=$(env ZIGHOUSE_THREADS="$threads" \
      "$ZIGHOUSE" query "$STORE" hits "$SQL")
    if ! grep -q "$TARGET" <<<"$output"; then
      echo "q20 regression: missing target with threads=${threads}" >&2
      exit 1
    fi
  done
done

echo "q20 regression: PASS"
