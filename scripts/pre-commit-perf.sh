#!/usr/bin/env bash
set -euo pipefail

ROOT=$(git rev-parse --show-toplevel)
cd "$ROOT"

PARQUET_PATH=${ZIGHOUSE_PERF_PARQUET:-data/hits.parquet}
BASELINE=${ZIGHOUSE_PERF_BASELINE:-perf/baselines/local-10m-submit.json}
LIMIT_ROWS=${ZIGHOUSE_PERF_LIMIT_ROWS:-10000000}
TMP_ROOT=${TMPDIR:-/tmp}

if [[ ! -f "$PARQUET_PATH" ]]; then
  echo "pre-commit perf: missing $PARQUET_PATH" >&2
  exit 1
fi

if [[ ! -f "$BASELINE" ]]; then
  echo "pre-commit perf: missing $BASELINE" >&2
  exit 1
fi

WORK_DIR=$(mktemp -d "${TMP_ROOT%/}/zighouse-precommit-perf.XXXXXX")
trap 'rm -rf "$WORK_DIR"' EXIT

STORE_DIR="$WORK_DIR/store"
OUT_JSON="$WORK_DIR/perf.json"

echo "pre-commit perf: running ClickBench ${LIMIT_ROWS}-row gate"
scripts/perf-baseline.sh "$PARQUET_PATH" "$STORE_DIR" "$OUT_JSON" "$LIMIT_ROWS"
scripts/perf-compare.py --import-threshold 1000 "$BASELINE" "$OUT_JSON"
