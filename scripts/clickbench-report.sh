#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/clickbench-report.sh <hits.parquet> <store_dir> <report_dir> [runs]

Runs a reproducible ClickBench-oriented report:
  1. zig build
  2. clean and import zighouse store
  3. zighouse vs DuckDB correctness compare
  4. zighouse native full43 benchmark
  5. DuckDB direct Parquet full43 benchmark
  6. write report.md plus raw logs

Environment:
  ZIGHOUSE_DUCKDB_EXE   DuckDB CLI path, default /opt/homebrew/bin/duckdb
  ZIGHOUSE_IMPORT_TRACE Set to 1 to include import phase timings
  ZIGHOUSE_REPORT_FAST  Set to 1 to include Q24/Q29/Q40 tiny result artifacts

Notes:
  The script removes <store_dir> before import.
  <runs> repeats the import+compare+bench sequence; default 1.
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -lt 3 || $# -gt 4 ]]; then
  usage >&2
  exit 2
fi

PARQUET_PATH=$1
STORE_DIR=$2
REPORT_DIR=$3
RUNS=${4:-1}
QUERIES=${QUERIES:-assets/queries.sql}
ZIGHOUSE=${ZIGHOUSE:-zig-out/bin/zighouse}
DUCKDB_EXE=${ZIGHOUSE_DUCKDB_EXE:-/opt/homebrew/bin/duckdb}

mkdir -p "$REPORT_DIR"

run_cmd() {
  local log=$1
  shift
  /usr/bin/time -l "$@" >"$log" 2>&1
}

extract_summary() {
  local log=$1
  grep '^summary:' "$log" | tail -n 1 || true
}

extract_real() {
  local log=$1
  awk '/^[[:space:]]*[0-9.]+ real[[:space:]]/ {print $1}' "$log" | tail -n 1
}

extract_rss() {
  local log=$1
  awk '/maximum resident set size/ {print $1}' "$log" | tail -n 1
}

echo "Building zighouse..."
zig build

REPORT="$REPORT_DIR/report.md"
{
  echo "# zighouse ClickBench Report"
  echo
  echo "Generated: $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  echo
  echo "## Environment"
  echo
  echo "- Host: $(uname -a)"
  echo "- zighouse: $($ZIGHOUSE --help >/dev/null 2>&1; printf '%s' "$ZIGHOUSE")"
  echo "- DuckDB CLI: $DUCKDB_EXE"
  "$DUCKDB_EXE" --version 2>/dev/null | sed 's/^- /- DuckDB version: /' || true
  echo "- Parquet: $PARQUET_PATH"
  echo "- Queries: $QUERIES"
  echo
  echo "## Disclosure"
  echo
  echo "- Import uses DuckDB C API vectors for Parquet decoding."
  echo "- Store is a ClickBench-oriented hot-column profile, not a general-purpose full-column store."
  if [[ -n "${ZIGHOUSE_REPORT_FAST:-}" ]]; then
    echo "- Q24/Q29/Q40 use import-time tiny result artifacts recorded in the store."
  else
    echo "- Q24/Q29/Q40 fall back to the source Parquet when native hot-store artifacts are absent."
  fi
  echo "- Correctness compare treats SQL top-k queries without complete tie-breakers as tie-ambiguous."
  echo
  echo "## Results"
  echo
  echo "| Run | Import wall | Import RSS | Compare | Native wall | Native summary | Native RSS | DuckDB wall | DuckDB summary | DuckDB RSS | Store size |"
  echo "|---:|---:|---:|---|---:|---|---:|---:|---|---:|---:|"
} >"$REPORT"

run=1
while [[ $run -le $RUNS ]]; do
  echo "Run $run/$RUNS: importing..."
  rm -rf "$STORE_DIR"
  IMPORT_LOG="$REPORT_DIR/run${run}-import.log"
  COMPARE_LOG="$REPORT_DIR/run${run}-compare.log"
  NATIVE_LOG="$REPORT_DIR/run${run}-native-bench.log"
  DUCKDB_STORE="$REPORT_DIR/duckdb-store-run${run}"
  DUCKDB_LOG="$REPORT_DIR/run${run}-duckdb-bench.log"

  import_env=(env)
  if [[ -n "${ZIGHOUSE_IMPORT_TRACE:-}" ]]; then
    import_env+=(ZIGHOUSE_IMPORT_TRACE="$ZIGHOUSE_IMPORT_TRACE")
  fi
  if [[ -n "${ZIGHOUSE_REPORT_FAST:-}" ]]; then
    import_env+=(ZIGHOUSE_IMPORT_TINY_CACHES=1)
  fi
  run_cmd "$IMPORT_LOG" "${import_env[@]}" "$ZIGHOUSE" import-clickbench-parquet-duckdb-vector-hot "$PARQUET_PATH" "$STORE_DIR"

  echo "Run $run/$RUNS: comparing correctness..."
  if "$ZIGHOUSE" compare-duckdb-native "$STORE_DIR" "$QUERIES" >"$COMPARE_LOG" 2>&1; then
    compare_result="PASS"
  else
    compare_result="FAIL"
  fi

  echo "Run $run/$RUNS: native benchmark..."
  run_cmd "$NATIVE_LOG" "$ZIGHOUSE" --backend native bench "$STORE_DIR" "$QUERIES"

  echo "Run $run/$RUNS: DuckDB benchmark..."
  rm -rf "$DUCKDB_STORE"
  "$ZIGHOUSE" init "$DUCKDB_STORE" >>"$DUCKDB_LOG" 2>&1
  "$ZIGHOUSE" import "$PARQUET_PATH" "$DUCKDB_STORE" >>"$DUCKDB_LOG" 2>&1
  run_cmd "$DUCKDB_LOG.tmp" "$ZIGHOUSE" --backend duckdb bench "$DUCKDB_STORE" "$QUERIES"
  cat "$DUCKDB_LOG.tmp" >>"$DUCKDB_LOG"
  rm -f "$DUCKDB_LOG.tmp"

  import_wall=$(extract_real "$IMPORT_LOG")
  import_rss=$(extract_rss "$IMPORT_LOG")
  native_wall=$(extract_real "$NATIVE_LOG")
  native_summary=$(extract_summary "$NATIVE_LOG")
  native_rss=$(extract_rss "$NATIVE_LOG")
  duckdb_wall=$(extract_real "$DUCKDB_LOG")
  duckdb_summary=$(extract_summary "$DUCKDB_LOG")
  duckdb_rss=$(extract_rss "$DUCKDB_LOG")
  store_size=$(du -sh "$STORE_DIR" | awk '{print $1}')

  printf '| %s | %ss | %s | %s | %ss | `%s` | %s | %ss | `%s` | %s | %s |\n' \
    "$run" "$import_wall" "$import_rss" "$compare_result" "$native_wall" "$native_summary" "$native_rss" "$duckdb_wall" "$duckdb_summary" "$duckdb_rss" "$store_size" >>"$REPORT"

  run=$((run + 1))
done

{
  echo
  echo "## Raw Logs"
  echo
  ls -1 "$REPORT_DIR"/*.log | sed 's#^#- #'
} >>"$REPORT"

echo "Report written to $REPORT"
