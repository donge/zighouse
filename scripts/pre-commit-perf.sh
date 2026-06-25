#!/usr/bin/env bash
set -euo pipefail

ROOT=$(git rev-parse --show-toplevel)
cd "$ROOT"

PARQUET_PATH=${ZIGHOUSE_PERF_PARQUET:-data/hits_10m.parquet}
BASELINE=${ZIGHOUSE_PERF_BASELINE:-perf/baselines/local-10m-ir.json}
LIMIT_ROWS=${ZIGHOUSE_PERF_LIMIT_ROWS:-10000000}
TMP_ROOT=${TMPDIR:-/tmp}
# Persistent store: only re-import when parquet or limit changes.
STORE_DIR="${ZIGHOUSE_PERF_STORE:-${TMP_ROOT%/}/zighouse-precommit-ir-store}"
BENCH_REPEATS=${ZIGHOUSE_PERF_BENCH_REPEATS:-2}
ZIGHOUSE=${ZIGHOUSE:-zig-out/bin/zighouse}

if [[ ! -f "$BASELINE" ]]; then
  echo "pre-commit perf: missing $BASELINE" >&2
  exit 1
fi

# Fingerprint: sha256 of parquet path + limit rows to detect stale store.
FINGERPRINT_FILE="${STORE_DIR}/.import_fingerprint"
FINGERPRINT="${PARQUET_PATH}:${LIMIT_ROWS}"

needs_import=false
if [[ ! -d "$STORE_DIR" ]]; then
  needs_import=true
elif [[ ! -f "$FINGERPRINT_FILE" ]]; then
  needs_import=true
elif [[ "$(cat "$FINGERPRINT_FILE")" != "$FINGERPRINT" ]]; then
  needs_import=true
fi

# Only check for parquet file if we actually need to import.
if $needs_import && [[ ! -f "$PARQUET_PATH" ]]; then
  echo "pre-commit perf: missing $PARQUET_PATH (needed for import)" >&2
  exit 1
fi

if $needs_import; then
  MIN_FREE_KB=${ZIGHOUSE_PERF_MIN_FREE_KB:-12582912} # 12 GiB
  FREE_KB=$(df -Pk "$TMP_ROOT" | awk 'NR==2 {print $4}')
  if [[ -n "$FREE_KB" && "$FREE_KB" -lt "$MIN_FREE_KB" ]]; then
    echo "pre-commit perf: insufficient free space under $TMP_ROOT (${FREE_KB}KB < ${MIN_FREE_KB}KB)" >&2
    exit 1
  fi
fi

zig build -Doptimize=ReleaseFast

if $needs_import; then
  echo "pre-commit perf: importing ClickBench ${LIMIT_ROWS} rows -> ${STORE_DIR}"
  rm -rf "$STORE_DIR"
  mkdir -p "$STORE_DIR"
  env ZIGHOUSE_IMPORT_TRACE=1 \
    "$ZIGHOUSE" import-parquet --format=generic "$PARQUET_PATH" "$STORE_DIR" hits
  echo "$FINGERPRINT" > "$FINGERPRINT_FILE"
else
  echo "pre-commit perf: reusing cached store ${STORE_DIR}"
fi

WORK_DIR=$(mktemp -d "${TMP_ROOT%/}/zighouse-precommit-perf.XXXXXX")
trap 'rm -rf "$WORK_DIR"' EXIT
OUT_JSON="$WORK_DIR/perf.json"

echo "pre-commit perf: running ClickBench bench x${BENCH_REPEATS}"

# Run bench BENCH_REPEATS times and collect timing rows.
TMP_BENCH="$WORK_DIR/bench_runs"
mkdir -p "$TMP_BENCH"
if [[ "$(uname -s)" == "Darwin" ]]; then
  TIME_CMD=(/usr/bin/time -l)
else
  TIME_CMD=(/usr/bin/time -v)
fi

for i in $(seq 1 "$BENCH_REPEATS"); do
  "${TIME_CMD[@]}" sh -c "env ZIGHOUSE_CLICKBENCH_SUBMIT=1 \
    '$ZIGHOUSE' bench '--store=$STORE_DIR' hits clickbench-submit/zighouse/queries.sql 2>/dev/null" \
    > "$TMP_BENCH/bench-${i}.log" 2>&1
done

# Import measurement: record wall time for a small sample import.
import_log="$WORK_DIR/import.log"
import_store="$WORK_DIR/import_store"
IMPORT_SAMPLE_PARQUET="${ZIGHOUSE_PERF_IMPORT_SAMPLE:-data/hits_1m_snappy.parquet}"
if [[ -f "$IMPORT_SAMPLE_PARQUET" ]]; then
  import_start=$(python3 -c "import time; print(time.time())")
  env ZIGHOUSE_IMPORT_TRACE=1 ZIGHOUSE_CLICKBENCH_SUBMIT=1 \
    "$ZIGHOUSE" import-parquet --format=generic "$IMPORT_SAMPLE_PARQUET" "$import_store" hits \
    > "$import_log" 2>&1
  import_end=$(python3 -c "import time; print(time.time())")
  python3 -c "print(f'        {float($import_end - $import_start):.2f} real')" >> "$import_log"
else
  # No sample parquet available; skip import measurement.
  touch "$import_log"
fi

python3 - "$OUT_JSON" "$PARQUET_PATH" "$STORE_DIR" "$LIMIT_ROWS" "$BENCH_REPEATS" \
          "$TMP_BENCH" "$import_log" <<'PY'
import ast
import json
import platform
import re
import subprocess
import sys
from pathlib import Path

out_path     = Path(sys.argv[1])
parquet_path = sys.argv[2]
store_dir    = Path(sys.argv[3])
limit_rows   = int(sys.argv[4])
repeats      = int(sys.argv[5])
bench_dir    = Path(sys.argv[6])
import_log   = Path(sys.argv[7])


def read(path: Path) -> str:
    return path.read_text(errors="replace")


def extract_time(text: str):
    m = re.findall(r"^\s*([0-9.]+)\s+real\b", text, re.M)
    if m:
        return float(m[-1])
    m = re.findall(r"Elapsed \(wall clock\) time .*: (?:(\d+):)?(\d+):(\d+(?:\.\d+)?)", text)
    if m:
        h, mm, ss = m[-1]
        return int(h or 0) * 3600 + int(mm) * 60 + float(ss)
    return None


def extract_rss(text: str):
    m = re.findall(r"maximum resident set size\s+([0-9]+)", text, re.I)
    if m:
        return int(m[-1])
    m = re.findall(r"Maximum resident set size \(kbytes\):\s+([0-9]+)", text)
    if m:
        return int(m[-1]) * 1024
    return None


def extract_import_total(text: str):
    m = re.findall(r"import_phase total seconds=([0-9.]+)", text)
    return float(m[-1]) if m else None


def extract_summary(text: str) -> dict:
    m = re.search(
        r"summary: queries=(\d+) nulls=(\d+) first_sum=([0-9.]+) warm_best_sum=([0-9.]+) all_runs_sum=([0-9.]+)",
        text,
    )
    if not m:
        raise SystemExit("missing benchmark summary")
    return {
        "queries": int(m.group(1)),
        "nulls": int(m.group(2)),
        "first_sum": float(m.group(3)),
        "warm_best_sum": float(m.group(4)),
        "all_runs_sum": float(m.group(5)),
    }


def extract_rows(text: str):
    rows = []
    for line in text.splitlines():
        s = line.strip()
        if not s.startswith("["):
            continue
        rows.append(ast.literal_eval(s.rstrip(",").replace("null", "None")))
    if len(rows) != 43:
        raise SystemExit(f"expected 43 timing rows, got {len(rows)}")
    return rows


def store_size(path: Path) -> int:
    total = 0
    for child in path.rglob("*"):
        if child.is_file():
            total += child.stat().st_size
    return total


def git_value(args):
    try:
        return subprocess.check_output(["git", *args], text=True).strip()
    except Exception:
        return None


def median(values):
    xs = sorted(values)
    n = len(xs)
    if n == 0:
        return None
    mid = n // 2
    if n % 2:
        return xs[mid]
    return (xs[mid - 1] + xs[mid]) / 2


# Read import measurement.
import_text  = read(import_log)
import_wall  = extract_time(import_text)
import_total = extract_import_total(import_text)
import_rss   = extract_rss(import_text)

# Read bench runs.
runs = []
for i in range(1, repeats + 1):
    bench_text = read(bench_dir / f"bench-{i}.log")
    summary = extract_summary(bench_text)
    rows = extract_rows(bench_text)
    query = {
        **summary,
        "timings": rows,
        "warm_best_from_rows": sum(min((x for x in row if x is not None), default=0.0) for row in rows),
    }
    runs.append({
        "run": i,
        "import": {"wall_seconds": import_wall, "total_seconds": import_total, "rss_bytes": import_rss},
        "query": query,
    })

# Drop first run (cold-cache warm-up) if we have >= 3 runs.
bench_runs = runs[1:] if len(runs) >= 3 else runs
query_values = [r["query"]["warm_best_sum"] for r in bench_runs]
representative = min(bench_runs, key=lambda r: abs(r["query"]["warm_best_sum"] - median(query_values)))

data = {
    "schema_version": 1,
    "benchmark": "clickbench-submit-10m" if limit_rows else "clickbench-submit-full",
    "parquet": parquet_path,
    "limit_rows": limit_rows or None,
    "queries": "clickbench-submit/zighouse/queries.sql",
    "build": {
        "git_commit": git_value(["rev-parse", "HEAD"]),
        "git_dirty": bool(git_value(["status", "--short"])),
    },
    "host": {
        "system": platform.system(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "platform": platform.platform(),
    },
    "import": {
        "wall_seconds": import_wall,
        "total_seconds": import_total,
        "rss_bytes": import_rss,
    },
    "query": {
        **representative["query"],
        "warm_best_sum": median(query_values),
    },
    "runs": runs,
    "store_size_bytes": store_size(store_dir),
}

out_path.write_text(json.dumps(data, indent=2) + "\n")
print(json.dumps({
    "out": str(out_path),
    "import_wall_seconds": data["import"]["wall_seconds"],
    "warm_best_sum": data["query"]["warm_best_sum"],
    "store_size_bytes": data["store_size_bytes"],
}, indent=2))
PY

scripts/perf-compare.py \
  --query-threshold 15 \
  --import-threshold 20 \
  --per-query-threshold 35 \
  --duckdb-ref "${ROOT}/perf/baselines/duckdb-10m.json" \
  --duckdb-sum-ratio 1.0 \
  --duckdb-query-ratio 2.0 \
  "$BASELINE" "$OUT_JSON"
