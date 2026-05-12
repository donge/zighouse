#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/perf-baseline.sh <hits.parquet> <store_dir> <out.json> [limit_rows]

Builds the no-DuckDB binary, imports a ClickBench Parquet sample, runs the
native 43-query benchmark, and writes a machine-readable JSON result.

Default limit_rows is 10000000. Set limit_rows to 0 to import the full file.

Environment:
  PERF_REPEATS  Number of import+query measurements to run, default 3.
  ZIGHOUSE_PERF_QUERY_PATH  Query path for the benchmark, default compare.
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
OUT_JSON=$3
LIMIT_ROWS=${4:-10000000}
QUERIES=${QUERIES:-assets/queries.sql}
ZIGHOUSE=${ZIGHOUSE:-zig-out/bin/zighouse}
BUILD_ARGS=(-Dduckdb=false)
PERF_REPEATS=${PERF_REPEATS:-3}
ZIGHOUSE_PERF_QUERY_PATH=${ZIGHOUSE_PERF_QUERY_PATH:-compare}

if [[ "$(uname -s)" == "Darwin" ]]; then
  TIME_CMD=(/usr/bin/time -l)
else
  TIME_CMD=(/usr/bin/time -v)
fi

tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT

mkdir -p "$(dirname "$OUT_JSON")"

zig build "${BUILD_ARGS[@]}"

run=1
while [[ $run -le $PERF_REPEATS ]]; do
  import_log="$tmp_dir/import-${run}.log"
  bench_log="$tmp_dir/bench-${run}.log"
  rm -rf "$STORE_DIR"

  import_args=("$ZIGHOUSE" import-clickbench-parquet-hot "$PARQUET_PATH" "$STORE_DIR")
  if [[ "$LIMIT_ROWS" != "0" ]]; then
    import_args+=("$LIMIT_ROWS")
  fi

  "${TIME_CMD[@]}" env ZIGHOUSE_CLICKBENCH_SUBMIT=1 ZIGHOUSE_IMPORT_TRACE=1 "${import_args[@]}" >"$import_log" 2>&1
  "${TIME_CMD[@]}" env ZIGHOUSE_CLICKBENCH_SUBMIT=1 ZIGHOUSE_QUERY_PATH="$ZIGHOUSE_PERF_QUERY_PATH" "$ZIGHOUSE" --backend native bench "$STORE_DIR" "$QUERIES" >"$bench_log" 2>&1
  run=$((run + 1))
done

python3 - "$OUT_JSON" "$PARQUET_PATH" "$STORE_DIR" "$LIMIT_ROWS" "$PERF_REPEATS" "$tmp_dir" "$ZIGHOUSE_PERF_QUERY_PATH" <<'PY'
import ast
import json
import platform
import re
import subprocess
import sys
from pathlib import Path

out_path = Path(sys.argv[1])
parquet_path = sys.argv[2]
store_dir = Path(sys.argv[3])
limit_rows = int(sys.argv[4])
repeats = int(sys.argv[5])
tmp_dir = Path(sys.argv[6])
query_path = sys.argv[7]


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
    text = re.sub(r"query_path_compare[^\n]*\n", "", text)
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


runs = []
for i in range(1, repeats + 1):
    import_text = read(tmp_dir / f"import-{i}.log")
    bench_text = read(tmp_dir / f"bench-{i}.log")
    summary = extract_summary(bench_text)
    rows = extract_rows(bench_text)
    runs.append({
        "run": i,
        "import": {
            "wall_seconds": extract_time(import_text),
            "total_seconds": extract_import_total(import_text),
            "rss_bytes": extract_rss(import_text),
        },
        "query": {
            **summary,
            "timings": rows,
            "warm_best_from_rows": sum(min(x for x in row if x is not None) for row in rows),
        },
    })

representative = min(runs, key=lambda r: abs(r["query"]["warm_best_sum"] - median([x["query"]["warm_best_sum"] for x in runs])))
import_total_values = [r["import"]["total_seconds"] for r in runs if r["import"]["total_seconds"] is not None]
import_wall_values = [r["import"]["wall_seconds"] for r in runs if r["import"]["wall_seconds"] is not None]
query_values = [r["query"]["warm_best_sum"] for r in runs]

data = {
    "schema_version": 1,
    "benchmark": "clickbench-submit-10m" if limit_rows else "clickbench-submit-full",
    "parquet": parquet_path,
    "limit_rows": limit_rows or None,
    "queries": "assets/queries.sql",
    "query_path": query_path,
    "build": {
        "args": ["-Dduckdb=false"],
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
        "wall_seconds": median(import_wall_values),
        "total_seconds": median(import_total_values),
        "rss_bytes": representative["import"]["rss_bytes"],
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
