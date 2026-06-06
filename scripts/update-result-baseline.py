#!/usr/bin/env python3
"""
Capture per-query result checksums for all 43 ClickBench queries and write
them to perf/baselines/local-10m-ir-results.json.

Run this intentionally after fixing a correctness bug or after any change
that legitimately alters query output.  The pre-commit script compares live
results against the stored checksums so silent regressions are caught early.

Usage:
    python3 scripts/update-result-baseline.py [options]

Options:
    --store DIR       Generic IR store directory
                      (default: /tmp/zighouse-precommit-ir-store)
    --queries FILE    SQL file with 43 semicolon-separated queries
                      (default: clickbench-submit/zighouse/queries.sql)
    --zighouse PATH   Path to zighouse binary (default: ./zig-out/bin/zighouse)
    --out FILE        Output JSON file
                      (default: perf/baselines/local-10m-ir-results.json)
    --query N         Only update checksum for query N (1-based); useful for
                      targeted fixes without re-running all 43 queries.
    --no-build        Skip 'zig build' step.
    --verbose         Print first few result rows for each query.
"""

import argparse
import csv
import hashlib
import io
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import List, Optional, Tuple


# ---------------------------------------------------------------------------
# Helpers shared with compare-results-duckdb.py
# ---------------------------------------------------------------------------

def load_queries(path: str) -> List[str]:
    with open(path) as f:
        raw = f.read()
    return [q.strip() for q in raw.split(";") if q.strip()]


def run_zighouse_query(zighouse: str, store: str, sql: str) -> Optional[bytes]:
    cmd = [zighouse, "ir-query", store, "hits", sql]
    try:
        result = subprocess.run(cmd, capture_output=True, timeout=120)
        if result.returncode != 0:
            return None
        return result.stdout
    except Exception as e:
        print(f"  ZigHouse error: {e}", file=sys.stderr)
        return None


def parse_csv(raw: bytes) -> Tuple[List[str], List[List[str]]]:
    """Return (headers, data_rows).  Strips ZigHouse type-hint sentinels."""
    text = raw.decode("utf-8", errors="replace").replace("\x00", "")
    if not text.strip():
        return [], []
    reader = csv.reader(io.StringIO(text))
    rows = list(reader)
    if not rows:
        return [], []
    headers = [re.sub(r'^[\x02\x03][^:]+:', '', h) for h in rows[0]]
    return headers, rows[1:]


def normalise_row(row: List[str]) -> List[str]:
    """Normalise floats to 6 significant figures so minor FP differences don't
    produce different checksums."""
    out = []
    for v in row:
        try:
            f = float(v)
            # Use %g with 6 sig figs; avoids trailing zeros and exp notation mismatch.
            out.append(f"{f:.6g}")
        except ValueError:
            out.append(v)
    return out


def checksum_of(rows: List[List[str]]) -> str:
    """SHA256 of sorted, normalised, deterministic CSV representation."""
    normalised = [normalise_row(r) for r in rows]
    sorted_rows = sorted(normalised)
    buf = io.StringIO()
    writer = csv.writer(buf)
    for r in sorted_rows:
        writer.writerow(r)
    return hashlib.sha256(buf.getvalue().encode()).hexdigest()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Capture ZigHouse IR query result checksums for all 43 ClickBench queries."
    )
    parser.add_argument("--store",
        default="/tmp/zighouse-precommit-ir-store",
        help="Generic IR store directory")
    parser.add_argument("--queries",
        default="clickbench-submit/zighouse/queries.sql",
        help="SQL file with semicolon-separated queries")
    parser.add_argument("--zighouse",
        default="./zig-out/bin/zighouse",
        help="Path to zighouse binary")
    parser.add_argument("--out",
        default="perf/baselines/local-10m-ir-results.json",
        help="Output JSON file")
    parser.add_argument("--query", type=int, default=None,
        help="Update only query N (1-based); merges with existing file")
    parser.add_argument("--no-build", action="store_true",
        help="Skip zig build step")
    parser.add_argument("--verbose", action="store_true",
        help="Print first few result rows per query")
    args = parser.parse_args()

    if not args.no_build:
        print("Building zighouse (no DuckDB)…")
        result = subprocess.run(
            ["zig", "build", "-Dduckdb=false", "-Doptimize=ReleaseFast"],
            capture_output=False,
        )
        if result.returncode != 0:
            sys.exit(result.returncode)

    queries = load_queries(args.queries)
    print(f"Loaded {len(queries)} queries from {args.queries}")

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Load existing file so we can merge when --query is used.
    existing: dict = {}
    if out_path.exists():
        try:
            existing = json.loads(out_path.read_text())
        except Exception:
            pass

    results_list: List[Optional[dict]] = [None] * len(queries)

    # Preserve existing entries if we're doing a partial update.
    for entry in existing.get("results", []):
        q = entry.get("q", 0)
        if 1 <= q <= len(queries):
            results_list[q - 1] = entry

    errors = []
    for i, sql in enumerate(queries, 1):
        if args.query is not None and i != args.query:
            continue

        print(f"Q{i:02d} … ", end="", flush=True)
        raw = run_zighouse_query(args.zighouse, args.store, sql)
        if raw is None:
            print("ERROR (skipped)")
            errors.append(i)
            # Keep existing checksum if partial update.
            if args.query is None:
                results_list[i - 1] = {"q": i, "rows": None, "sorted_sha256": None, "error": True}
            continue

        headers, rows = parse_csv(raw)
        chk = checksum_of(rows)
        entry = {"q": i, "rows": len(rows), "sorted_sha256": chk}
        results_list[i - 1] = entry
        print(f"OK  ({len(rows)} rows)  sha256={chk[:16]}…")
        if args.verbose and rows:
            for r in rows[:3]:
                print(f"    {r}")

    # Build output document.
    try:
        git_commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip()
        git_dirty = bool(subprocess.check_output(
            ["git", "status", "--short"], text=True
        ).strip())
    except Exception:
        git_commit = None
        git_dirty = None

    doc = {
        "schema_version": 1,
        "description": (
            "Per-query result checksums (sorted-CSV SHA256) for the ZigHouse IR pipeline. "
            "Regenerate with: python3 scripts/update-result-baseline.py"
        ),
        "store": args.store,
        "queries": args.queries,
        "build": {
            "git_commit": git_commit,
            "git_dirty": git_dirty,
        },
        "results": [r for r in results_list if r is not None],
    }

    out_path.write_text(json.dumps(doc, indent=2) + "\n")
    print()
    print(f"Written {len(doc['results'])} entries to {out_path}")
    if errors:
        print(f"WARNING: {len(errors)} queries failed and were recorded as errors: {errors}")
        sys.exit(1)
    else:
        print("All queries captured successfully.")


if __name__ == "__main__":
    main()
