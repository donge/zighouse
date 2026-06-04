#!/usr/bin/env python3
"""
Compare ZigHouse IR-pipeline query results against DuckDB for all 43 ClickBench queries.

Usage:
    python3 scripts/compare-results-duckdb.py \
        --store /tmp/zighouse-generic-store \
        --db data/hits_10m.parquet.duckdb \
        --queries clickbench-submit/zighouse/queries.sql \
        [--zighouse ./zig-out/bin/zighouse] \
        [--verbose]

Exit code 0 = all queries match; non-zero = mismatches found.
"""

import argparse
import csv
import io
import math
import re
import subprocess
import sys
from typing import List, Optional, Tuple


# ── DuckDB query adaptation (copied from bench-vs-duckdb.py) ─────────────────

def adapt_query_for_duckdb(q: str) -> str:
    def replace_date_cmp(m):
        col, op, date_str = m.group(1), m.group(2), m.group(3)
        return f"{col} {op} (DATE '{date_str}' - DATE '1970-01-01')"
    q = re.sub(
        r'\b(EventDate)\s*(>=|<=|=|>|<|!=|<>)\s*\'(\d{4}-\d{2}-\d{2})\'',
        replace_date_cmp,
        q,
    )
    q = re.sub(
        r'\bEXTRACT\s*\(\s*(\w+)\s+FROM\s+EventTime\s*\)',
        lambda m: f"date_part('{m.group(1)}', to_timestamp(EventTime))",
        q,
        flags=re.IGNORECASE,
    )
    q = re.sub(
        r"\bdate_part\s*\(\s*('[^']+'),\s*EventTime\s*\)",
        r"date_part(\1, to_timestamp(EventTime))",
        q,
        flags=re.IGNORECASE,
    )
    q = re.sub(
        r"\bDATE_TRUNC\s*\(\s*('[^']+'),\s*EventTime\s*\)",
        r"DATE_TRUNC(\1, to_timestamp(EventTime))",
        q,
        flags=re.IGNORECASE,
    )
    return q


# ── Load queries ──────────────────────────────────────────────────────────────

def load_queries(path: str) -> List[str]:
    with open(path) as f:
        raw = f.read()
    queries = [q.strip() for q in raw.split(";") if q.strip()]
    return queries


# ── Run DuckDB ────────────────────────────────────────────────────────────────

def run_duckdb_query(db_path: str, sql: str) -> Optional[str]:
    duck_sql = adapt_query_for_duckdb(sql)
    cmd = ["duckdb", db_path, "-csv", "-c", duck_sql]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.returncode != 0:
            return None
        return result.stdout
    except Exception as e:
        print(f"  DuckDB error: {e}", file=sys.stderr)
        return None


# ── Run ZigHouse IR pipeline ──────────────────────────────────────────────────

def run_zighouse_query(zighouse: str, store: str, sql: str) -> Optional[str]:
    cmd = [zighouse, "ir-query", store, "hits", sql]
    try:
        result = subprocess.run(cmd, capture_output=True, timeout=120)
        if result.returncode != 0:
            return None
        # Decode with errors='replace' to handle non-UTF-8 bytes in binary columns.
        return result.stdout.decode('utf-8', errors='replace')
    except Exception as e:
        print(f"  ZigHouse error: {e}", file=sys.stderr)
        return None


# ── Parse CSV output ──────────────────────────────────────────────────────────

def parse_csv(text: str) -> Tuple[List[str], List[List[str]]]:
    """Return (headers, rows). Strip type-hint sentinels from ZigHouse headers."""
    if not text or not text.strip():
        return [], []
    # Remove NUL bytes that can appear in binary columns
    text = text.replace('\x00', '')
    reader = csv.reader(io.StringIO(text))
    rows = list(reader)
    if not rows:
        return [], []
    # Strip ZigHouse sentinel prefixes (\x03U8:  \x02D:) from header names
    headers = [re.sub(r'^[\x02\x03][^:]+:', '', h) for h in rows[0]]
    data = rows[1:]
    return headers, data


# ── Numeric comparison ────────────────────────────────────────────────────────

REL_TOL = 1e-4   # 0.01% relative tolerance for floats

def values_match(a: str, b: str) -> bool:
    """Compare two cell values with tolerance for floats."""
    if a == b:
        return True
    # Try numeric comparison
    try:
        fa, fb = float(a), float(b)
        if math.isnan(fa) and math.isnan(fb):
            return True
        if fa == 0 and fb == 0:
            return True
        return abs(fa - fb) / max(abs(fa), abs(fb), 1.0) <= REL_TOL
    except ValueError:
        return False


def rows_match(zh_rows: List[List[str]], duck_rows: List[List[str]], sort: bool) -> bool:
    if len(zh_rows) != len(duck_rows):
        return False
    if sort:
        zh_sorted = sorted(zh_rows)
        duck_sorted = sorted(duck_rows)
    else:
        zh_sorted = zh_rows
        duck_sorted = duck_rows
    for r1, r2 in zip(zh_sorted, duck_sorted):
        if len(r1) != len(r2):
            return False
        for v1, v2 in zip(r1, r2):
            if not values_match(v1, v2):
                return False
    return True


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Compare ZigHouse vs DuckDB query results")
    parser.add_argument("--store", default="/tmp/zighouse-generic-store")
    parser.add_argument("--db", default="data/hits_10m.parquet.duckdb")
    parser.add_argument("--queries", default="clickbench-submit/zighouse/queries.sql")
    parser.add_argument("--zighouse", default="./zig-out/bin/zighouse")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--query", type=int, default=None, help="Run only query N (1-indexed)")
    args = parser.parse_args()

    queries = load_queries(args.queries)
    print(f"Loaded {len(queries)} queries from {args.queries}")

    mismatches = []
    errors = []

    for i, sql in enumerate(queries, 1):
        if args.query is not None and i != args.query:
            continue

        print(f"Q{i:02d} ... ", end="", flush=True)

        zh_out = run_zighouse_query(args.zighouse, args.store, sql)
        duck_out = run_duckdb_query(args.db, sql)

        if zh_out is None:
            print("ZH_ERROR")
            errors.append(i)
            continue
        if duck_out is None:
            print("DUCK_ERROR")
            errors.append(i)
            continue

        zh_hdr, zh_rows = parse_csv(zh_out)
        duck_hdr, duck_rows = parse_csv(duck_out)

        # Always sort both result sets by all columns for comparison.
        # This handles tie-breaking non-determinism in ORDER BY queries with equal-count rows.
        match = rows_match(zh_rows, duck_rows, sort=True)

        if match:
            print(f"OK  ({len(zh_rows)} rows)")
        else:
            print(f"MISMATCH  zh={len(zh_rows)} rows, duck={len(duck_rows)} rows")
            mismatches.append(i)
            if args.verbose:
                print(f"  SQL: {sql[:120]}")
                print(f"  ZH headers:   {zh_hdr}")
                print(f"  Duck headers: {duck_hdr}")
                print(f"  ZH rows (first 5):   {zh_rows[:5]}")
                print(f"  Duck rows (first 5): {duck_rows[:5]}")

    print()
    print(f"{'='*60}")
    print(f"Results: {len(queries) - len(mismatches) - len(errors)}/{len(queries)} matched")
    if errors:
        print(f"Errors (could not run): Q{sorted(errors)}")
    if mismatches:
        print(f"Mismatches: Q{sorted(mismatches)}")
        sys.exit(1)
    else:
        print("All queries match!")
        sys.exit(0)


if __name__ == "__main__":
    main()
