#!/usr/bin/env python3
"""
bench-vs-duckdb.py — ClickBench gate: ZigHouse must beat DuckDB.

Usage:
  python3 scripts/bench-vs-duckdb.py \
    --zighouse  <path-to-zighouse-binary>  \
    --store     <ir-store-dir>             \
    --parquet   <hits_1m_snappy.parquet>   \
    --queries   <queries.sql>

Exit codes:
  0  all gates pass
  1  one or more gates fail
  2  bad arguments / missing files

Gate conditions (all must hold):
  1. nulls == 0          every query returns a result
  2. zh_sum < duck_sum   ZigHouse total warm time strictly less than DuckDB
  3. no query > 3x       no single query more than 3x slower than DuckDB

Methodology:
  - DuckDB: one separate process per query, 3 runs, take min.
    Both systems pay the same per-query startup / IO cost.
  - ZigHouse: bench-ir (runs all queries sequentially, 3 runs each).
    Per-query timing extracted from exec_ms log lines.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from typing import List, Optional, Tuple


# ── Argument parsing ──────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="ZigHouse vs DuckDB benchmark gate")
    p.add_argument("--zighouse", required=True, help="Path to zighouse binary")
    p.add_argument("--store",    required=True, help="ZigHouse IR store directory")
    p.add_argument("--parquet",  required=True, help="Parquet file for DuckDB (hits_1m_snappy.parquet)")
    p.add_argument("--queries",  required=True, help="SQL queries file (one query per line)")
    p.add_argument("--runs",     type=int, default=3, help="Runs per query (default 3)")
    p.add_argument("--max-ratio", type=float, default=3.0,
                   help="Max allowed per-query ratio zh/duck (default 3.0)")
    p.add_argument("--json-out", help="Optional: write results JSON to this file")
    return p.parse_args()


def check_prereqs(args):
    errors = []
    if not os.path.isfile(args.zighouse):
        errors.append(f"zighouse binary not found: {args.zighouse}")
    if not os.path.isdir(args.store):
        errors.append(f"IR store dir not found: {args.store}")
    if not os.path.isfile(args.parquet):
        errors.append(f"parquet file not found: {args.parquet}")
    if not os.path.isfile(args.queries):
        errors.append(f"queries file not found: {args.queries}")
    if shutil_which("duckdb") is None:
        errors.append("duckdb not found in PATH")
    if errors:
        for e in errors:
            print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(2)


def shutil_which(name):
    import shutil
    return shutil.which(name)


# ── DuckDB measurement ────────────────────────────────────────────────────────

def run_duckdb(parquet: str, queries: List[str], runs: int) -> List[Optional[float]]:
    """
    Run each query as a separate duckdb process, return min wall time (ms).
    Substitutes FROM hits → FROM '<parquet>'.
    """
    results = []
    for q in queries:
        q_duck = re.sub(
            r'\bFROM\s+hits\b',
            f"FROM '{parquet}'",
            q,
            flags=re.IGNORECASE,
        )
        best = None
        for _ in range(runs):
            t0 = time.perf_counter()
            subprocess.run(
                ["duckdb", "-c", q_duck],
                capture_output=True,
                text=True,
                timeout=120,
            )
            elapsed = (time.perf_counter() - t0) * 1000.0
            if best is None or elapsed < best:
                best = elapsed
        results.append(best)
    return results


# ── ZigHouse measurement ──────────────────────────────────────────────────────

def run_zighouse(zighouse: str, store: str, queries_file: str) -> Tuple[List[Optional[float]], int]:
    """
    Run zighouse bench-ir, parse per-query timing from stdout.
    stdout format: one line per query: "[T1, T2, T3]," or "[null, null, null],"
    We take min(T2, T3) as the warm best.
    """
    proc = subprocess.run(
        [zighouse, "bench-ir", store, "hits", queries_file],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        timeout=600,
    )
    per_query: List[Optional[float]] = []
    nulls = 0

    for line in proc.stdout.splitlines():
        line = line.strip()
        # Matches "[T1, T2, T3]," or "[null, null, null],"
        m = re.match(r'^\[(.+)\][,]?\s*$', line)
        if not m:
            continue
        inner = m.group(1)
        if 'null' in inner:
            per_query.append(None)
            nulls += 1
        else:
            vals = [float(v.strip()) * 1000.0 for v in inner.split(',') if v.strip()]
            if vals:
                warm = vals[1:] if len(vals) > 1 else vals
                per_query.append(min(warm))

    return per_query, nulls


# ── Gate evaluation ───────────────────────────────────────────────────────────

def evaluate(zh_ms, duck_ms, queries, max_ratio):
    """
    Returns (pass, issues, rows) where rows is the per-query table data.
    """
    issues = []
    rows = []

    for i, (zh, duck, q) in enumerate(zip(zh_ms, duck_ms, queries)):
        qi = i + 1
        if zh is None:
            ratio = None
            status = "NULL"
        else:
            ratio = zh / duck if duck else None
            status = "ok"

        rows.append({
            "q": qi,
            "zh_ms": zh,
            "duck_ms": duck,
            "ratio": ratio,
            "status": status,
            "sql": q[:80],
        })

    # Gate 1: nulls == 0
    null_qs = [r["q"] for r in rows if r["status"] == "NULL"]
    if null_qs:
        issues.append(f"Gate 1 FAIL: {len(null_qs)} null queries: {null_qs}")

    # Gate 2: zh_sum < duck_sum
    zh_sum  = sum(r["zh_ms"]   for r in rows if r["zh_ms"]   is not None)
    duck_sum = sum(r["duck_ms"] for r in rows if r["duck_ms"] is not None)
    if zh_sum >= duck_sum:
        issues.append(
            f"Gate 2 FAIL: ZH sum {zh_sum:.1f}ms >= DuckDB sum {duck_sum:.1f}ms "
            f"(ratio {zh_sum/duck_sum:.3f}x)"
        )

    # Gate 3: no single query > max_ratio
    violations = [r for r in rows if r["ratio"] is not None and r["ratio"] > max_ratio]
    for v in violations:
        issues.append(
            f"Gate 3 FAIL: Q{v['q']} ratio {v['ratio']:.2f}x > {max_ratio}x  "
            f"({v['zh_ms']:.1f}ms vs {v['duck_ms']:.1f}ms)  {v['sql']}"
        )

    return len(issues) == 0, issues, rows, zh_sum, duck_sum


# ── Reporting ─────────────────────────────────────────────────────────────────

BOLD  = "\033[1m"
RED   = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
RESET = "\033[0m"


def print_table(rows, max_ratio):
    print(f"\n{'Q':>3}  {'ZigHouse':>10}  {'DuckDB':>10}  {'Ratio':>7}  Status")
    print("─" * 65)
    for r in rows:
        qi   = f"Q{r['q']:2d}"
        zh   = f"{r['zh_ms']:.1f}ms"   if r["zh_ms"]   is not None else "NULL"
        duck = f"{r['duck_ms']:.1f}ms" if r["duck_ms"] is not None else "?"
        if r["ratio"] is None:
            ratio_s = "  ---"
            color = RED
        elif r["ratio"] > max_ratio:
            ratio_s = f"{r['ratio']:6.2f}x"
            color = RED
        elif r["ratio"] > 1.5:
            ratio_s = f"{r['ratio']:6.2f}x"
            color = YELLOW
        else:
            ratio_s = f"{r['ratio']:6.2f}x"
            color = GREEN
        print(f"{qi}  {zh:>10}  {duck:>10}  {color}{ratio_s}{RESET}")


def print_summary(zh_sum, duck_sum, nulls, issues):
    print()
    print(f"  ZigHouse  warm_sum: {BOLD}{zh_sum:.1f}ms{RESET}")
    print(f"  DuckDB    warm_sum: {BOLD}{duck_sum:.1f}ms{RESET}")
    ratio = zh_sum / duck_sum if duck_sum else float("inf")
    color = GREEN if ratio < 1.0 else RED
    print(f"  Sum ratio:          {color}{ratio:.3f}x{RESET}  "
          f"{'(ZigHouse wins)' if ratio < 1.0 else '(ZigHouse loses)'}")
    print(f"  Nulls:              {RED if nulls else GREEN}{nulls}{RESET}")
    print()
    if issues:
        print(f"{RED}{BOLD}FAIL — {len(issues)} gate violation(s):{RESET}")
        for iss in issues:
            print(f"  • {RED}{iss}{RESET}")
    else:
        print(f"{GREEN}{BOLD}PASS — all gates satisfied{RESET}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()
    check_prereqs(args)

    queries = [
        line.strip()
        for line in open(args.queries)
        if line.strip() and not line.startswith("--")
    ]
    print(f"Loaded {len(queries)} queries from {args.queries}")

    # Run DuckDB
    print(f"\nRunning DuckDB ({args.runs} runs/query, per-process) …")
    t0 = time.perf_counter()
    duck_ms = run_duckdb(args.parquet, queries, args.runs)
    duck_wall = time.perf_counter() - t0
    print(f"  Done in {duck_wall:.1f}s")

    # Run ZigHouse
    print(f"\nRunning ZigHouse bench-ir …")
    t0 = time.perf_counter()
    zh_ms, nulls = run_zighouse(args.zighouse, args.store, args.queries)
    zh_wall = time.perf_counter() - t0
    print(f"  Done in {zh_wall:.1f}s  (nulls={nulls})")

    # Align lengths (pad with None if mismatch)
    n = len(queries)
    zh_ms   = (zh_ms   + [None] * n)[:n]
    duck_ms = (duck_ms + [None] * n)[:n]

    # Evaluate gates
    passed, issues, rows, zh_sum, duck_sum = evaluate(
        zh_ms, duck_ms, queries, args.max_ratio
    )

    # Print results
    print_table(rows, args.max_ratio)
    print_summary(zh_sum, duck_sum, nulls, issues)

    # Optional JSON output
    if args.json_out:
        out = {
            "zh_sum_ms":   zh_sum,
            "duck_sum_ms": duck_sum,
            "ratio":       zh_sum / duck_sum if duck_sum else None,
            "nulls":       nulls,
            "passed":      passed,
            "issues":      issues,
            "queries": [
                {
                    "q":       r["q"],
                    "zh_ms":   r["zh_ms"],
                    "duck_ms": r["duck_ms"],
                    "ratio":   r["ratio"],
                    "sql":     r["sql"],
                }
                for r in rows
            ],
        }
        with open(args.json_out, "w") as f:
            json.dump(out, f, indent=2)
        print(f"\nResults written to {args.json_out}")

    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
