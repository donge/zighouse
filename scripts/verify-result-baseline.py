#!/usr/bin/env python3
"""
Verify ZigHouse IR query results against stored checksums.

Used by pre-commit-perf.sh.  Exits non-zero if any stored checksum mismatches
the live output, so correctness regressions are caught alongside perf checks.

Usage:
    python3 scripts/verify-result-baseline.py \
        --store   /tmp/zighouse-precommit-ir-store \
        --baseline perf/baselines/local-10m-ir-results.json \
        --zighouse ./zig-out/bin/zighouse
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
# Shared helpers (duplicated from update-result-baseline.py to keep scripts
# self-contained and importable without a package structure).
# ---------------------------------------------------------------------------

def load_queries(path: str) -> List[str]:
    with open(path) as f:
        raw = f.read()
    return [q.strip() for q in raw.split(";") if q.strip()]


def run_zighouse_query(zighouse: str, store: str, sql: str) -> Optional[bytes]:
    cmd = [zighouse, "query", store, "hits", sql]
    try:
        result = subprocess.run(cmd, capture_output=True, timeout=120)
        if result.returncode != 0:
            return None
        return result.stdout
    except Exception as e:
        print(f"  ZigHouse error: {e}", file=sys.stderr)
        return None


def parse_csv(raw: bytes) -> Tuple[List[str], List[List[str]]]:
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
    out = []
    for v in row:
        try:
            out.append(f"{float(v):.6g}")
        except ValueError:
            out.append(v)
    return out


def checksum_of(rows: List[List[str]]) -> str:
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
        description="Verify ZigHouse IR query results against stored checksums."
    )
    parser.add_argument("--store", required=True)
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--zighouse", default="./zig-out/bin/zighouse")
    parser.add_argument("--queries",
        default="clickbench-submit/zighouse/queries.sql")
    args = parser.parse_args()

    baseline_path = Path(args.baseline)
    if not baseline_path.exists():
        print(f"verify-result-baseline: baseline file not found: {args.baseline}",
              file=sys.stderr)
        sys.exit(1)

    doc = json.loads(baseline_path.read_text())
    stored: dict = {e["q"]: e for e in doc.get("results", [])}

    queries = load_queries(args.queries)

    mismatches = []
    errors = []
    skipped = []

    for i, sql in enumerate(queries, 1):
        entry = stored.get(i)
        if entry is None:
            skipped.append(i)
            continue
        if entry.get("error"):
            skipped.append(i)
            continue
        expected_sha = entry.get("sorted_sha256")
        expected_rows = entry.get("rows")
        if not expected_sha:
            skipped.append(i)
            continue

        print(f"  Q{i:02d} … ", end="", flush=True)
        raw = run_zighouse_query(args.zighouse, args.store, sql)
        if raw is None:
            print("EXEC_ERROR")
            errors.append(i)
            continue

        _, rows = parse_csv(raw)
        actual_sha = checksum_of(rows)
        actual_rows = len(rows)

        if actual_sha == expected_sha:
            print(f"OK  ({actual_rows} rows)")
        else:
            row_info = (f"{actual_rows} rows (expected {expected_rows})"
                        if actual_rows != expected_rows
                        else f"{actual_rows} rows")
            print(f"MISMATCH  {row_info}")
            print(f"    expected sha256: {expected_sha}")
            print(f"    actual   sha256: {actual_sha}")
            mismatches.append(i)

    print()
    total_checked = len(queries) - len(skipped) - len(errors)
    passed = total_checked - len(mismatches)
    print(f"Result check: {passed}/{total_checked} correct"
          + (f"  (skipped {len(skipped)} without stored checksum)" if skipped else ""))

    if errors:
        print(f"ERRORS (could not run): Q{sorted(errors)}", file=sys.stderr)

    if mismatches:
        print(f"CORRECTNESS REGRESSIONS detected for: Q{sorted(mismatches)}", file=sys.stderr)
        print("", file=sys.stderr)
        print("If this is an intentional result change (e.g. a bug fix), regenerate", file=sys.stderr)
        print("the baseline with:", file=sys.stderr)
        print("    python3 scripts/update-result-baseline.py", file=sys.stderr)
        sys.exit(1)

    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
