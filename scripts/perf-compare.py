#!/usr/bin/env python3
import argparse
import json
import sys
from pathlib import Path


def load(path: Path) -> dict:
    return json.loads(path.read_text())


def pct(new: float, base: float) -> float:
    if base == 0:
        return 0.0 if new == 0 else float("inf")
    return (new - base) / base * 100.0


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare a ZigHouse perf result against a baseline.")
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--query-threshold", type=float, default=5.0, help="Allowed warm_best_sum regression percent")
    parser.add_argument("--import-threshold", type=float, default=7.5, help="Allowed import wall regression percent")
    parser.add_argument("--per-query-threshold", type=float, default=20.0, help="Allowed per-query warm best regression percent")
    args = parser.parse_args()

    base = load(args.baseline)
    cand = load(args.candidate)
    failures: list[str] = []

    base_path = base.get("query_path")
    cand_path = cand.get("query_path")
    if base_path != cand_path:
        failures.append(f"query_path mismatch ({base_path!r} baseline vs {cand_path!r} candidate)")

    base_q = float(base["query"]["warm_best_sum"])
    cand_q = float(cand["query"]["warm_best_sum"])
    q_delta = pct(cand_q, base_q)
    if q_delta > args.query_threshold:
        failures.append(f"warm_best_sum regressed {q_delta:.2f}% ({base_q:.6f}s -> {cand_q:.6f}s)")

    import_metric = "total_seconds"
    base_import = base["import"].get(import_metric)
    cand_import = cand["import"].get(import_metric)
    if base_import is None or cand_import is None:
        import_metric = "wall_seconds"
        base_import = base["import"].get(import_metric)
        cand_import = cand["import"].get(import_metric)
    if base_import is not None and cand_import is not None:
        import_delta = pct(float(cand_import), float(base_import))
        if import_delta > args.import_threshold:
            failures.append(f"import {import_metric} regressed {import_delta:.2f}% ({base_import:.6f}s -> {cand_import:.6f}s)")
    else:
        import_delta = None

    per_query = []
    for idx, (brow, crow) in enumerate(zip(base["query"]["timings"], cand["query"]["timings"]), 1):
        bvals = [float(x) for x in brow[1:] if x is not None]
        cvals = [float(x) for x in crow[1:] if x is not None]
        if not bvals or not cvals:
            continue
        b = min(bvals)
        c = min(cvals)
        d = pct(c, b)
        per_query.append((idx, b, c, d))
        if d > args.per_query_threshold and c - b > 0.002:
            failures.append(f"q{idx} warm best regressed {d:.2f}% ({b:.6f}s -> {c:.6f}s)")

    if base_path is not None or cand_path is not None:
        print(f"query_path: {base_path!r} -> {cand_path!r}")
    print(f"warm_best_sum: {base_q:.6f}s -> {cand_q:.6f}s ({q_delta:+.2f}%)")
    if import_delta is not None:
        print(f"import_{import_metric}: {float(base_import):.6f}s -> {float(cand_import):.6f}s ({import_delta:+.2f}%)")
    print("largest per-query regressions:")
    for idx, b, c, d in sorted(per_query, key=lambda x: x[3], reverse=True)[:10]:
        print(f"  q{idx}: {b:.6f}s -> {c:.6f}s ({d:+.2f}%)")

    if failures:
        print("FAIL:", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        return 1
    print("PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
