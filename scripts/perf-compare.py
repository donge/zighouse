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


def compare_summary(data: dict):
    return data.get("query", {}).get("compare")


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare a ZigHouse perf result against a baseline.")
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--query-threshold", type=float, default=5.0, help="Allowed warm_best_sum regression percent")
    parser.add_argument("--import-threshold", type=float, default=7.5, help="Allowed import wall regression percent")
    parser.add_argument("--per-query-threshold", type=float, default=20.0, help="Allowed per-query warm best regression percent")
    # DuckDB-relative gates (optional — only active when --duckdb-ref is supplied).
    parser.add_argument("--duckdb-ref", type=Path, default=None,
                        help="Path to DuckDB reference JSON (perf/baselines/duckdb-10m.json). "
                             "When supplied, enables two additional gates.")
    parser.add_argument("--duckdb-sum-ratio", type=float, default=1.0,
                        help="Gate: candidate warm_best_sum must be < duckdb_sum × ratio (default 1.0 = must beat DuckDB)")
    parser.add_argument("--duckdb-query-ratio", type=float, default=2.0,
                        help="Gate: each per-query warm best must be < duckdb_query × ratio (default 2.0). "
                             "A floor of 1 ms is applied to DuckDB reference times so near-zero queries are not penalised.")
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
        if d > args.per_query_threshold and c - b > 0.005:
            failures.append(f"q{idx} warm best regressed {d:.2f}% ({b:.6f}s -> {c:.6f}s)")

    # ── DuckDB-relative gates ──────────────────────────────────────────────────
    duck_violations: list[str] = []
    duck_ref_sum_ms: float | None = None
    if args.duckdb_ref is not None:
        duck_ref = load(args.duckdb_ref)
        duck_ref_sum_ms = float(duck_ref["warm_best_sum_ms"])
        duck_per_q_ms: list[float] = [float(v) if v is not None else 0.0
                                      for v in duck_ref["per_query_ms"]]
        FLOOR_MS = 2.0  # floor for near-zero DuckDB times (DuckDB timer has 1ms resolution)

        # Gate A: overall warm_best_sum < duckdb_sum × duckdb_sum_ratio
        cand_sum_ms = float(cand["query"]["warm_best_sum"]) * 1000.0
        limit_ms = duck_ref_sum_ms * args.duckdb_sum_ratio
        if cand_sum_ms >= limit_ms:
            failures.append(
                f"duckdb-sum gate: {cand_sum_ms:.1f}ms >= {limit_ms:.1f}ms "
                f"(DuckDB {duck_ref_sum_ms:.1f}ms × {args.duckdb_sum_ratio})"
            )

        # Gate B: per-query warm best < duckdb_time × duckdb_query_ratio
        cand_timings = cand["query"]["timings"]
        for idx, crow in enumerate(cand_timings, 1):
            cvals = [float(x) for x in crow[1:] if x is not None]
            if not cvals:
                continue
            c_ms = min(cvals) * 1000.0
            ref_ms = max(FLOOR_MS, duck_per_q_ms[idx - 1] if idx - 1 < len(duck_per_q_ms) else FLOOR_MS)
            limit_q = ref_ms * args.duckdb_query_ratio
            if c_ms >= limit_q:
                duck_violations.append((idx, c_ms, ref_ms, c_ms / ref_ms))
                failures.append(
                    f"duckdb-query gate: q{idx} {c_ms:.1f}ms >= {limit_q:.1f}ms "
                    f"(DuckDB {ref_ms:.1f}ms × {args.duckdb_query_ratio})"
                )

    base_cmp = compare_summary(base)
    cand_cmp = compare_summary(cand)
    compare_generic = []
    compare_specialized = []
    if base_cmp is not None and cand_cmp is not None:
        base_gen = float(base_cmp["warm_best_generic_sum"])
        cand_gen = float(cand_cmp["warm_best_generic_sum"])
        base_spec = float(base_cmp["warm_best_specialized_sum"])
        cand_spec = float(cand_cmp["warm_best_specialized_sum"])
        generic_delta = pct(cand_gen, base_gen)
        specialized_delta = pct(cand_spec, base_spec)
        for idx, (brows, crows) in enumerate(zip(base_cmp["timings"], cand_cmp["timings"]), 1):
            bg = min(float(row["generic_seconds"]) for row in brows)
            cg = min(float(row["generic_seconds"]) for row in crows)
            bs = min(float(row["specialized_seconds"]) for row in brows)
            cs = min(float(row["specialized_seconds"]) for row in crows)
            compare_generic.append((idx, bg, cg, pct(cg, bg)))
            compare_specialized.append((idx, bs, cs, pct(cs, bs)))
        if not cand_cmp.get("all_equal", False):
            failures.append("compare output equality failed")
    else:
        base_gen = cand_gen = base_spec = cand_spec = generic_delta = specialized_delta = None

    if base_path is not None or cand_path is not None:
        print(f"query_path: {base_path!r} -> {cand_path!r}")
    print(f"warm_best_sum: {base_q:.6f}s -> {cand_q:.6f}s ({q_delta:+.2f}%)")
    if duck_ref_sum_ms is not None:
        cand_sum_ms = float(cand["query"]["warm_best_sum"]) * 1000.0
        duck_ratio = cand_sum_ms / duck_ref_sum_ms
        gate_a_symbol = "PASS" if cand_sum_ms < duck_ref_sum_ms * args.duckdb_sum_ratio else "FAIL"
        print(f"vs DuckDB sum: {cand_sum_ms:.1f}ms / {duck_ref_sum_ms:.1f}ms = {duck_ratio:.3f}x  [{gate_a_symbol}]")
    if base_cmp is not None and cand_cmp is not None:
        print(f"compare_generic_sum: {base_gen:.6f}s -> {cand_gen:.6f}s ({generic_delta:+.2f}%)")
        print(f"compare_specialized_sum: {base_spec:.6f}s -> {cand_spec:.6f}s ({specialized_delta:+.2f}%)")
    if import_delta is not None:
        print(f"import_{import_metric}: {float(base_import):.6f}s -> {float(cand_import):.6f}s ({import_delta:+.2f}%)")
    print("largest per-query regressions:")
    for idx, b, c, d in sorted(per_query, key=lambda x: x[3], reverse=True)[:10]:
        print(f"  q{idx}: {b:.6f}s -> {c:.6f}s ({d:+.2f}%)")
    if duck_violations:
        print("duckdb-query gate failures (zh >= duckdb × ratio):")
        for idx, c_ms, ref_ms, ratio_v in sorted(duck_violations, key=lambda x: x[3], reverse=True):
            print(f"  q{idx}: {c_ms:.1f}ms vs DuckDB {ref_ms:.1f}ms ({ratio_v:.2f}x)")
    if compare_generic:
        print("largest compare generic regressions:")
        for idx, b, c, d in sorted(compare_generic, key=lambda x: x[3], reverse=True)[:10]:
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
