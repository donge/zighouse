# ZigHouse Generalization Roadmap

## Background

ZigHouse is a native analytical engine optimized for ClickBench.  The codebase
started as a benchmark-specific binary and has been progressively generalized
across Phases A–G while preserving the specialized ClickBench performance path.

The goal is to support arbitrary analytical SQL on flat Parquet datasets without
losing the existing ClickBench performance baseline unless a trade-off is
explicitly reviewed.

## Principles

1. Preserve performance first; stop adding new query-specific benchmark hacks.
2. Keep the ClickBench fast path, but isolate it in ClickBench-specific modules.
3. Run performance regression checks before and after refactors:

```sh
scripts/perf-baseline.sh data/hits.parquet /tmp/zighouse-perf-store /tmp/zighouse-candidate.json 10000000
scripts/perf-compare.py perf/baselines/local-10m-submit.json /tmp/zighouse-candidate.json
```

4. If a generality improvement regresses performance, record the regression,
   likely cause, and whether the trade-off is acceptable before merging.
5. Default gates:
   - Query `warm_best_sum`: max `5%` regression.
   - Import total: max `7.5%` regression.
   - Per-query warm best: max `20%` regression and at least `2ms` absolute increase.

## Current Performance Baseline

Baseline file: `perf/baselines/local-10m-submit.json`

- Data: `data/hits.parquet`
- Rows: `10,000,000`
- Build: `zig build -Dduckdb=false`
- Query path: `specialized` (`ZIGHOUSE_QUERY_PATH=specialized`)
- Mode: `ZIGHOUSE_CLICKBENCH_SUBMIT=1`
- Repeats: 3, using medians
- Import total: ~`7.21s`
- Query `warm_best_sum`: ~`1.26s`
- Store size: `1121101103` bytes
- Queries: `43`, Nulls: `0`

## Completed Work

### Phase A: Schema-Driven Operator Generalization (v0.6 era)

- PR-A5/A6/A7/A9: introduced `CapabilityTag`, `BoundColumn`, `PlanShape`,
  `bindColumn`; dropped dead reducers; folded `length(URL)` into native column
  binding; schema-driven filter rewrite via `asGroup`/`ReduceFilterColumn`.

### Phase B: Physical Column Adapter Layer

- PR-B2a/B2b: `BoundColumn` → physical `Column` adapter, dedup
  `GenericColumn`/`bindGenericColumn`.
- PR-B3a: extracted late-materialize core + Q21/Q34 to
  `src/clickbench/late_materialize.zig`.

### Phase G: DuckDB Parser + Generic Parquet Streaming Executor

- PR-G1: allow non-`hits` table names in generic SQL parser.
- PR-G2: DuckDB-backed SQL parser via `json_serialize_sql`.
- PR-G3a: singleton DuckDB parser connection (2.8 ms → ~66 µs per parse).
- PR-G5: generic executor with `WhereNode`, date comparison, scan path; arena
  leak fix in `readMetadataFromFile`.
- PR-G6/G6b: generic parquet-streaming executor; enable generic fallback for
  q28/q36/q43; smoke tests against `fixture_hits.parquet`.

Result: 43/43 ClickBench queries hit the specialized path with byte-identical
output; arbitrary `hits`-table SQL falls through to the generic Parquet executor.

## Roadmap

### Next: Phase H – General Table Import

Goal: import arbitrary flat Parquet tables via `zighouse import-parquet`.

- Infer schema from Parquet metadata.
- Use import-time sampling to choose physical encoding.
- Write generic table manifest (complement to ClickBench-specific sidecar).
- Materialized fixed-width numeric columns; dictionary-encoded low-cardinality
  strings; lazy/hash for high-cardinality strings.

Acceptance:
- `zighouse import-parquet <file.parquet> <store_dir> <table_name>` works on
  arbitrary flat Parquet.
- Generic SQL executor can query the resulting store.
- ClickBench specialized path unaffected.

### Phase I – Encoding Policy

Replace hardcoded storage hints with a deterministic policy driven by import-time
sampling.  Record policy in manifest.  Allow ClickBench-specific profile overrides.

### Phase J – Correctness and Differential Testing

Expand DuckDB differential tests beyond ClickBench.  Add operator unit tests,
parser snapshot tests, planner tests.

## Development Workflow

1. Generate or inspect the current baseline.
2. Implement the smallest behavior-preserving change.
3. Run:

```sh
zig build -Dduckdb=false
zig build test
```

4. Generate a candidate result:

```sh
scripts/perf-baseline.sh data/hits.parquet /tmp/zighouse-after-store /tmp/zighouse-after.json 10000000
```

5. Compare:

```sh
scripts/perf-compare.py perf/baselines/local-10m-submit.json /tmp/zighouse-after.json
```

6. If performance fails, determine whether it is noise, a bug, or an intentional
   generality trade-off.  Do not merge accidental regressions.
