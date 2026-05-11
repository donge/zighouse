# ZigHouse Generalization Roadmap

## Background

ZigHouse currently has a high-performance native columnar execution path for ClickBench, but the codebase is still organized around a ClickBench-specific workload:

- `src/native.zig` contains many query-pattern matchers and `formatQ...` execution paths.
- `src/schema.zig` is fixed to the ClickBench `hits` schema.
- `src/storage.zig` has many ClickBench hot-column, sidecar, and artifact file names.
- `src/planner.zig` only covers a few artifact plans and CSV count, not a general SQL planner.
- The reusable assets are the Parquet decoder, mmap column scans, SIMD/hashmap/parallel primitives, and columnar execution kernels.

The goal is to evolve ZigHouse from a benchmark-specific binary into a native analytical engine that can import flat Parquet datasets and execute a useful SQL subset, without losing the existing ClickBench performance baseline unless a trade-off is explicitly reviewed.

## Principles

1. Preserve performance first; stop adding new query-specific benchmark hacks.
2. Keep the ClickBench fast path, but isolate it in ClickBench-specific modules.
3. Run performance regression checks before and after refactors:

```sh
scripts/perf-baseline.sh data/hits.parquet /tmp/zighouse-perf-store /tmp/zighouse-candidate.json 10000000
scripts/perf-compare.py perf/baselines/local-10m-submit.json /tmp/zighouse-candidate.json
```

4. If a generality improvement regresses performance, record the regression, likely cause, and whether the trade-off is acceptable before merging.
5. Default gates:
   - Query `warm_best_sum`: max `5%` regression.
   - Import total: max `7.5%` regression.
   - Per-query warm best: max `20%` regression and at least `2ms` absolute increase.

## Current Performance Baseline

Baseline file:

- `perf/baselines/local-10m-submit.json`

Current baseline:

- Data: `data/hits.parquet`
- Rows: `10,000,000`
- Build: `zig build -Dduckdb=false`
- Mode: `ZIGHOUSE_CLICKBENCH_SUBMIT=1`
- Repeats: 3, using medians
- Import total: `7.398315s`
- Import wall: `7.40s`
- Query `warm_best_sum`: `1.303544s`
- Store size: `1121101092` bytes
- Queries: `43`
- Nulls: `0`

## Phase 1: Isolate ClickBench-Specific Logic

Goal: move ClickBench-specific boundaries without changing behavior.

Tasks:

- Add a ClickBench query module such as `src/clickbench.zig` or `src/clickbench_queries.zig`.
- Move ClickBench query matcher and dispatch shell out of `native.zig` where possible.
- Keep execution kernels unchanged at first.
- Keep query dispatch order unchanged, especially q34/q35 memoization and q37-q40 paths.

Acceptance:

- `zig build -Dduckdb=false` passes.
- `zig build test` passes.
- 10M performance compare passes.
- ClickBench correctness remains unchanged.

Risks:

- Dispatch order changes can affect cached state and memoization.
- Moving helpers too aggressively can cause inlining or allocation changes.

## Phase 2: General Store Manifest and Table Schema

Goal: move from fixed `hits_columns` toward self-describing stores.

Tasks:

- Introduce `TableSchema`: table name, columns, logical types, physical encodings, nullability, row count.
- Introduce `StoreManifest`: format version, tables, columns, segment size, import source.
- Keep existing ClickBench manifest compatibility while adding the new catalog path.
- Make `store-info` report table and column metadata.

Acceptance:

- Existing ClickBench stores still load.
- New manifest can describe the `hits` table.
- Performance compare passes.

## Phase 3: General Physical Operators

Goal: turn current specialized kernels into reusable operators.

Initial operators:

- `ColumnScan`
- `Predicate`
- `Projection`
- `HashAggregate`
- `TopK`
- `SortLimit`
- `LateMaterialize`
- `StringContains`
- `DictDecode`
- `CountDistinct`

Tasks:

- Create `src/engine/` or equivalent modules.
- Wrap hashmap/top-k/SIMD scan/parallel fan-out as reusable APIs.
- Gradually route low-risk ClickBench query classes through operators.
- Keep specialized fallback when generic operators are measurably slower.

Acceptance:

- Migrate low-risk query classes first, such as numeric aggregates and simple group-by/top-k.
- Run performance checks after each migration.

## Phase 4: Minimal SQL AST and Planner

Goal: support a practical analytical SQL subset without implementing full SQL.

Initial SQL subset:

- `SELECT expr... FROM table`
- `WHERE` with `=`, `<>`, `<`, `<=`, `>`, `>=`, `AND`, `IN`, `BETWEEN`, and `LIKE '%literal%'`
- `COUNT(*)`, `SUM`, `AVG`, `MIN`, `MAX`, `COUNT(DISTINCT)`
- `GROUP BY`
- `ORDER BY`
- `LIMIT`
- `OFFSET`

Tasks:

- Add parser and AST.
- Convert AST to physical plans.
- Support single-table queries first.
- Return clear unsupported-query errors for everything outside the subset.

Acceptance:

- Non-ClickBench SQL examples execute through the generic path.
- DuckDB differential tests pass for supported SQL.
- ClickBench performance remains within the agreed gates or trade-offs are reviewed.

## Phase 5: General Parquet Import

Goal: import arbitrary flat Parquet tables.

First version constraints:

- Flat schema only.
- No nested/list/map types.
- INT16/INT32/INT64, Date, Timestamp, and BYTE_ARRAY strings.
- Dictionary/plain encoding and Snappy.
- Nullable support can start minimal or explicitly reject unsupported encodings.

CLI target:

```sh
zighouse import-parquet <file.parquet> <store_dir> <table_name>
```

Tasks:

- Infer schema from Parquet metadata.
- Use import-time sampling to choose physical encoding.
- Materialize fixed-width numeric columns eagerly.
- Dictionary-encode low-cardinality strings.
- Use lazy/blob/hash policy for high-cardinality strings.
- Write generic table manifest.

Acceptance:

- Import ClickBench `hits.parquet` as `hits` through the generic path.
- Import small flat Parquet fixtures.
- Supported SQL subset works on generic imports.

## Phase 6: Encoding Policy

Goal: replace hardcoded storage hints with deterministic policy.

Initial policy:

- Fixed-width numeric: eager fixed column.
- Low-cardinality string: dictionary id plus string table.
- Medium-cardinality string: dictionary or blob based on sample.
- High-cardinality string: lazy/blob/hash-only.
- Segment stats: min, max, null count, row count.

Tasks:

- Add import sampling.
- Record physical encoding policy in manifest.
- Pick operators based on physical encoding.
- Allow ClickBench-specific profile overrides.

Acceptance:

- Policy is deterministic for the same input.
- Store size does not unexpectedly grow.
- Performance compare passes.

## Phase 7: Correctness and Differential Testing

Goal: build safety nets for generalization.

Test types:

- Parser snapshot tests.
- Planner tests.
- Operator unit tests.
- Parquet fixture tests.
- DuckDB differential tests.
- ClickBench integration tests.
- Performance regression tests.

Acceptance:

- `zig build test` covers parser/planner/operator basics.
- Differential tests can run independently.
- Performance baseline remains part of the development loop.

## Version Plan

- `v0.7`: ClickBench-specific logic isolation.
- `v0.8`: General schema/catalog/manifest.
- `v0.9`: Initial reusable physical operators.
- `v0.10`: SQL AST and planner subset.
- `v0.11`: Generic `import-parquet`.
- `v0.12`: DuckDB differential testing and wider generic coverage.

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

6. If performance fails, determine whether it is noise, a bug, or an intentional generality trade-off. Do not merge accidental regressions.

## Immediate Next Task

Start Phase 1 with the lowest-risk extraction: isolate ClickBench query dispatch helpers from `native.zig` while preserving the exact execution order and behavior.
