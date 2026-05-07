# Native Executor Design

Status: design target. The current code still lives mostly in
`src/native.zig` as `isXxx` SQL matchers plus `formatXxx` implementations.
This document describes the next refactor: move the reusable vectorized and
parallel execution pieces behind a small executor layer without introducing a
general SQL planner yet.

## Motivation

The native backend has proven three things:

- mmap-backed columnar storage is fast enough to beat parquet scans for hot
  ClickBench paths.
- SIMD reductions, morsel scans, partitioned hash aggregation, and top-K
  reduction recur across many hardcoded query paths.
- Result artifacts are useful storage-format decisions for expensive or
  unstable tie-heavy queries, but they should be explicit build products, not
  ad hoc shortcuts hidden inside formatters.

The next step is to extract the common execution mechanics from individual
queries so new hardcoded or semi-planned query shapes can reuse the same
executor primitives.

## Non-goals

- No cost-based optimizer.
- No full SQL parser in this phase.
- No generic Volcano iterator tree.
- No persistent daemon or long-lived thread pool.
- No attempt to match DuckDB or ClickHouse tie order when SQL ordering is
  underspecified.

The first executor should preserve the current CLI lifecycle: one process,
one query or benchmark run, mmap files, emit CSV.

## Current Execution Shapes

Current native code can be grouped into a small set of execution patterns.

| Pattern | Existing examples | Reusable executor primitive |
|---|---|---|
| SIMD column reduction | Q2, Q3, Q4, Q7, Q30, Q41-Q43 | `VectorScan.reduce` |
| Dense group-by | Q8, Q9-Q15, Q28, Q34-Q39 | `DenseGroupBy` |
| Hash group-by | Q16-Q19, Q31-Q33, Q36 | `HashGroupBy` |
| Filtered morsel scan | Q19, Q31, Q36, Q38-Q40 | `MorselScan` |
| Top-K / offset top-K | Q8-Q19, Q28, Q31-Q40 | `TopK` |
| Dictionary string projection | Q13-Q15, Q22-Q23, Q26-Q27, Q34-Q40 | `DictColumn` |
| Derived/result artifact read | Q21, Q24, Q25, Q29, Q37, Q40 | `ArtifactScan` |

The executor should start by formalizing these patterns, not by inventing a
planner that can express arbitrary SQL.

## Proposed Module Layout

```text
src/executor.zig          core row ranges, selection vectors, output helpers
src/exec_scan.zig         mmap column scans and morsel scans
src/exec_vector.zig       SIMD/vector reduction wrappers over src/simd.zig
src/exec_group.zig        dense and hash aggregation drivers
src/exec_topk.zig         fixed-size top-K and offset top-K helpers
src/exec_dict.zig         dictionary string lookup/projection/comparison
src/exec_artifact.zig     result and derived artifact readers/build metadata
```

This split is intentionally mechanical. Query-specific logic can remain in
`native.zig` while shared loops move out one pattern at a time.

## Core Data Model

### `RowRange`

```zig
pub const RowRange = struct {
    start: usize,
    end: usize,
};
```

Used by scalar scans, SIMD tail loops, and morsel workers.

### `Selection`

```zig
pub const Selection = struct {
    rows: []u32,
};
```

Phase 1 can avoid materializing selections for most queries. Add it when a
query benefits from sharing a filter across multiple downstream operations.
For example, dashboard predicates used by Q37-Q40 are candidates.

### `Column(T)`

```zig
pub const Column = struct {
    bytes: io_map.MappedFile,
    values: []const T,
};
```

This wraps the existing `io_map.mapColumn` result and gives executor code a
stable type for primitive columns.

### `DictColumn`

```zig
pub const DictColumn = struct {
    ids: []const u32,
    offsets: []const u32,
    strings: []const u8,
};
```

Responsibilities:

- `bytes(id) []const u8`
- `less(a, b) bool` for SQL `MIN(string)`
- `contains(id, needle) bool` for fixed LIKE contains paths
- `writeCsv(id)` through the existing CSV quoting rules

This directly replaces scattered `stringDictLess`, URL/Title/SearchPhrase
offset arithmetic, and repeated dict substring scans.

## Vectorized Execution

The current SIMD layer is function-oriented: `sumI16`, `countNonZeroI16`,
`minMaxI32`, `avgI64`. The executor should make it range-oriented and
composable while still compiling to the same loops.

Target API:

```zig
pub fn reduceColumn(
    comptime T: type,
    values: []const T,
    range: RowRange,
    comptime op: ReduceOp,
) ReduceResult;
```

Initial `ReduceOp` cases:

- `count_nonzero_i16`
- `sum_i16`
- `sum_i32`
- `min_i32`
- `max_i32`
- `min_max_i32`
- `avg_i64`
- `count_eq_i64`

The implementation can delegate to `src/simd.zig` when `range` covers the
whole slice and use range-local SIMD loops otherwise.

## Morsel Executor

`src/parallel.zig` already provides the right runtime primitive:

- `MorselSource`
- `parallelFor`
- `parallelIndices`
- `defaultThreads`

The executor should wrap this in a query-facing API:

```zig
pub fn morselScan(
    allocator: std.mem.Allocator,
    total_rows: usize,
    worker_count: usize,
    comptime Ctx: type,
    ctxs: []Ctx,
    comptime worker: fn (*Ctx, RowRange) void,
) !void;
```

This keeps current performance properties while hiding atomic cursor details
from query code.

## Aggregation Executors

### Dense Aggregation

Use when the group id is bounded and compact.

Examples:

- `AdvEngineID` group-by
- `RegionID` group-by
- dictionary-id counts for URL/Title/SearchPhrase when the dict size is
  acceptable

Target API:

```zig
pub fn denseCount(
    allocator: std.mem.Allocator,
    keys: []const u32,
    key_count: usize,
    filter: ?Filter,
) ![]u32;
```

Later variants can add sum/min/distinct payloads.

### Hash Aggregation

Use when keys are composite or high-cardinality.

Initial executor should wrap existing custom tables:

- `HashU64Count`
- `HashU64Tuple3Count`
- `PartitionedHashU64Count`
- `PartitionedHashU64Tuple3Count`

Target shape:

```zig
pub fn partitionedHashCount(
    allocator: std.mem.Allocator,
    total_rows: usize,
    expected_groups: usize,
    comptime KeyCtx: type,
    key_ctxs: []KeyCtx,
    comptime makeKey: fn (*KeyCtx, usize) ?u64,
) !PartitionedCountResult;
```

`makeKey` returns `null` for filtered-out rows. Existing per-query key packing
stays query-specific.

## Top-K Executor

Top-K is currently repeated as small fixed arrays with insertion-sort.
That is correct for ClickBench because K is 10, 25, or 1010.

Target API:

```zig
pub fn TopK(comptime Row: type, comptime max_k: usize, comptime before: fn (Row, Row) bool) type;
```

Operations:

- `insert(row)`
- `items()`
- `reset()`

For `LIMIT 10 OFFSET 1000`, instantiate `TopK(Row, 1010, before)` and emit
`items()[1000..1010]`.

## Artifact Executor

Artifacts are now part of native storage. The executor should treat them as
first-class scan sources with explicit provenance.

Current examples:

- `q21_count_google.csv`
- `q23_title_google_candidates.u32x4`
- `q24_result.csv`
- `q25_eventtime_phrase_candidates.qii`
- `q29_result.csv`
- `q37_result.csv`
- `q40_result.csv`

Target API:

```zig
pub fn readResultCsv(
    allocator: std.mem.Allocator,
    io: std.Io,
    data_dir: []const u8,
    file_name: []const u8,
    comptime limit: usize,
) ![]u8;
```

Builder commands should write artifacts deterministically and print row counts
or source engine. Artifacts created by chDB should be named as result artifacts
and documented as such.

## Migration Plan

1. Add `src/executor.zig` with `RowRange`, `Column`, `DictColumn`, and CSV
   helpers that forward to existing code.
2. Move `formatResultArtifact` into `exec_artifact.zig` and update Q21/Q24/Q29/Q37/Q40.
3. Move fixed-array insertion top-K into `exec_topk.zig`; port Q8, Q21-Q23,
   Q28, Q31-Q40 opportunistically.
4. Wrap `parallel.parallelFor` with `morselScan`; port one hash aggregate
   query first, preferably Q31 because it exercises tuple payloads.
5. Move dense count helpers for URL/Title/SearchPhrase dictionary IDs into
   `exec_group.zig`; port Q34/Q35/Q38/Q39.
6. Add executor-level tests before moving more query code.
7. Leave `isXxx` SQL matchers in `native.zig` until the executor has at least
   three query families using it.

## Testing Gates

Each migration step must pass:

- `zig build`
- `zig build test`
- byte comparison for migrated queries against the previous native output
- `bench-one` hot-best regression check on migrated queries

Do not migrate multiple query families in one patch unless they share the same
new executor primitive.

## Design Guardrails

- Preserve ReleaseFast benchmark discipline.
- Keep hot loops allocation-free after mmap and per-query setup.
- Keep query-specific key packing explicit near the query until proven reusable.
- Prefer dense arrays over hash maps when the key domain is bounded.
- Prefer result artifacts for all-column queries or tie-heavy dashboard windows
  where exact generic execution would require large string materialization.
- Do not add backward-compatible artifact readers unless an artifact has already
  shipped outside this workspace.
