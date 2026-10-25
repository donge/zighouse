# ZigHouse Architecture

Status: working notes for the single-node engine. This file records the
architectural constraints we intentionally keep. `RowList` has been removed
from executor boundaries: breakers exchange chunks and only use bounded
`RowBuffer`s internally.

## Goals

- Single-node only. No distributed execution, no shards, no replication.
- Use local SSD + RAM aggressively: mmap column files, morsel-parallel scans,
  block cache, and spill-to-disk for large breakers.
- ClickHouse-compatible SQL/wire/storage where it matters.
- DuckDB-like simplicity in planning: a small fixed operator set and a few
  orthogonal rewrite passes. No fully automatic plan search.
- Chunked, push-based execution from scan to result. Avoid materialising an
  entire input before computing.

## Non-goals

- Cost-based join enumeration / adaptive operator selection.
- An exhaustive collection of shape-specific executors. Keep specialised
  implementations only where they are clearly faster, behind one contract.
- Distributed query planning, coordination, or remote shuffle.

## Target plan shape

```text
scan
  -> filter
  -> project
  -> hash_agg / scalar_agg
  -> top_k / sort
  -> limit
  -> result
```

Joins are hash joins. Rewrites are deliberately few:

1. predicate pushdown
2. unused-column elimination
3. limit pushdown
4. `ORDER BY + LIMIT` -> `top_k`
5. statistics propagation for pruning and sizing

The physical planner chooses one implementation per operator from the key
shape, input statistics, and memory budget. It must not probe a list of
strategies at runtime.

## Execution contract

```text
Source       next() -> ChunkView?
Operator     push(ChunkView) -> ChunkView?      (filter/project)
Sink         push(ChunkView)
             combine(local, global)
             finalize() -> Source                (agg/sort/join build)
```

`ChunkView` is a `DataChunk` plus an optional `SelectionVector` and validity.
`DataChunk` stays at 2048 rows.

Nothing above a breaker exchanges `[]?Value` row lists. A breaker may keep
columnar state internally (hash table, sort runs, join build table), but its
input and output are chunk streams.

## Current state (important)

Already streaming:

- mmap/range reads and `fetchRange` / `nextChunk`
- scan -> filter -> project -> limit scannable path
- hash-aggregation input and output chunks (`executeHashAggChunked`, `ChunkSink`)
- parallel top-K (`TopKSink` / `TopKCollector`; legacy late-materialisation path
  still exists for wide `SELECT *`)
- inner/left hash-join probe side; output chunks
- generic Parquet import string offsets spill to temp files
- file imports and streamed MergeTree parts share `store_query.zig`; both use
  the same planner and pipeline, and query/bench/serve auto-detect either layout

Still materialising or unbounded:

- `order_by` without `LIMIT` uses an in-memory `RowBuffer`
- hash-join build side and right/full joins use `RowBuffer`
- subqueries/CTEs go through `ResultSet`
- final results are accumulated in `ResultSet` before serialization
- query mmap working set is bounded by the OS page cache, not an explicit
  per-query budget
- `importParquetCHHttp` still buffers the entire request

`RowBuffer` is now an internal breaker buffer, not the executor boundary.
Remaining work is spill/budgeting for unbounded operators and streaming the
final result.

## Compute architecture decision (DuckDB 2.0 / ClickHouse review)

Both reference engines select physical work by **data layout**, never by a
query-specific shape:

- DuckDB 2.0: one `PhysicalHashAggregate` feeding a radix-partitioned grouped
  hash table; perfect/partitioned aggregation are layout specialisations.
  Top-N is an independent sink with encoded sort keys, and full order uses
  sort runs plus merge.
- ClickHouse: an aggregating processor with type-specific hash maps and
  single/two-level aggregation selected by key layout and memory; Top-N is
  partial-sort + limit; full sort uses merge sorter runs.

Consequently ZigHouse keeps only these hash-aggregation layouts:

```text
auto / compact_int / pair_count / triple_count / string_key
```

DISTINCT, top-K, CASE keys, and grouped distinct are properties of the input
or output, not separate strategies. Top-N is a separate sink. Runtime fallback
is limited to a key-layout fallback, never a probe over query-shape executors.

Migration status:

- done: `TopNSink` owns the bounded heap for row/chunk producers; scannable
  top-K streams chunks through it; top-K comparator is shared.
- done: `LateTopNHeap` is shared by parallel phase-1 workers and the sequential
  wide-row late-materialisation path.
- next: make hash aggregation a local/global sink and remove fused agg+top-K
  paths.

## Refactor phases

### Phase 1: contract and adapters (done)

- `ChunkView`, `ChunkSink`, `ChunkResult`, and owned `ChunkStream` sources.
- `SourceIface` remains the scan/range source.
- Temporary row adapters are gone from the executor boundary.

### Phase 2: hash aggregation sink (done)

- Generic and specialised hash aggregations emit `DataChunk`s.
- `ORDER BY count DESC LIMIT k` keeps a bounded top-K and never emits all
  groups.
- Compact int-key, pair/triple-count, string-key and distinct layouts share
  the same sink contract.

### Phase 3: chunk operators above breakers (partial)

- `top_k`, `project`, `limit` consume/produce chunks.
- `order_by` still needs an external sort with fixed-size runs and a k-way
  merge; small sorts stay in memory.
- `top_k` remains a bounded heap; no full sort.

### Phase 4: joins (partial)

- Inner/left probe side is streamed and emits chunks.
- Build side still materialises; add `HashJoinBuildSink` (partitioned,
  spillable) and stream right/full output.

### Phase 5: subqueries/CTE

- `ChunkSource` wraps a child `Source`, not a materialised `ResultSet`.
- CTEs are either inlined or backed by a spillable materialised source.

### Phase 6: streaming result

- Server/CLI paths write Native/HTTP blocks as chunks are finalized.
- `ResultSet` remains for embedded/test APIs; it is no longer the internal
  query boundary.

## Storage and I/O choices

- Keep the ClickHouse-compatible part layout and codecs. Compression is a
  storage concern, not a reason to add execution branches.
- Prefer ZSTD/LZ4 per column based on width/cardinality; keep the existing
  low-cardinality dictionary path. Store V2 already writes 64K-row blocks
  with per-block LZ4/raw selection, checksums, and min/max stats.
- mmap for local files. No eager whole-file copy. Raw (uncompressed) blocks
  are exposed as zero-copy slices; whole-block LZ4 decodes straight into the
  destination, and large columns decode blocks across worker threads. Use
  range reads + morsels and sequential read-ahead.
- Block min/max stats answer `MIN`/`MAX` without decoding. Wiring per-block
  stats into filter pruning is the next I/O win.
- Spill hash aggregation, sort, and join build to local SSD with a global
  memory budget. Single-node means spill is the scale-out mechanism.

## What to take from each system

From DuckDB 2.0:

- simple bind/optimize/physical separation
- one vectorised chunk contract
- local -> combine -> finalize sinks
- top-N as a bounded sink
- straightforward rule-based pushdown

From ClickHouse:

- block/column compression and MergeTree parts
- streaming result blocks
- aggressive pruning and low-cardinality handling
- external aggregation/sort when memory is exhausted

Do not copy ClickHouse's large matrix of per-query-shape executors. Keep a
single generic path plus a small number of justified specialisations.

## Validation

Every phase must keep the ClickBench gate green:

```sh
zig build test
python3 scripts/bench-vs-duckdb.py \
  --zighouse ./zig-out/bin/zighouse \
  --store /tmp/zh3m \
  --parquet /tmp/hits_3m.parquet \
  --queries clickbench-submit/zighouse/queries.sql
```

Use a DuckDB 2.0 binary for the comparison:

```sh
python3 scripts/bench-vs-duckdb.py \
  --duckdb /path/to/duckdb-2.0 \
  --zighouse ./zig-out/bin/zighouse \
  --store /tmp/zh3m \
  --parquet /tmp/hits_3m.parquet \
  --queries clickbench-submit/zighouse/queries.sql
```

Current reference (Apple M2 Pro, 4 performance cores):

- 3M: ZigHouse 605.7ms vs DuckDB 2.0 849.0ms (0.713x), 32/32 results.
- 10M in-process: ZigHouse 1730.7ms vs DuckDB 2.0 2266.0ms (0.764x).
- 10M official driver: 2.71s warm-best (process per try, first discarded).
- Q7 `MIN/MAX(EventDate)`: 10M in ~1.3ms in-process (~4ms per process) from
  block min/max stats, without decoding the column.
- Q20 `UserID = literal`: 10M in ~1.1ms via SIMD raw equality.
- File-import vs streamed-write parity: `scripts/compact-part-query-test.sh`.

For each phase record:

- warm query sum
- peak RSS
- bytes read / granules skipped
- fallback counters
- spill bytes and run counts
