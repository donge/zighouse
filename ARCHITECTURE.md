# ZigHouse Architecture

Status: working notes for the single-node engine. This file records the
architectural constraints we intentionally keep, and the refactor needed to
remove `RowList` from breaker boundaries.

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
- hash-aggregation input loops (`executeHashAggChunked`)
- parallel top-K (`TopKCollector`, late materialisation)

Still materialising:

- `executeNode()` scan/filter/project produces a full `RowList`
- hash-aggregation output is emitted as `RowList` when there is no top-K
- `order_by` sorts a full `RowList`
- hash join builds both sides as `RowList`
- subqueries/CTEs go through `ResultSet -> cloneRowList`
- final results are accumulated in `ResultSet` before serialization

`RowList` is therefore still the universal breaker boundary. Removing it is
the main architectural task.

## Refactor phases

### Phase 1: contract and adapters

- Add `ChunkView` and `Source`/`Operator`/`Sink` in `src/core/exec/`.
- Add a `RowList -> Source` adapter so existing operators keep working during
  the transition. The adapter is temporary and must be identifiable in
  profiles.
- Keep `SourceIface` as the scan/range source.

### Phase 2: hash aggregation sink

- Make the generic hash aggregation emit `DataChunk`s directly through a
  `ChunkSink`; do not build a `RowList` of groups.
- Migrate specialised layouts (`CompactIntKeyHashTable`, `PairCountHashTable`,
  `TripleCountHashTable`, string-key tables) one at a time behind the same
  sink. The planner selects the layout once; no runtime probing.
- For `ORDER BY count DESC LIMIT k`, the sink keeps a bounded top-K and emits
  only K rows (already implemented for triple/string-key paths).

### Phase 3: chunk operators above breakers

- `top_k`, `sort`, `project`, `limit` consume/produce chunks.
- `order_by` becomes an external sort with fixed-size runs and a k-way merge;
  small sorts stay in memory.
- `top_k` remains a bounded heap; no full sort.

### Phase 4: joins

- Build side: `HashJoinBuildSink` (partitioned, spillable).
- Probe side: streamed chunks, emitting output chunks directly.
- No `executeNode(left)` / `executeNode(right)` row materialisation.

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
  low-cardinality dictionary path.
- mmap for local files. No eager whole-file copy. Use range reads + morsels;
  keep a block cache and read-ahead for cold scans.
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

For each phase record:

- warm query sum
- peak RSS
- bytes read / granules skipped
- fallback counters
- spill bytes and run counts
