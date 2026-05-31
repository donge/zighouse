# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
# Standard build (with DuckDB SQL parser)
zig build

# Build without DuckDB (uses native Zig parser)
zig build -Dduckdb=false

# Release build for Linux
zig build -Dduckdb=false -Dtarget=x86_64-linux-gnu -Doptimize=ReleaseFast -Dstrip=true

# Run all tests
zig build test -Dduckdb=false

# Run the binary
./zig-out/bin/zighouse schema
./zig-out/bin/zighouse queries
```

**Important:** Default optimize mode is `ReleaseFast` (not Debug) because benchmarks measure 100M-row loops. Use `-Doptimize=Debug` to enable bounds/overflow checks.

Key build options:
- `-Dduckdb=false` — disable DuckDB SQL parser, use native Zig parser
- `-Dduckdb-prefix=<path>` — DuckDB installation prefix (default: `/opt/homebrew/opt/duckdb`)
- `-Dstatic-libs=true` — link lz4/zstd statically (use for release builds)

## Environment Variables

- `ZIGHOUSE_QUERY_PATH`: `specialized` | `generic` | `compare` (default: `specialized`)
- `ZIGHOUSE_IMPORT_TRACE`: print import phase timings
- `ZIGHOUSE_CLICKBENCH_SUBMIT`: enable ClickBench submission format

## Architecture

ZigHouse is a schema-driven columnar OLAP engine. It ingests Parquet/ClickHouse Native blocks into a MergeTree-compatible on-disk format and executes analytical SQL.

### Three Execution Stages

```
Input (Parquet / TCP Native blocks)
    ↓
Stage 1: Ingest (loader.zig, ingest/)
    — decode wire format, encode columns, write MergeTree parts
    ↓
Stage 2: Plan + Execute (generic_sql.zig, core/, generic_executor.zig)
    — parse SQL → build plan → two-dimensional dispatch
    ↓
Stage 3: Format (serializer.zig)
    — emit CSV or ClickHouse Native Block
```

### Two Query Paths

**Specialized path** (`native.zig`, `clickbench/`): Hand-tuned executors for the 43 ClickBench queries. ~588KB file of per-query shape-specific loops.

**Generic path** (`generic_executor.zig`): Stream-based executor for arbitrary analytical SQL. Uses DuckDB-backed or native Zig SQL parsing.

Switch between paths at runtime via `ZIGHOUSE_QUERY_PATH`. Use `compare` to run both and diff results.

### Two-Dimensional Dispatch

Query execution dispatches on both **shape** (11 patterns: `scalar_aggregate`, `lowcard_count_top`, `fixed_count_top`, etc.) and **capability** (column encoding: `fixed_i32`, `lowcard_text`, `lazy_text`, `hash_text`). Shape is inferred by `exec/planner.zig` from the parsed plan; capability comes from the physical schema.

### Named Module Imports

Build uses Zig named modules — import via module name, not relative paths:
```zig
const schema = @import("schema");
const ch_part = @import("ch_part");
```
`build.zig` (952 lines) wires the full dependency graph of 40+ modules.

### Key Modules

| Module | Role |
|--------|------|
| `schema.zig` | Type system: `ColumnType`, `PhysicalColumn`, `CapabilityTag`, `Table` |
| `catalog.zig` | Table registry and manifest persistence |
| `generic_sql.zig` | SQL AST and plan structs |
| `core/exec/planner.zig` | Generic plan → `PhysicalNode` IR |
| `core/exec/pipeline.zig` | Pipeline-parallel hash table aggregation |
| `core/exec/kernels.zig` | Scalar filter/projection/arithmetic ops |
| `generic_executor.zig` | Streaming generic SQL executor |
| `native.zig` | Specialized ClickBench executors |
| `parquet.zig` | Parquet format reader/writer |
| `loader.zig` | Parquet → MergeTree import pipeline |
| `ingest/tcp_server.zig` | ClickHouse TCP protocol server |
| `ingest/part_writer_session.zig` | Builds MergeTree parts from decoded rows |
| `clickhouse_format/part.zig` | MergeTree part directory I/O |
| `clickhouse_format/block.zig` | ClickHouse Native Block codec (LZ4/Zstd) |
| `compactor.zig` | Background part merging |
| `sql/` | Native Zig SQL parser (tokenizer, parser, plan_builder, ast) |
| `simd.zig` | AVX2 vectorized comparisons |
| `hashmap.zig` | Specialized hash tables (`HashU64Count`, etc.) |
| `parallel.zig` | Work-stealing parallel iteration |
| `agg.zig` | Aggregation primitives (sum, avg, min, max, percentiles) |

### Schema Binding

At query time, `bindColumn(native, name)` resolves a column name to a `BoundColumn` union holding mmap slice references. The `CapabilityTag` is derived from the physical form, driving operator dispatch. Columns carry no ownership — they reference memory-mapped storage directly.

### MergeTree Storage Layout

Each part directory contains per-column files: `<col>.bin` (encoded data), `<col>.dict` (dictionary for low-cardinality), plus metadata (`columns.txt`, `count.txt`, `marks`). The compactor periodically merges small parts and applies materialized views.

### Vendored C Libraries

`vendor/` contains lz4 and zstd compiled as static libraries via `build.zig`. No external package manager.
