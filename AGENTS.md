# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

## Build Commands

```bash
# Standard build
zig build

# Release build for Linux x86-64
zig build -Dtarget=x86_64-linux-gnu -Doptimize=ReleaseFast -Dstrip=true -Dstatic-libs=true

# Run all tests
zig build test

# Run the binary
./zig-out/bin/zighouse import-parquet --format=generic hits.parquet ./store hits
./zig-out/bin/zighouse bench ./store hits clickbench-submit/zighouse/queries.sql
```

**Important:** Default optimize mode is `ReleaseFast` (not Debug) because benchmarks measure 100M-row loops. Use `-Doptimize=Debug` to enable bounds/overflow checks.

Key build options:
- `-Dstatic-libs=true` — link lz4/zstd statically (use for release builds)
- `-Dlz4-prefix=<path>` / `-Dzstd-prefix=<path>` — library search prefix overrides
- `-Dstrip=true` — strip debug symbols from output binary
- `-Dbench-tools=true` — build benchmark helper binaries

## Environment Variables

- `ZIGHOUSE_IMPORT_TRACE`: print import phase timings
- `ZIGHOUSE_CLICKBENCH_SUBMIT`: enable ClickBench submission format (JSON timing rows)

## Architecture

ZigHouse is a schema-driven columnar OLAP engine. It ingests Parquet/ClickHouse Native blocks into a MergeTree-compatible on-disk format and executes analytical SQL via an IR-pipeline executor.

### Three Execution Stages

```
Input (Parquet / TCP Native blocks)
    ↓
Stage 1: Ingest (loader.zig, ingest/)
    — decode wire format, encode columns, write MergeTree parts
    ↓
Stage 2: Plan + Execute (generic_sql.zig, core/, pipeline.zig)
    — parse SQL → IR plan → pipeline executor
    ↓
Stage 3: Format (serializer.zig)
    — emit CSV or ClickHouse Native Block
```

### Single Execution Path

All queries — including the 43 ClickBench queries — run through the same IR pipeline:

```
generic_sql.parse()        native Zig SQL parser (sql/)
    ↓
ir_planner.plan_query()    SQL plan → PhysicalNode IR  (core/exec/planner.zig)
    ↓
pipeline.executePlan()     parallel hash agg, TopK, scan/filter  (core/exec/pipeline.zig)
```

Query execution dispatches on **plan shape** (11 patterns: `scalar_aggregate`,
`lowcard_count_top`, `fixed_count_top`, etc.) inferred by the planner from the
parsed SQL.

### Named Module Imports

Build uses Zig named modules — import via module name, not relative paths:
```zig
const schema = @import("schema");
const ch_part = @import("ch_part");
```
`build.zig` wires the full dependency graph of 40+ modules.

### Key Modules

| Module | Role |
|--------|------|
| `schema.zig` | Type system: `ColumnType`, `PhysicalColumn`, `Table` |
| `catalog.zig` | Table registry and manifest persistence |
| `generic_sql.zig` | SQL AST and plan structs |
| `core/exec/planner.zig` | SQL plan → `PhysicalNode` IR |
| `core/exec/pipeline.zig` | Pipeline-parallel hash table aggregation |
| `core/exec/kernels.zig` | Scalar filter/projection/arithmetic ops |
| `core/exec/hash_table.zig` | Specialized hash tables for aggregation |
| `core/simd_ops.zig` | SIMD aggregation and filter primitives |
| `core/simd_batch.zig` | SIMD comparison and mask operations |
| `parquet.zig` | Parquet format reader/writer |
| `loader.zig` | Parquet → MergeTree import pipeline |
| `ingest/tcp_server.zig` | ClickHouse TCP protocol server |
| `ingest/part_writer_session.zig` | Builds MergeTree parts from decoded rows |
| `clickhouse_format/part.zig` | MergeTree part directory I/O |
| `clickhouse_format/block.zig` | ClickHouse Native Block codec (LZ4/Zstd) |
| `compactor.zig` | Background part merging |
| `sql/` | Native Zig SQL parser (tokenizer, parser, plan_builder, ast) |
| `hashmap.zig` | `DistinctEpochSet` for COUNT DISTINCT |
| `parallel.zig` | Work-stealing parallel iteration |
| `agg.zig` | Aggregation primitives (sum, avg, min, max, percentiles) |

### Storage Layout

Generic store format (used by the IR pipeline):
- `<col>.bin` — raw LE fixed-width column data
- `<col>.str.bin` — `[u64 row_count | (N+1)×u64 offsets | bytes]` for string columns
- `columns.txt`, `count.txt` — table metadata

MergeTree format (for ClickHouse wire protocol ingest/export):
- `<col>.bin` — raw bytes, `<col>.size.bin` — per-row u64 lengths
- `data.bin` + `<col>.cmrk2` — compact format with granule marks

### Schema Binding

At query time, `bindColumn(native, name)` resolves a column name to a `BoundColumn`
union holding mmap slice references. Columns carry no ownership — they reference
memory-mapped storage directly.

### Vendored C Libraries

`vendor/` contains lz4 and zstd compiled as static libraries via `build.zig`.
No external package manager.

## SQL Standard Conformance (sqltest)

Run SQL:2016 conformance tests (from [ClickHouse/sqltest](https://github.com/ClickHouse/sqltest)):

```bash
pip3 install pyyaml requests
bash scripts/run-sqltest.sh
```

Overall: **716 / 1464 (48.9%)** mandatory features pass.

| Category | Pass Rate | Notes |
|----------|-----------|-------|
| **E011** Numeric | 85/112 (76%) | All standard aliases (INT, INTEGER, SMALLINT, BIGINT, FLOAT, REAL, DOUBLE, DECIMAL, NUMERIC) mapped; DECIMAL(p,s) → float64 (precision lost) |
| **E021** Strings | 33/58 (57%) | VARCHAR, CHAR, CHARACTER, TEXT, CLOB, BLOB → text; CHARACTER VARYING, VARCHAR(n) → text |
| **E031** Identifiers | 3/3 (100%) | All pass after standard type support |
| **E051** Names | 53/53 (100%) | All pass |
| **F051** Date/Time | 42/42 (100%) | All pass after TIME/TIMESTAMP WITH TIME ZONE handling |
| **E111** CAST | 2/2 (100%) | All pass |
| **F471** Scalar subquery | 1/1 (100%) | All pass |
| **F481** NULL predicates | 2/2 (100%) | All pass |
| **E091** Set functions | 11/16 (69%) | Most aggregate functions pass |
| **E061** Predicates | 40/81 (49%) | IN, BETWEEN, LIKE, comparison predicates work |
| **E071** Row value exprs | 10/15 (67%) | Row value comparisons pass |
| **E141** Column constraints | 13/83 (16%) | DEFAULT clause works; NOT NULL, UNIQUE, FK fail |
| **F031** DDL | 7/102 (7%) | CREATE TABLE, DROP TABLE work; CREATE SCHEMA, VIEW, ROLE fail |
| **F041** Referential constraints | 10/31 (32%) | Basic FK syntax works |
| **F131** Array | 32/33 (97%) | Array type support |
| **F081** UNION | 3/3 (100%) | UNION ALL works |
| **F221** DEFAULT | 1/2 (50%) | |
| **E031-E161, F031-F481** | 0% | All require DDL with standard types — now substantially improved |

**Root cause of remaining failures**: DDL parser (`ingest/ddl_parser.zig`) needs `CREATE SCHEMA`, `CREATE VIEW`, `CREATE TYPE`, cursors, and transaction support (Phase 2).
