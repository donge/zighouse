# ZigHouse

A schema-driven columnar OLAP engine written in Zig. Ingests Parquet and ClickHouse
Native blocks into a MergeTree-compatible on-disk format and executes analytical SQL
via an IR-pipeline executor.

## Install

### macOS (Homebrew)

```bash
brew tap donge/zighouse
brew install zighouse
brew services start zighouse
```

### Linux / Docker

```bash
docker pull ghcr.io/donge/zighouse:latest
```

Binary releases are available on the [releases page](https://github.com/donge/zighouse/releases).

### From source

Requires [Zig 0.17+](https://ziglang.org/download/).

```sh
zig build -Doptimize=ReleaseFast -Dstrip=true -Dstatic-libs=true
```

No external parser dependency. All 43 ClickBench queries are handled by the native
Zig SQL parser.

## Quick start

```sh
# Import a Parquet file
./zighouse import hits.parquet --table=hits

# Start the HTTP server
./zighouse serve /var/lib/zighouse

# Run all 43 ClickBench queries
./zighouse bench --store=./store --query=9
```

## ClickBench

ZigHouse participates in [ClickBench](https://benchmark.clickhouse.com/).
Submission files: [`clickbench-submit/zighouse/`](clickbench-submit/zighouse/).

```sh
bash clickbench-submit/zighouse/install   # download binary
bash clickbench-submit/zighouse/run.sh    # run benchmark
```

### Current results — Apple M2 Pro, 10M rows

Warm-best times from the official ClickBench driver (`./query` process per try,
first try discarded; `BENCH_TRIES=3`):

| Q | ms | Q | ms | Q | ms | Q | ms |
|---|---:|---|---:|---|---:|---|---:|
| Q1 | 4.0 | Q12 | 23 | Q23 | 67 | Q34 | 169 |
| Q2 | 6.0 | Q13 | 38 | Q24 | 132 | Q35 | 170 |
| Q3 | 8.0 | Q14 | 91 | Q25 | 20 | Q36 | 45 |
| Q4 | 11 | Q15 | 42 | Q26 | 16 | Q37 | 28 |
| Q5 | 34 | Q16 | 54 | Q27 | 20 | Q38 | 26 |
| Q6 | 48 | Q17 | 99 | Q28 | 37 | Q39 | 22 |
| Q7 | 4.0 | Q18 | 30 | Q29 | 173 | Q40 | 61 |
| Q8 | 8.0 | Q19 | 331 | Q30 | 7.0 | Q41 | 61 |
| Q9 | 73 | Q20 | 11 | Q31 | 76 | Q42 | 53 |
| Q10 | 96 | Q21 | 79 | Q32 | 62 | Q43 | 28 |
| Q11 | 21 | Q22 | 49 | Q33 | 274 | | |

**Warm total: 2.71 s**, 0 null results.

Against DuckDB 2.0 on the same 10M dataset:

- Query sum: **0.76×** DuckDB (ZigHouse 1.73 s vs DuckDB 2.27 s without process startup)
- Per-query gate: **≤3×** DuckDB on every query
- 3M gate: **32/32** deterministic query results match

`zighouse query` and `zighouse bench` now auto-detect both Store V2 parts and
streamed ClickHouse MergeTree parts, so file imports and INSERT streams run
through the same planner and pipeline. See
[`scripts/compact-part-query-test.sh`](scripts/compact-part-query-test.sh).
## Tests

```sh
zig build test
```

## Architecture

ZigHouse uses a single execution path:

```
Parquet / ClickHouse TCP Native blocks
    ↓  loader.zig / ingest/
    ↓  MergeTree-compatible column files (.bin, .str.bin)
    ↓
SQL parse  →  IR plan  →  pipeline executor
    ↓           ↓               ↓
sql/parser  planner.zig   pipeline.zig
                             (hash agg, TopK, scan, filter)
    ↓
serializer.zig  →  CSV / ClickHouse Native Block
```

Key modules: `schema.zig`, `catalog.zig`, `generic_sql.zig`, `core/exec/pipeline.zig`,
`core/exec/planner.zig`, `loader.zig`, `ingest/tcp_server.zig`.

## License

MIT — see [LICENSE](LICENSE).
