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

Requires [Zig 0.14+](https://ziglang.org/download/).

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

### v1.0.0 results — Apple M2 Pro, 10M rows

Warm-best times (round 2 of 3, first round discarded as cold-cache):

| Q | ms | Q | ms | Q | ms | Q | ms |
|---|---:|---|---:|---|---:|---|---:|
| Q1 | 0.03 | Q12 | 23 | Q23 | 46 | Q34 | 304 |
| Q2 | 1.2 | Q13 | 78 | Q24 | 104 | Q35 | 317 |
| Q3 | 5.6 | Q14 | 103 | Q25 | 3.4 | Q36 | 65 |
| Q4 | 1.7 | Q15 | 91 | Q26 | 7.2 | Q37 | 37 |
| Q5 | 42 | Q16 | 58 | Q27 | 8.3 | Q38 | 9.9 |
| Q6 | 41 | Q17 | 130 | Q28 | 20 | Q39 | 5.4 |
| Q7 | 2.7 | Q18 | 119 | Q29 | 139 | Q40 | 68 |
| Q8 | 2.3 | Q19 | 342 | Q30 | 0.2 | Q41 | 5.4 |
| Q9 | 60 | Q20 | 2.1 | Q31 | 54 | Q42 | 4.2 |
| Q10 | 89 | Q21 | 98 | Q32 | 36 | Q43 | 9.7 |
| Q11 | 16 | Q22 | 37 | Q33 | 243 | | |

**Warm total: 2.83 s** across all 43 queries, 0 null results.

Gates (warm-best vs DuckDB, 100M rows):
- Query sum: **1.24×** DuckDB ✅ (gate ≤ 1.5×)
- Max single query: **2.68×** (Q15) ✅ (gate ≤ 3×)
- Correctness: **35/35** deterministic queries match DuckDB ✅

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
