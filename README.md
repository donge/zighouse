# ZigHouse

A schema-driven columnar OLAP engine written in Zig. Built for high-performance analytics on structured data, with a focus on ClickBench workloads.

## Architecture

ZigHouse provides two execution paths:

**Specialized path** — hand-tuned executors per query shape, operating on pre-imported columnar stores (hot columns). Optimized for the 43-query ClickBench suite.

**Generic path** — SQL parsed via DuckDB's `json_serialize_sql`, evaluated by a streaming Parquet executor. Supports a growing subset of analytical SQL without schema-specific pre-compilation.

The active path is controlled by `ZIGHOUSE_QUERY_PATH={specialized|generic|compare}`. The default is `specialized`.

See [`docs/architecture.md`](docs/architecture.md) and [`docs/generalization-roadmap.md`](docs/generalization-roadmap.md) for design details.

## Build

Requires [Zig 0.16](https://ziglang.org/download/).

```sh
# Local build (with DuckDB SQL parser)
zig build

# Portable build without DuckDB dependency
zig build -Dduckdb=false

# Linux x86-64 release binary (cross-compile)
zig build -Dduckdb=false -Dtarget=x86_64-linux -Doptimize=ReleaseFast -Dstrip=true
```

## ClickBench

ZigHouse participates in [ClickBench](https://benchmark.clickhouse.com/). Submission files are in [`clickbench-submit/zighouse/`](clickbench-submit/zighouse/).

```sh
# Download pre-built binary and run ClickBench
bash clickbench-submit/zighouse/install
bash clickbench-submit/zighouse/run.sh
```

Performance gates (warm best, vs previous release):
- Query sum: ≤ +5%
- Import total: ≤ +7.5%
- Any single query: ≤ +20% and ≤ +2ms

## Tests

```sh
zig build test -Dduckdb=false
```

## License

MIT — see [LICENSE](LICENSE).
