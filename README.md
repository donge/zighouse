# ZigHouse

ZigHouse is an experimental analytical database binary written in Zig for running ClickBench-style workloads.

It uses a native column-oriented storage format built from the ClickBench Parquet dataset, with vectorized execution and SIMD-oriented kernels for scan, filter, aggregation, and top-k query patterns.

This repository is used to publish reproducible benchmark binaries. Source code is not included in this repository.

## ClickBench

The ClickBench binary is built without DuckDB support and runs as a standalone Linux x86_64 executable.

The benchmark imports `hits.parquet` into a local ZigHouse store, then executes the 43 ClickBench queries through the native engine.
