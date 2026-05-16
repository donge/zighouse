# ZigHouse Development Log

---

## 2026-05-16 — CH-compatible wide part: import perf fix + primary.idx bug fix + regression baseline

### Context

This entry covers work from the PR-CH1 through PR-CH9 series plus follow-up stabilization.
The goal is a ClickHouse MergeTree-compatible `wide part` writer so that ZigHouse-produced
parts can be `ATTACH`ed directly into a running CH 26.3 instance.

---

### What Was Done

#### PR-CH1~CH9: end-to-end wide part writer

- `src/clickhouse_format/part.zig`: full `ColumnWriter` + `Part` implementation
  - Writes `.bin` (LZ4-compressed), `.cmrk2` (LZ4-compressed marks), `primary.idx`,
    `columns.txt`, `count.txt`, `checksums.txt`, `serialization.json`, `metadata_version.txt`
  - Phase-1 types: `Int16`, `Int32`, `Int64`, `Date`, `DateTime`, `String`
  - Granule size: 8192 rows (fixed, non-adaptive)
  - String columns use CH's split sub-stream layout: `{col}.bin` / `{col}.size.bin` + matching `.cmrk2`
- `src/loader.zig`: `importParquetCH(pk_col_name)` wires Parquet → `Part`
- `src/main.zig`: `import-parquet --format=ch --pk=<col>` CLI
- `scripts/interop-test.sh`: end-to-end fixture test against live CH 26.3 container

#### P0 bug: primary.idx wrong column

**Root cause**: `pk_col_idx` was hardcoded to `0`, so `primary.idx` always stored column 0
(`WatchID`, i64). But the table was `ORDER BY CounterID` (i32 at index 1). CH's granule
skipping misparse caused `CANNOT_READ_ALL_DATA` on any `WHERE` query.

**Fix** (`Part.open` now takes `pk_col_name: ?[]const u8`):
- `null` → default col 0, backward-compatible
- explicit name → linear scan of schema, error `PkColumnNotFound` if missing
- `--pk=WatchID` now writes `WatchID` (i64) values to `primary.idx` and the table is
  `ORDER BY WatchID`, so types match and WHERE queries work

#### Import performance fix: eliminate `.unc.tmp` disk double-write

**Before**: every value was written to both the compressed `.bin` and an uncompressed `.unc.tmp`
temp file (needed for CityHash128 checksums). Disk I/O dominated: 1M row import took ~188s.

**After**: uncompressed bytes are buffered in `unc_buf: std.ArrayList(u8)` (RAM). At
`Part.finish()` each column's `unc_buf` is hashed then freed before moving to the next column,
so peak RAM = size of largest single column (not all columns simultaneously).

| Dataset | Before | After | Speedup |
|---|---|---|---|
| 1M rows (110 MB parquet) | ~188s | ~2.9s | 65× |
| 10M rows (1.5 GB parquet) | ~30min est. | ~28s | ~65× |

**Memory cost**: `unc_buf` holds one full column at a time. For 1M rows: ~300 MB peak.
For 10M rows: ~3 GB peak. See "Known Limitations / Risks" below.

#### Interop validation (CH 26.3.2.3 container `sw_asdb`)

- `hits_zh_1m`: ZigHouse-produced part, 1M rows, `ORDER BY WatchID`, ATTACH'd to CH
  - `count() = 1,000,000` ✓
  - `WHERE WatchID > 0`, `WHERE CounterID >= 0`, `WHERE Title != ''`, mixed aggregation ✓
- Disk comparison vs CH-native import (`hits_ch_1m`, OPTIMIZE FINAL → `all_1_4_1`):
  - Data: 128.2 MB (CH) vs 132.8 MB (ZH) → +3.6%
  - Marks: 58.6 KB (CH) vs 136 KB (ZH) → ZH 2.3× larger (no Sparse encoding)

#### ClickBench regression (2026-05-16)

| Metric | Baseline | This run | Delta |
|---|---|---|---|
| warm_best_sum (43q) | 5.810s | 5.814s | +0.07% ✓ |
| Correctness compare | PASS | PASS | — |

Gate: warm_best ≤5% regression. **PASS**.

---

### Known Limitations / Risks

| Issue | Severity | Mitigation plan |
|---|---|---|
| `unc_buf` memory: 10M rows ~3 GB peak | Medium | Block-level `.unc.tmp` fallback (Plan B, see below) |
| No Sparse encoding: some columns 100×+ vs CH | Low | Sparse encoding (Phase 2) |
| Single part only (`all_1_1_0`) | Low | Merge support not needed for sidecar use case |
| `primary.idx` single-column only | Low | Composite ORDER BY is Phase 2 |
| LZ4 only (no ZSTD) | Low | LZ4 matches CH default; ZSTD optional later |

---

### Design: three product lines (clarified)

1. **ClickBench hot path** (`import-clickbench-parquet-hot`): hand-tuned, schema-specific,
   never touched by generic work. Performance gate: warm_best ≤5%.

2. **ZigHouse generic Parquet DB** (`import-parquet --format=zg`): schema-driven OLAP engine,
   long-term DuckDB alternative target.

3. **CH-compatible sidecar** (`import-parquet --format=ch --pk=<col>`): produces MergeTree wide
   parts, ATTACHable to a live CH instance. Current scope: Phase-1 types, single part, single
   ORDER BY column.

---

### Plan B: block-level unc_buf memory cap (next step)

**Problem**: `unc_buf` holding a full column unbounded. 10M rows URL column ≈ 200MB,
but 10M rows all-columns ≈ 3 GB total, column-by-column. 100M rows → ~30 GB.

**Plan B design**:
- Keep `unc_buf` for values within a single LZ4 block (≤1 MiB uncompressed, already bounded).
- After each `flushBlock()`, append that block's uncompressed bytes to a per-column `.unc.tmp`
  temp file (sequential write, no random access).
- At `Part.finish()`: open each `.unc.tmp`, read sequentially through CityHash128 update,
  delete the file.
- **CityHash128 challenge**: the current `cityhash102` binding does not expose an incremental
  interface. Options:
  - (a) Implement incremental CityHash128 in `src/clickhouse_format/cityhash.zig` (complex).
  - (b) mmap the `.unc.tmp` file and pass the full slice to the existing one-shot hash
    (same API, avoids incremental complexity; mmap is OS-level lazy, no extra RAM).
  - **Recommended: option (b)** — minimal code change, mmap is zero-copy on macOS/Linux.
- Memory impact: `unc_buf` (scratch buffer, ≤1 MiB) + mmap window (OS-managed). Constant RAM.
- Disk impact: sequential `.unc.tmp` writes at ~disk bandwidth; 10M rows ≈ 3 GB temp I/O.
  On NVMe this adds ~1–3s per 10M row import vs the current in-memory approach (~28s).

**Decision gate**: implement Plan B when 10M row import on target hardware runs OOM or
when a 100M-row use case is confirmed.

---

### Files Changed (this batch)

| File | Summary |
|---|---|
| `.gitignore` | Add `data/hits_*.parquet` glob to suppress large data files |
| `src/clickhouse_format/part.zig` | ColumnWriter unc_buf, pk_col_name, cmrk2 marks, serialization.json/metadata_version.txt |
| `src/loader.zig` | importParquetCH, chAllFixedBatch, chAllStrValue |
| `src/parquet.zig` | streamAllColumnsPath low-memory multi-column import |
| `src/main.zig` | --format=ch --pk=<col> CLI |
| `scripts/interop-test.sh` | Step 8 WHERE regression (4 queries) |

---

### Next Steps (priority order)

1. **Commit this batch** — all tests green, comments clean.
2. **Plan B unc_buf cap** — implement mmap-based `.unc.tmp` fallback for 100M-row safety.
3. **Phase 2 read path** — `OpenedPart` reads CH existing parts (mmap `.bin` + `.cmrk2`).
4. **CH HTTP sidecar** — minimal `/ping` + `/?query=SELECT...` so ZH serves CH data dirs.
5. **Sparse encoding** — low-cardinality / all-zero columns, closes 100×+ disk gap.
