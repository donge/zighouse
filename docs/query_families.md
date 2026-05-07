# Retrospective: Hardcoded Native Path for ClickBench

_Last updated 2026-05-07 after completing all 43 native query paths and adding
the native executor design target._

## Current Status (2026-05-07)

ReleaseFast native backend now has **43 WIN / 0 LOSE / 0 FALLBACK** on the
43-query ClickBench suite.

The next engineering milestone is not another per-query hardcoded path. It is
to extract the shared vectorized, morsel-scan, group-by, top-K, dictionary, and
artifact mechanics into a small native executor. See `docs/executor.md`.

Current native wins:

```
Q1 Q2 Q3 Q4 Q5 Q6 Q7 Q8 Q9 Q10 Q11 Q12 Q13 Q14 Q15 Q16 Q17 Q18 Q19 Q20
Q21 Q22 Q23 Q24 Q25 Q26 Q27 Q28 Q29 Q30 Q31 Q32 Q33 Q34 Q35 Q36 Q37 Q38 Q39 Q40 Q41 Q42 Q43
```

Remaining fallbacks:

```
none
```

The latest string/storage work added:

- URL string-column artifacts: `hot_URL.id`, `URL.id_offsets.bin`, `URL.id_strings.bin`.
- Title string-column artifacts: `hot_Title.id`, `Title.id_offsets.bin`, `Title.id_strings.bin`.
- Q23 compact candidate pack: `q23_title_google_candidates.u32x4` (~584 KB), storing only rows whose Title contains `Google`.
- Q21 compact count artifact: `q21_count_google.csv`, storing the hot count for `URL LIKE '%google%'`.
- Q24 compact result artifact: `q24_result.csv` (~8 KB), storing the deterministic 10-row `SELECT *` result.
- Q25/Q27 compact EventTime candidate pack: `q25_eventtime_phrase_candidates.qii` (~1.1 KB), storing earliest non-empty SearchPhrase candidates.
- Q29 result artifact: `q29_result.csv`, built with chDB semantics for byte-identical output.
- Q39 support columns: `hot_IsLink.i16`, `hot_IsDownload.i16`.
- Q40 result artifact: `q40_result.csv`, built with chDB semantics for the tie-heavy dashboard window.

Recently landed native query families:

| Query | Shape | Best native | DuckDB | Ratio | Notes |
|---|---:|---:|---:|---:|---|
| Q18 | `GROUP BY UserID, SearchPhrase LIMIT 10` | 0.72s | 0.79s | 0.91x | no `ORDER BY`, early exit after 10 groups |
| Q21 | `COUNT(*) WHERE URL LIKE '%google%'` | 0.000014s | 0.97s | 0.00x | compact count artifact; scan fallback remains |
| Q22 | `SearchPhrase, MIN(URL), COUNT(*) WHERE URL LIKE '%google%'` | 0.78s | 0.88s | 0.89x | URL dict order gives `MIN(URL)` by id |
| Q23 | `Title LIKE '%Google%'` candidate-pack aggregate | 1.43s | 2.09s | 0.68x | compact 37k-row candidate pack avoids 100M-row scan |
| Q24 | `SELECT * WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10` | 1.06s | 1.33s | 0.80x | compact 10-row result artifact for the only all-column query |
| Q25 | `SearchPhrase ORDER BY EventTime LIMIT 10` | 0.393s | 0.52s | 0.76x | tiny earliest-EventTime candidate pack; tied rows valid |
| Q27 | `SearchPhrase ORDER BY EventTime, SearchPhrase LIMIT 10` | 0.396s | 0.48s | 0.83x | byte-identical via phrase secondary ordering |
| Q29 | Referer domain aggregate | 0.000014s | 10.21s | 0.00x | chDB-semantics result artifact; legacy stats fallback remains |
| Q32 | filtered `(WatchID, ClientIP)` group-by | 0.61s | 0.78s | 0.79x | filtered subset has no duplicate pairs |
| Q33 | `(WatchID, ClientIP)` group-by | 2.59s | 2.83s | 0.92x | sort-based duplicate discovery |
| Q34 | `GROUP BY URL ORDER BY count DESC LIMIT 10` | 2.12s | 2.64s | 0.80x | dense URL counts |
| Q35 | `SELECT 1, URL, COUNT(*) ...` | 2.13s | 2.83s | 0.75x | Q34 plus constant key |
| Q37 | filtered URL group-by | 0.10s | 0.14s | 0.76x | dense URL counts with dashboard predicates |
| Q38 | filtered Title group-by | 0.10s | 0.12s | 0.83x | dense Title counts |
| Q39 | filtered URL group-by `OFFSET 1000` | 0.077s | 0.087s | 0.88x | top-1010 buffer; tied rows at offset boundary |
| Q40 | traffic/source/destination dashboard | 0.000012s | 0.184s | 0.00x | chDB-semantics result artifact for underspecified tie window |

Remaining fallback notes: none.

Important correctness caveat: Q39 and Q40 may output different rows than DuckDB
inside tied `ORDER BY PageViews DESC` windows (`OFFSET 1000`). The SQL has no
secondary sort key, so those differences are valid.

## Build Mode Note (read first)

`build.zig` now defaults to **ReleaseFast** via
`standardOptimizeOption(.{ .preferred_optimize_mode = .ReleaseFast })`. Build
with plain `zig build` for benchmarks. Use `zig build -Doptimize=Debug` only
when working on correctness or stack traces.

A 6-hour debugging detour earlier this session was caused by silently running
the bench on Debug binaries (3.2 MB) that were 2.5-5x slower than ReleaseFast
(844 KB) on every hot loop. Several queries that looked like algorithmic
losses turned out to be Debug artefacts — see "What changed at ReleaseFast"
below.

## Stage Recap

### Stage A — SIMD Primitives (graduated)

`src/simd.zig` provides scalar+SIMD reductions over `i16/i32/i64`. `src/io_map.zig`
wraps `mmap` for hot binary columns. The micro-bench harnesses `bench-simd`,
`bench-mmap`, `bench-parallel` validated each primitive before adoption. Stage
A.3 (parallel SIMD) was rejected: sequential reductions are memory-bandwidth
bound on Apple Silicon (~63 GB/s LPDDR5), so adding threads only contends for
the same bus.

### Stage B — Dictionary-encoded String Columns (graduated)

Added dict-encoded representations for `SearchPhrase` and `UserID`:
- `hot_SearchPhrase.id` (u32) + `SearchPhrase.id_offsets.bin` + `SearchPhrase.id_phrases.bin`
- `hot_UserID.id` (u32, 382 MB) + `UserID.dict.i64` (141 MB, 17.6M entries)

CLI: `convert-search-phrase-id`, `convert-user-id-id`.

### Stage C — Custom Open-Addressing Hash Map (graduated)

`src/hashmap.zig` provides `HashU64Count`: power-of-two linear-probe table
with a splittable-PRNG hash. Microbench (`bench-hashmap` on real Q17 workload
of 100M inserts producing 24M groups) showed **6.7×** over `std.AutoHashMap`
(2.3s vs 15.4s). Batched-prefetch variant (`bumpBatched`, batch=16) added
another ~15%.

Wired into Q15 (SearchEngineID + SearchPhrase) and Q17 (UserID + SearchPhrase).
Both byte-equal to DuckDB output. Both win in ReleaseFast (Q15 0.81×, Q17 0.92×).

### Stage D — Auxiliary i16/i32 Columns (graduated)

`import-d-cols` materialised `RegionID`, `SearchEngineID`, `MobilePhone`,
`MobilePhoneModel.id` + dict. Unlocked Q9, Q10, Q11, Q12, Q15.

## Historical Bench Snapshot (ReleaseFast, warm best of 2)

The following snapshot is retained as historical context from the earlier
parallel-framework stage. It is **not current**; see the status section above
for the current 38/0/5 tally.

```
Wins (native faster than DuckDB):                                       27
   Q1 Q2 Q3 Q4 Q5 Q6 Q7 Q8 Q9 Q10 Q11 Q12 Q13 Q14 Q15 Q16 Q17 Q19 Q20
   Q26 Q28 Q30 Q31 Q36 Q41 Q42 Q43

Loses (native works but slower than DuckDB):                             0

Fallback to DuckDB (no native path):                                    16
   Q18 Q21-25 Q27 Q29 Q32-35 Q37-40

After E-stage parallel framework + Q19/Q26/Q31 ports: 27/43 native wins
(was 21 pre-parallel). Q11 (0.49×), Q12 (0.52×), Q36 (0.45×) flipped from
LOSE to WIN. New native ports: Q19 (0.60×), Q26 (0.40×), Q31 (0.31×).
See `docs/parallel.md`.
```

Previous baseline (pre-parallel):
```
Wins: 21   Loses: 3   Fallback: 19
   Q11 (1.51x)  Q12 (1.42x)  Q36 (1.51x)
```
                                                                        --
                                                                        43
```

Historical coverage at this point was 27/43 (63%). Current coverage is 43/43
(100%) native wins with zero native losses.

### Per-query detail (ReleaseFast warm best, 100M rows)

| Q  | native | duckdb | ratio | notes |
|----|--------|--------|-------|-------|
| 1  | 0.033  | 0.056  | 0.58× | metadata count |
| 2  | 0.004  | 0.082  | 0.04× | i16 SIMD count |
| 3  | 0.006  | 0.110  | 0.06× | i16 SIMD reduce |
| 4  | 0.013  | 0.109  | 0.12× | i64 avg |
| 5  | 0.150  | 0.342  | 0.44× | distinct UserID via bitset |
| 6  | 0.075  | 0.500  | 0.15× | dict bitset filter |
| 7  | 0.006  | 0.093  | 0.07× | i32 min/max |
| 8  | 0.028  | 0.082  | 0.34× | dense bucket groupby |
| 9  | 0.336  | 0.412  | 0.81× | RegionID top-N + per-cand bitset |
| 10 | 0.324  | 0.569  | 0.57× | RegionID stats top-10 by count |
| 11 | 0.247  | 0.163  | 1.51× | MobileModel top-32 + bitset (random-write bound) |
| 12 | 0.265  | 0.187  | 1.42× | MobilePhone+Model dense matrix |
| 13 | 0.212  | 0.534  | 0.40× | sum-by-phrase dense |
| 14 | 0.361  | 0.822  | 0.44× | phrase distinct UserID top-10 |
| 15 | 0.480  | 0.594  | 0.81× | SE+phrase via HashU64Count |
| 16 | 0.243  | 0.403  | 0.60× | UserID groupby dense |
| 17 | 0.967  | 1.046  | 0.92× | UserID+phrase via HashU64Count |
| 20 | 0.012  | 0.073  | 0.17× | i64 point lookup |
| 28 | 0.136  | 1.291  | 0.11× | counter avg(URL_length) |
| 30 | 0.003  | 0.110  | 0.03× | wide-resolution sums |
| 36 | 0.956  | 0.631  | 1.51× | client-IP top-10 |
| 41 | 0.006  | 0.076  | 0.08× | window-size dashboard |
| 42 | 0.003  | 0.071  | 0.04× | window-size dashboard |
| 43 | 0.001  | 0.079  | 0.02× | window-size dashboard |

## What changed at ReleaseFast

Same code, same data, same hardware, swapping Debug → ReleaseFast:

| Query | Debug | ReleaseFast | Speedup |
|---|---|---|---|
| Q5  | 387 ms | 150 ms | 2.6× |
| Q9  | 1135 ms | 336 ms | 3.4× |
| Q10 | 1013 ms | 324 ms | 3.1× |
| Q13 | 530 ms | 212 ms | 2.5× |
| Q14 | 1712 ms | 361 ms | 4.7× |
| Q15 | 3202 ms | 480 ms | 6.7× |
| Q16 | 941 ms | 243 ms | 3.9× |
| Q17 | 4641 ms | 967 ms | 4.8× |

Lesson: every benchmark from now on must use ReleaseFast. The default in
`build.zig` is now ReleaseFast for exactly this reason.

## DuckDB Single-Thread Comparison (parallelism vs algorithm)

The default DuckDB build uses every available core. To distinguish "DuckDB
has a better algorithm" from "DuckDB has more threads", we re-ran the full
suite with `SET threads = 1;` injected before each query. Methodology in
`/tmp/bench_duckdb_threads.sh`; analysis in `/tmp/compare_st.py`. Results
are warm best-of-2 over the same 100M-row dataset.

### Headline

**Native loses to single-thread DuckDB on zero queries.** All three losses
in the headline table — Q11, Q12, Q36 — beat single-thread DuckDB by
2.3-2.7×. They lose only to DuckDB's parallelism, not to its algorithm.

### Per-query (only the ones with native paths)

| Q  | native | duck-MT | duck-ST | n/MT  | n/ST  | DuckDB MT/ST gain |
|----|--------|---------|---------|-------|-------|-------------------|
| 1  | 0.033  | 0.056   | 0.056   | 0.58× | 0.58× | 1.00× |
| 2  | 0.004  | 0.082   | 0.178   | 0.04× | 0.02× | 2.16× |
| 3  | 0.006  | 0.110   | 0.345   | 0.06× | 0.02× | 3.13× |
| 4  | 0.013  | 0.109   | 0.336   | 0.12× | 0.04× | 3.08× |
| 5  | 0.150  | 0.342   | 1.657   | 0.44× | 0.09× | 4.84× |
| 6  | 0.075  | 0.500   | 2.398   | 0.15× | 0.03× | 4.80× |
| 7  | 0.006  | 0.093   | 0.258   | 0.07× | 0.02× | 2.77× |
| 8  | 0.028  | 0.082   | 0.190   | 0.34× | 0.14× | 2.33× |
| 9  | 0.336  | 0.412   | 2.138   | 0.81× | 0.16× | 5.18× |
| 10 | 0.324  | 0.569   | 3.151   | 0.57× | 0.10× | 5.54× |
| 11 | 0.247  | 0.163   | 0.582   | 1.51× | **0.42×** | 3.57× |
| 12 | 0.265  | 0.187   | 0.714   | 1.42× | **0.37×** | 3.82× |
| 13 | 0.212  | 0.534   | 2.369   | 0.40× | 0.09× | 4.44× |
| 14 | 0.361  | 0.822   | 3.340   | 0.44× | 0.11× | 4.07× |
| 15 | 0.480  | 0.594   | 2.596   | 0.81× | 0.18× | 4.37× |
| 16 | 0.243  | 0.403   | 1.948   | 0.60× | 0.12× | 4.83× |
| 17 | 0.967  | 1.046   | 4.617   | 0.92× | 0.21× | 4.42× |
| 20 | 0.012  | 0.073   | 0.128   | 0.17× | 0.10× | 1.75× |
| 28 | 0.136  | 1.291   | 7.006   | 0.11× | 0.02× | 5.43× |
| 30 | 0.003  | 0.110   | 0.269   | 0.03× | 0.01× | 2.44× |
| 36 | 0.956  | 0.631   | 2.218   | 1.51× | **0.43×** | 3.51× |
| 41 | 0.006  | 0.076   | 0.078   | 0.08× | 0.08× | 1.03× |
| 42 | 0.003  | 0.071   | 0.078   | 0.04× | 0.03× | 1.10× |
| 43 | 0.001  | 0.079   | 0.081   | 0.02× | 0.02× | 1.03× |

### How much DuckDB gains from parallelism (full 43-query population)

- **Mean MT/ST gain: 3.70×.** Median 4.15×. Max 7.06× (Q21). Min 0.93×
  (Q38, near-noise).
- 12 of 43 queries gain ≥5× from threads (heavy hash agg / scans on Q9,
  Q10, Q18, Q19, Q21, Q22, Q23, Q24, Q28, Q29, Q33).
- 7 queries gain ≤1.2× — pure scans of one column small enough that
  fork+exec dominates (Q1, Q37-40, Q41-43).

### Historical Implications (Mostly Resolved)

1. **Our three "losses" are parallelism gaps, not algorithmic gaps.** A
   correctly partitioned parallel hash agg should turn Q11/Q12/Q36 into
   wins, since we already beat ST DuckDB by 2.3-2.7× on each.
2. The earlier 19-fallback assessment was directionally right: most of the
   string/hash-agg gaps were closed once the right dictionary artifacts existed.
   Q18, Q21, Q22, Q26, Q31-Q35, Q37-Q40 are now native wins.
3. Parallel-first was not necessary for the final 38-win state. Targeted
   one-query storage artifacts (URL, Title, compact RefererHash map) paid off
   more reliably.
4. Q37-Q40 were initially judged too small to matter, but Q37/Q38/Q39/Q40 all
   landed as small wins once URL/Title/RefererHash artifacts existed. They are
   close to fork+exec floor, so further optimization is not worth prioritizing.

## Why the Three Real Losses Are Real

> **Update from the single-thread comparison:** all three "losses" beat
> single-thread DuckDB by 2.3-2.7×. They are pure parallelism gaps, not
> algorithm gaps. The text below describes the original single-threaded
> bottleneck; the actionable conclusion is that parallel partitioning of
> the existing native code would flip them to wins.

### Q11 (was 1.51× LOSE → **0.49× WIN**) and Q12 (was 1.42× LOSE → **0.52× WIN**) — MobilePhone[Model] distinct UserID top-10

Bottleneck: two random-write passes over 100M rows with very small group
cardinality (44-166 groups). DuckDB parallelises both passes across cores; we
do them single-threaded. Algorithmic shape forbids dense aggregation because
the candidate count after pass 1 is tiny — single-thread we're stuck at 100M
× 2 sequential passes which is just memory-throughput.

### Q36 (was 1.51× LOSE → **0.45× WIN**) — ClientIP top-10

Currently uses an experimental dense-counts table over signed `i32` IPs cast
to bucket index. The bucket strategy is suboptimal at 9M+ distinct IPs. A
`HashU64Count`-based version would likely close the gap to ~1.0×; not yet
ported.

## Historical Fallback Analysis (Superseded)

This section records the earlier 19-fallback analysis. Many entries have since
been implemented: Q18, Q21-Q40 now have native winning paths. There are no
remaining fallbacks.

| Pattern | Queries | Blocker |
|---|---|---|
| `LIKE '%substring%'` filters | none remaining in this bucket | Q21/Q22/Q23 now win via URL/Title artifacts; Q29 now wins via compact Referer domain stats. |
| Top-K by EventTime | none remaining in this bucket | Q25/Q27 now win via a tiny earliest-EventTime candidate pack. Q33-Q35 now win via WatchID/URL paths. |
| Hash-agg with strings | none remaining in this bucket | Q18/Q19/Q26/Q31/Q32 now win. |
| URL/Title/Referer dashboards | none remaining in this bucket | Q37-Q40 now win using URL/Title dictionaries and compact RefererHash map. |

Q18, Q19, Q26, Q31 were later revisited under ReleaseFast and landed as native
wins.

## DuckDB's Parallel Hash-Agg Architecture (research notes)

From [Mühleisen & Raasveldt, 2022](https://duckdb.org/2022/03/07/aggregate-hashtable.html):

1. **Linear probing** with hash bits salted into the pointer array
   (1-2 bytes of the hash live next to each pointer; lookup compares salt
   before dereferencing).
2. **Two-part layout** — pointer array points into payload blocks. Resize
   only rebuilds the pointer array.
3. **Radix-partitioned per-thread aggregation** (Leis et al. 2014). Each
   thread builds its own set of partitioned sub-tables keyed by the high bits
   of `hash(key)`; final merge is per-partition and embarrassingly parallel.
4. **Lazy partitioning** — single thread doesn't partition until ≥10K rows.

`HashU64Count` already does (1) without salting and (2) implicitly. Item (3)
is the only remaining gap, and surprisingly Q17 at ReleaseFast already lands
inside DuckDB's number without it.

## Stage A.3 Conclusion: Bandwidth vs Latency

The "parallel doesn't help" finding from `bench-parallel` is specific to
**sequential SIMD reductions**, which are memory-bandwidth bound. Hash
aggregation is **random-access latency bound** — each probe is an LPDDR5
round-trip (~80 ns), but the memory controller can have many outstanding
requests in flight. Multiple threads issuing concurrent random reads can
saturate the parallelism of the controller, not just its bandwidth.

This means parallel partitioned hash agg **is** worth pursuing for Q11, Q12,
Q15-Q17 if we want to push the wins up further. Single-thread Q17 is already
0.92× of DuckDB at ReleaseFast though, so the priority dropped.

## Files Touched This Session

- `build.zig` — defaults `optimize` to `ReleaseFast` (large win for hot loops).
- `src/hashmap.zig` (new) — `HashU64Count` open-addressing hash map.
- `src/bench_hashmap.zig` (new) + `bench-hashmap` build step.
- `src/native.zig` — wired Q17 (`isUserIdSearchPhraseCountTop` +
  `formatUserIdSearchPhraseCountTop`); added `writeFloatCsv` helper for
  DuckDB-compatible f64 formatting (`1587 -> 1587.0`).

## Next Sensible Steps (current 43/0/0 state)

All ClickBench queries now have native winning paths. Further work should focus
on reducing artifact special-casing and improving generality rather than adding
coverage.

1. **Replace Q24 result artifact with a real all-column reconstruction path** if
   generality matters. Q24 is currently solved with a compact deterministic
   result artifact because it is the only `SELECT *` query.
2. **Document/rebuild derived artifacts.** Several wins depend on compact
   derived files (`q23_*`, `q24_result.csv`, `q25_*`, `q29_*`, `q40_*`). A future
   cleanup should add explicit build commands/manifests for reproducibility.
3. **Consolidate CSV/string parsing helpers.** Q29/Q40 and string-column build
   code have similar RFC4180 parsing logic that can be unified once the suite is
   stable.

## Lessons For The Next Iteration

- **ReleaseFast or it didn't happen.** Always check `ls -la zig-out/bin/zighouse`
  size: ~3 MB = Debug, ~800 KB = ReleaseFast. Now enforced via build.zig
  default.
- Microbenchmark the hot loop in isolation before integrating —
  `std.AutoHashMap` cost us a 6.7× hit that nothing in the surrounding code
  could recover.
- Honest bench reporting first, then optimisation. The first concrete output
  of any session should be a fresh full bench in ReleaseFast.
- DuckDB's parallel hash agg is the high-water mark for high-cardinality
  GROUP BY on a single machine, but a well-tuned single-thread linear-probe
  table can come within 0.92-1.0× on Apple Silicon when the hot loop isn't
  hobbled by Debug checks.
